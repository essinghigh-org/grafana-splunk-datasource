package plugin

import (
	"context"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

const maxCachedBaseSearches = 1000

type searchNamespace struct {
	orgID          int64
	user           backend.User
	hasUser        bool
	dashboardUID   string
	dashboardTitle string
}

type searchCacheKey struct {
	namespace     searchNamespace
	identifier    string
	query         string
	sourcePanelID string
	earliest      string
	latest        string
	dashboardFrom int64
	dashboardTo   int64
}

type baseSearchFlight struct {
	done     chan struct{}
	job      cachedSearch
	err      error
	searchID string
	refID    string
}

func dashboardNamespace(req *backend.QueryDataRequest) searchNamespace {
	if req == nil {
		return searchNamespace{}
	}
	namespace := searchNamespace{orgID: req.PluginContext.OrgID}
	if req.PluginContext.User != nil {
		namespace.user = *req.PluginContext.User
		namespace.hasUser = true
	}
	namespace.dashboardUID = req.GetHTTPHeader("X-Dashboard-Uid")
	if namespace.dashboardUID == "" {
		namespace.dashboardTitle = req.GetHTTPHeader("X-Dashboard-Title")
	}
	return namespace
}

func (d *Datasource) cacheKey(req *backend.QueryDataRequest, query backend.DataQuery, model queryModel, identifier string) searchCacheKey {
	earliest, latest := queryTimes(query, model)
	panelID := ""
	if req != nil {
		panelID = req.GetHTTPHeader("X-Panel-Id")
	}
	return searchCacheKey{
		namespace:     dashboardNamespace(req),
		identifier:    identifier,
		query:         model.QueryText,
		sourcePanelID: panelID,
		earliest:      earliest,
		latest:        latest,
		dashboardFrom: query.TimeRange.From.UnixNano(),
		dashboardTo:   query.TimeRange.To.UnixNano(),
	}
}

func (d *Datasource) getCached(key searchCacheKey) (cachedSearch, bool) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	job, ok := d.cache[key]
	if !ok || time.Since(job.createdAt) >= d.cacheTTL {
		delete(d.cache, key)
		return cachedSearch{}, false
	}
	return job, true
}

func (d *Datasource) putCached(key searchCacheKey, job cachedSearch) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	d.putCachedLocked(key, job)
}

// putCachedLocked requires cacheMu to be held by the caller.
func (d *Datasource) putCachedLocked(key searchCacheKey, job cachedSearch) {
	d.pruneExpired(time.Now())
	if d.cache == nil {
		d.cache = make(map[searchCacheKey]cachedSearch)
	}
	if key.sourcePanelID != "" {
		for candidate, cached := range d.cache {
			if candidate.namespace == key.namespace && candidate.identifier == key.identifier &&
				candidate.sourcePanelID == key.sourcePanelID && cached.refID == job.refID &&
				candidate.dashboardFrom == key.dashboardFrom && candidate.dashboardTo == key.dashboardTo {
				delete(d.cache, candidate)
			}
		}
	}
	if _, exists := d.cache[key]; !exists {
		for len(d.cache) >= maxCachedBaseSearches {
			var oldestKey searchCacheKey
			var oldestTime time.Time
			for candidate, cached := range d.cache {
				if oldestTime.IsZero() || cached.createdAt.Before(oldestTime) {
					oldestKey = candidate
					oldestTime = cached.createdAt
				}
			}
			delete(d.cache, oldestKey)
		}
	}
	d.cache[key] = job
}

// pruneExpired requires cacheMu to be held by the caller.
func (d *Datasource) pruneExpired(now time.Time) {
	for key, job := range d.cache {
		if now.Sub(job.createdAt) >= d.cacheTTL {
			delete(d.cache, key)
		}
	}
}

func (d *Datasource) lookupCachedSearch(req *backend.QueryDataRequest, query backend.DataQuery, identifier string) (cachedSearch, bool) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	d.pruneExpired(time.Now())
	namespace := dashboardNamespace(req)
	if identifier == "" || (namespace.dashboardUID == "" && namespace.dashboardTitle == "") {
		return cachedSearch{}, false
	}
	return d.lookupCachedSearchLocked(namespace, query, identifier)
}

// lookupCachedSearchLocked requires cacheMu to be held by the caller.
func (d *Datasource) lookupCachedSearchLocked(namespace searchNamespace, query backend.DataQuery, identifier string) (cachedSearch, bool) {
	var result cachedSearch
	found := false
	for key, job := range d.cache {
		if !matchesCachedSearch(key, job, namespace, query, identifier) {
			continue
		}
		if found && result.sid != job.sid {
			return cachedSearch{}, false
		}
		if !found || job.createdAt.After(result.createdAt) {
			result = job
		}
		found = true
	}
	return result, found
}

func matchesSearchScope(key searchCacheKey, namespace searchNamespace, query backend.DataQuery) bool {
	return key.namespace == namespace && key.dashboardFrom == query.TimeRange.From.UnixNano() && key.dashboardTo == query.TimeRange.To.UnixNano()
}

func matchesCachedSearch(key searchCacheKey, job cachedSearch, namespace searchNamespace, query backend.DataQuery, identifier string) bool {
	return matchesSearchScope(key, namespace, query) && job.sid != "" && (job.searchID == identifier || job.refID == identifier)
}

func (d *Datasource) resolveBaseSearch(
	ctx context.Context,
	query backend.DataQuery,
	model queryModel,
	req *backend.QueryDataRequest,
	baseJobs map[string]cachedSearch,
) (cachedSearch, error) {
	if err := ctx.Err(); err != nil {
		return cachedSearch{}, err
	}
	identifier := model.SearchID
	if identifier == "" {
		identifier = query.RefID
	}
	remember := func(job cachedSearch) (cachedSearch, error) {
		baseJobs[identifier] = job
		baseJobs[query.RefID] = job
		return job, nil
	}
	key := d.cacheKey(req, query, model, identifier)
	d.cacheMu.Lock()
	d.pruneExpired(time.Now())
	if job, ok := d.cache[key]; ok {
		d.cacheMu.Unlock()
		return remember(job)
	}
	if flight, ok := d.inflight[key]; ok {
		d.cacheMu.Unlock()
		select {
		case <-ctx.Done():
			return cachedSearch{}, ctx.Err()
		case <-flight.done:
			if flight.err != nil {
				return cachedSearch{}, flight.err
			}
			return remember(flight.job)
		}
	}
	if d.inflight == nil {
		d.inflight = make(map[searchCacheKey]*baseSearchFlight)
	}
	flight := &baseSearchFlight{done: make(chan struct{}), searchID: identifier, refID: query.RefID}
	d.inflight[key] = flight
	d.cacheMu.Unlock()

	sid, err := d.startSearch(ctx, query, model, "", d.base)
	var job cachedSearch
	if err == nil {
		job = cachedSearch{sid: sid, searchID: identifier, refID: query.RefID, createdAt: time.Now()}
	}
	d.cacheMu.Lock()
	if err == nil {
		d.putCachedLocked(key, job)
	}
	flight.job, flight.err = job, err
	if d.inflight[key] == flight {
		delete(d.inflight, key)
	}
	close(flight.done)
	d.cacheMu.Unlock()
	if err != nil {
		return cachedSearch{}, err
	}
	return remember(job)
}

func (d *Datasource) waitForCachedSearch(ctx context.Context, req *backend.QueryDataRequest, query backend.DataQuery, identifier string) (cachedSearch, bool, error) {
	namespace := dashboardNamespace(req)
	if identifier == "" || (namespace.dashboardUID == "" && namespace.dashboardTitle == "") {
		return cachedSearch{}, false, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return cachedSearch{}, false, err
		}
		d.cacheMu.Lock()
		d.pruneExpired(time.Now())
		var pending *baseSearchFlight
		var pendingKey searchCacheKey
		for key, flight := range d.inflight {
			if !matchesSearchScope(key, namespace, query) || (flight.searchID != identifier && flight.refID != identifier) {
				continue
			}
			if pending != nil && pending != flight {
				d.cacheMu.Unlock()
				return cachedSearch{}, false, nil
			}
			pending, pendingKey = flight, key
		}
		if pending == nil {
			job, ok := d.lookupCachedSearchLocked(namespace, query, identifier)
			d.cacheMu.Unlock()
			return job, ok, nil
		}
		for key, job := range d.cache {
			if matchesCachedSearch(key, job, namespace, query, identifier) &&
				(key.sourcePanelID == "" || key.sourcePanelID != pendingKey.sourcePanelID || key.identifier != pendingKey.identifier || job.refID != pending.refID) {
				d.cacheMu.Unlock()
				return cachedSearch{}, false, nil
			}
		}
		d.cacheMu.Unlock()
		select {
		case <-ctx.Done():
			return cachedSearch{}, false, ctx.Err()
		case <-pending.done:
			if pending.err != nil {
				return cachedSearch{}, false, pending.err
			}
		}
	}
}
