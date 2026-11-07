package plugin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
)

func cacheTestRequest() (*backend.QueryDataRequest, backend.DataQuery) {
	req := &backend.QueryDataRequest{PluginContext: backend.PluginContext{
		OrgID: 1,
		User:  &backend.User{Login: "alice", Name: "Alice", Email: "alice@example.com", Role: "Viewer"},
	}}
	req.SetHTTPHeader("X-Dashboard-Uid", "dashboard-1")
	query := backend.DataQuery{RefID: "B", TimeRange: backend.TimeRange{From: time.Unix(1000, 0), To: time.Unix(2000, 0)}}
	return req, query
}

func cacheTestDatasource() *Datasource {
	return &Datasource{cacheTTL: time.Minute, cache: make(map[searchCacheKey]cachedSearch)}
}

func TestCachedBaseSearchIsScopedToUserAndOrganization(t *testing.T) {
	tests := []struct {
		name   string
		change func(*backend.QueryDataRequest)
	}{
		{"other user", func(req *backend.QueryDataRequest) { req.PluginContext.User.Login = "bob" }},
		{"changed role", func(req *backend.QueryDataRequest) { req.PluginContext.User.Role = "Admin" }},
		{"other organization", func(req *backend.QueryDataRequest) { req.PluginContext.OrgID = 2 }},
		{"backend request", func(req *backend.QueryDataRequest) { req.PluginContext.User = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d := cacheTestDatasource()
			req, query := cacheTestRequest()
			model := queryModel{QueryText: "index=main"}
			d.putCached(d.cacheKey(req, query, model, "base"), cachedSearch{sid: "alice-sid", searchID: "base", refID: "B", createdAt: time.Now()})
			otherReq, _ := cacheTestRequest()
			test.change(otherReq)
			if _, ok := d.lookupCachedSearch(otherReq, query, "base"); ok {
				t.Fatal("resolved a cached base search from another user or organization")
			}
			if _, ok := d.getCached(d.cacheKey(otherReq, query, model, "base")); ok {
				t.Fatal("reused another user's or organization's exact cached query")
			}
		})
	}
}

func TestCachedBaseSearchRequiresMatchingDashboardRange(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	model := queryModel{QueryText: "index=main"}
	d.putCached(d.cacheKey(req, query, model, "base"), cachedSearch{sid: "old-range", searchID: "base", createdAt: time.Now()})
	otherQuery := query
	otherQuery.TimeRange.From = otherQuery.TimeRange.From.Add(time.Hour)
	otherQuery.TimeRange.To = otherQuery.TimeRange.To.Add(time.Hour)
	if _, ok := d.lookupCachedSearch(req, otherQuery, "base"); ok {
		t.Fatal("new dashboard range resolved a previous range's base search")
	}
	if _, ok := d.getCached(d.cacheKey(req, otherQuery, model, "base")); ok {
		t.Fatal("new dashboard range reused a previous range's exact cached query")
	}
}

func TestCachedFixedRangeBaseSearchKeepsDashboardAssociation(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	useDashboard := false
	model := queryModel{QueryText: "index=main", UseDashboardTimeRange: &useDashboard, Earliest: "-30d@d", Latest: "now"}
	d.putCached(d.cacheKey(req, query, model, "base"), cachedSearch{sid: "fixed-range", searchID: "base", createdAt: time.Now()})
	if job, ok := d.lookupCachedSearch(req, query, "base"); !ok || job.sid != "fixed-range" {
		t.Fatal("dashboard chain could not resolve its fixed-range base search")
	}
	otherQuery := query
	otherQuery.TimeRange.To = otherQuery.TimeRange.To.Add(time.Hour)
	if _, ok := d.lookupCachedSearch(req, otherQuery, "base"); ok {
		t.Fatal("different dashboard range resolved an unrelated fixed-range base search")
	}
}

func TestCachedBaseSearchResolvesSearchIDAndRefID(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	d.putCached(d.cacheKey(req, query, queryModel{QueryText: "index=main"}, "base"), cachedSearch{sid: "base-sid", searchID: "base", refID: "B", createdAt: time.Now()})
	for _, identifier := range []string{"base", "B"} {
		if job, ok := d.lookupCachedSearch(req, query, identifier); !ok || job.sid != "base-sid" {
			t.Fatalf("identifier %q did not resolve the cached base search", identifier)
		}
	}
}

func TestCachedBaseSearchRejectsAmbiguousAliases(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	for i, queryText := range []string{"index=one", "index=two"} {
		d.putCached(d.cacheKey(req, query, queryModel{QueryText: queryText}, "base"), cachedSearch{sid: fmt.Sprintf("sid-%d", i), searchID: "base", createdAt: time.Now()})
	}
	if _, ok := d.lookupCachedSearch(req, query, "base"); ok {
		t.Fatal("ambiguous base identifier selected an arbitrary cached SID")
	}
}

func TestCachedBaseSearchReplacesUpdatedQueryFromSamePanel(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	req.SetHTTPHeader("X-Panel-Id", "10")
	useDashboard := false
	oldModel := queryModel{QueryText: "index=old", UseDashboardTimeRange: &useDashboard, Earliest: "-30d@d", Latest: "now"}
	oldKey := d.cacheKey(req, query, oldModel, "base")
	d.putCached(oldKey, cachedSearch{sid: "old-sid", searchID: "base", refID: "B", createdAt: time.Now().Add(-time.Second)})
	newModel := queryModel{QueryText: "index=new", UseDashboardTimeRange: &useDashboard, Earliest: "-7d@d", Latest: "now"}
	d.putCached(d.cacheKey(req, query, newModel, "base"), cachedSearch{sid: "new-sid", searchID: "base", refID: "B", createdAt: time.Now()})
	if _, ok := d.cache[oldKey]; ok {
		t.Fatal("previous SPL from the same panel and query retained a stale alias")
	}
	chainReq, chainQuery := cacheTestRequest()
	chainReq.SetHTTPHeader("X-Panel-Id", "20")
	chainQuery.RefID = "C"
	if job, ok := d.lookupCachedSearch(chainReq, chainQuery, "base"); !ok || job.sid != "new-sid" {
		t.Fatal("another panel could not resolve the refreshed base search")
	}
}

func TestCachedBaseSearchRejectsDuplicateAliasesAcrossPanels(t *testing.T) {
	d := cacheTestDatasource()
	for i, panelID := range []string{"10", "20"} {
		req, query := cacheTestRequest()
		req.SetHTTPHeader("X-Panel-Id", panelID)
		model := queryModel{QueryText: fmt.Sprintf("index=%d", i)}
		d.putCached(d.cacheKey(req, query, model, "base"), cachedSearch{sid: "sid-" + panelID, searchID: "base", refID: "B", createdAt: time.Now()})
	}
	req, query := cacheTestRequest()
	req.SetHTTPHeader("X-Panel-Id", "30")
	if len(d.cache) != 2 {
		t.Fatal("one panel's base search replaced another panel's duplicate identifier")
	}
	if _, ok := d.lookupCachedSearch(req, query, "base"); ok {
		t.Fatal("duplicate identifiers from different panels selected an arbitrary SID")
	}
}

func TestCachedBaseSearchAllowsDuplicateAliasesForSameSID(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	for _, identifier := range []string{"base", "B"} {
		d.putCached(d.cacheKey(req, query, queryModel{QueryText: "index=main"}, identifier), cachedSearch{sid: "same-sid", searchID: "base", refID: "B", createdAt: time.Now()})
	}
	if job, ok := d.lookupCachedSearch(req, query, "base"); !ok || job.sid != "same-sid" {
		t.Fatal("duplicate aliases for a single SID were treated as ambiguous")
	}
}

func TestCachedBaseSearchRequiresDashboardForCrossRequestLookup(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	req.DeleteHTTPHeader("X-Dashboard-Uid")
	key := d.cacheKey(req, query, queryModel{QueryText: "index=main"}, "base")
	d.putCached(key, cachedSearch{sid: "explore-sid", searchID: "base", createdAt: time.Now()})
	if _, ok := d.lookupCachedSearch(req, query, "base"); ok {
		t.Fatal("request without a dashboard resolved a cross-request alias")
	}
	if _, ok := d.getCached(key); !ok {
		t.Fatal("missing dashboard unexpectedly disabled exact query caching")
	}
}

func TestBaseSearchCacheKeySeparatesQueryAndIdentifier(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	a := d.cacheKey(req, query, queryModel{QueryText: "| stats count"}, "id|index=foo")
	b := d.cacheKey(req, query, queryModel{QueryText: "index=foo|| stats count"}, "id")
	if a == b {
		t.Fatal("different identifiers and SPL collided in the cache key")
	}
}

func TestBaseSearchCacheUsesEffectiveTimeDefaults(t *testing.T) {
	d := cacheTestDatasource()
	req, query := cacheTestRequest()
	useDashboard := false
	a := d.cacheKey(req, query, queryModel{UseDashboardTimeRange: &useDashboard}, "base")
	b := d.cacheKey(req, query, queryModel{UseDashboardTimeRange: &useDashboard, Earliest: "-30d@d", Latest: "now"}, "base")
	if a != b {
		t.Fatal("equivalent default and explicit fixed ranges did not share a cache key")
	}
}

func TestBaseSearchCachePrunesExpiredEntries(t *testing.T) {
	t.Run("on insert", func(t *testing.T) {
		d := cacheTestDatasource()
		oldKey := searchCacheKey{identifier: "old"}
		d.putCached(oldKey, cachedSearch{createdAt: time.Now().Add(-2 * time.Minute)})
		d.putCached(searchCacheKey{identifier: "new"}, cachedSearch{createdAt: time.Now()})
		if _, ok := d.cache[oldKey]; ok {
			t.Fatal("expired unique key remained after inserting another search")
		}
	})
	t.Run("on lookup", func(t *testing.T) {
		d := cacheTestDatasource()
		oldKey := searchCacheKey{identifier: "old"}
		d.cache[oldKey] = cachedSearch{createdAt: time.Now().Add(-2 * time.Minute)}
		req, query := cacheTestRequest()
		_, _ = d.lookupCachedSearch(req, query, "missing")
		if _, ok := d.cache[oldKey]; ok {
			t.Fatal("expired unique key remained after an alias lookup")
		}
	})
}

func TestBaseSearchCacheEvictsOldestAtCapacity(t *testing.T) {
	d := cacheTestDatasource()
	createdAt := time.Now().Add(-time.Second)
	for i := 0; i < maxCachedBaseSearches; i++ {
		d.putCached(searchCacheKey{identifier: fmt.Sprint(i)}, cachedSearch{sid: fmt.Sprint(i), createdAt: createdAt.Add(time.Duration(i) * time.Millisecond)})
	}
	d.putCached(searchCacheKey{identifier: "new"}, cachedSearch{sid: "new", createdAt: time.Now()})
	if len(d.cache) != maxCachedBaseSearches {
		t.Fatalf("cache retained %d entries, expected %d", len(d.cache), maxCachedBaseSearches)
	}
	if _, ok := d.cache[searchCacheKey{identifier: "0"}]; ok {
		t.Fatal("oldest cache entry was not evicted")
	}
	if _, ok := d.cache[searchCacheKey{identifier: "new"}]; !ok {
		t.Fatal("new cache entry was not retained")
	}
	d.putCached(searchCacheKey{identifier: "new"}, cachedSearch{sid: "replacement", createdAt: time.Now()})
	if len(d.cache) != maxCachedBaseSearches {
		t.Fatal("replacing an existing cache entry evicted an unrelated search")
	}
}

func TestBaseSearchCacheSupportsConcurrentRequests(t *testing.T) {
	d := cacheTestDatasource()
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			req, query := cacheTestRequest()
			for i := 0; i < 16; i++ {
				identifier := fmt.Sprintf("base-%d-%d", worker, i)
				key := d.cacheKey(req, query, queryModel{QueryText: "index=main"}, identifier)
				d.putCached(key, cachedSearch{sid: identifier, searchID: identifier, createdAt: time.Now()})
				if job, ok := d.getCached(key); !ok || job.sid != identifier {
					t.Errorf("concurrent exact lookup lost search %q", identifier)
				}
				if job, ok := d.lookupCachedSearch(req, query, identifier); !ok || job.sid != identifier {
					t.Errorf("concurrent alias lookup lost search %q", identifier)
				}
			}
		}(worker)
	}
	workers.Wait()
}

type observedCacheWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *observedCacheWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func blockedCacheSearch(t *testing.T) (*Datasource, <-chan struct{}, func(), *atomic.Int32) {
	t.Helper()
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	var starts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/services/search/jobs":
			starts.Add(1)
			startedOnce.Do(func() { close(started) })
			select {
			case <-release:
			case <-request.Context().Done():
				return
			}
			_, _ = writer.Write([]byte(`{"sid":"new-sid"}`))
		case request.Method == http.MethodGet && request.URL.Path == "/services/search/jobs/new-sid":
			_, _ = writer.Write([]byte(`{"entry":[{"content":{"dispatchState":"DONE"}}]}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(server.Close)
	t.Cleanup(unblock)
	return testDatasource(server), started, unblock, &starts
}

type cacheResolution struct {
	job cachedSearch
	ok  bool
	err error
}

func TestResolveBaseSearchDeduplicatesConcurrentRequests(t *testing.T) {
	d, started, release, starts := blockedCacheSearch(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, query := cacheTestRequest()
	req.SetHTTPHeader("X-Panel-Id", "10")
	model := queryModel{QueryText: "index=main", SearchID: "base"}
	leader := make(chan cacheResolution, 1)
	go func() {
		job, err := d.resolveBaseSearch(ctx, query, model, req, make(map[string]cachedSearch))
		leader <- cacheResolution{job: job, err: err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("base search did not begin")
	}
	followerCtx := &observedCacheWaitContext{Context: ctx, entered: make(chan struct{})}
	follower := make(chan cacheResolution, 1)
	go func() {
		job, err := d.resolveBaseSearch(followerCtx, query, model, req, make(map[string]cachedSearch))
		follower <- cacheResolution{job: job, err: err}
	}()
	select {
	case <-followerCtx.entered:
	case <-ctx.Done():
		t.Fatal("duplicate base search did not begin waiting")
	}
	cancelContext, cancelFollower := context.WithCancel(ctx)
	cancelCtx := &observedCacheWaitContext{Context: cancelContext, entered: make(chan struct{})}
	canceled := make(chan error, 1)
	go func() {
		_, err := d.resolveBaseSearch(cancelCtx, query, model, req, make(map[string]cachedSearch))
		canceled <- err
	}()
	select {
	case <-cancelCtx.entered:
	case <-ctx.Done():
		cancelFollower()
		t.Fatal("cancelable duplicate search did not begin waiting")
	}
	cancelFollower()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled duplicate waiter, got %v", err)
	}
	release()
	for _, result := range []cacheResolution{<-leader, <-follower} {
		if result.err != nil || result.job.sid != "new-sid" {
			t.Fatalf("duplicate base search failed: sid=%q err=%v", result.job.sid, result.err)
		}
	}
	if got := starts.Load(); got != 1 {
		t.Fatalf("duplicate requests created %d Splunk jobs, expected one", got)
	}
}

func TestCrossPanelChainWaitsForRefreshedBaseSearch(t *testing.T) {
	d, started, release, _ := blockedCacheSearch(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, query := cacheTestRequest()
	req.SetHTTPHeader("X-Panel-Id", "10")
	d.putCached(d.cacheKey(req, query, queryModel{QueryText: "index=old"}, "base"), cachedSearch{sid: "old-sid", searchID: "base", refID: "B", createdAt: time.Now()})
	leader := make(chan error, 1)
	go func() {
		_, err := d.resolveBaseSearch(ctx, query, queryModel{QueryText: "index=new", SearchID: "base"}, req, make(map[string]cachedSearch))
		leader <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("refreshed base search did not begin")
	}
	chainReq, chainQuery := cacheTestRequest()
	chainReq.SetHTTPHeader("X-Panel-Id", "20")
	chainQuery.RefID = "C"
	chainCtx := &observedCacheWaitContext{Context: ctx, entered: make(chan struct{})}
	chain := make(chan cacheResolution, 1)
	go func() {
		job, ok, err := d.waitForCachedSearch(chainCtx, chainReq, chainQuery, "base")
		chain <- cacheResolution{job: job, ok: ok, err: err}
	}()
	select {
	case <-chainCtx.entered:
	case result := <-chain:
		t.Fatalf("chain returned before its refreshed base completed: %#v", result)
	case <-ctx.Done():
		t.Fatal("chain did not begin waiting for its base")
	}
	release()
	if err := <-leader; err != nil {
		t.Fatal(err)
	}
	result := <-chain
	if result.err != nil || !result.ok || result.job.sid != "new-sid" {
		t.Fatalf("chain did not receive the refreshed SID: %#v", result)
	}
}

func TestCrossPanelChainWaitCanBeCanceled(t *testing.T) {
	d, started, release, _ := blockedCacheSearch(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, query := cacheTestRequest()
	req.SetHTTPHeader("X-Panel-Id", "10")
	leader := make(chan error, 1)
	go func() {
		_, err := d.resolveBaseSearch(ctx, query, queryModel{QueryText: "index=main", SearchID: "base"}, req, make(map[string]cachedSearch))
		leader <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("base search did not begin")
	}
	chainContext, cancelChain := context.WithCancel(ctx)
	chainCtx := &observedCacheWaitContext{Context: chainContext, entered: make(chan struct{})}
	chain := make(chan error, 1)
	go func() {
		_, _, err := d.waitForCachedSearch(chainCtx, req, query, "base")
		chain <- err
	}()
	select {
	case <-chainCtx.entered:
	case <-ctx.Done():
		cancelChain()
		t.Fatal("chain did not begin waiting")
	}
	cancelChain()
	if err := <-chain; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled chain wait, got %v", err)
	}
	release()
	if err := <-leader; err != nil {
		t.Fatalf("canceling the chain canceled its base search: %v", err)
	}
}

func TestWaitForCachedSearchRejectsAmbiguousInflightAliases(t *testing.T) {
	d := cacheTestDatasource()
	d.inflight = make(map[searchCacheKey]*baseSearchFlight)
	req, query := cacheTestRequest()
	for _, panelID := range []string{"10", "20"} {
		req.SetHTTPHeader("X-Panel-Id", panelID)
		key := d.cacheKey(req, query, queryModel{QueryText: "index=main"}, "base")
		d.inflight[key] = &baseSearchFlight{done: make(chan struct{}), searchID: "base", refID: "B"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, ok, err := d.waitForCachedSearch(ctx, req, query, "base"); ok || err != nil {
		t.Fatalf("ambiguous in-flight aliases should fail promptly: found=%t err=%v", ok, err)
	}
}
