package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

const (
	defaultStandardTimeout = 30 * time.Second
	defaultBaseTimeout     = 120 * time.Second
	defaultChainTimeout    = 30 * time.Second
	defaultCacheTTL        = 5 * time.Minute
	defaultPageSize        = 50_000
	defaultMaxRows         = 100_000
	defaultMaxPages        = 100
	defaultMaxBytes        = 50 * 1024 * 1024
	pollInterval           = 100 * time.Millisecond
)

type queryModel struct {
	QueryText             string `json:"queryText"`
	SearchType            string `json:"searchType"`
	Mode                  string `json:"mode"`
	BaseSearchRefID       string `json:"baseSearchRefId"`
	SearchID              string `json:"searchId"`
	UseDashboardTimeRange *bool  `json:"useDashboardTimeRange"`
	Earliest              string `json:"earliest"`
	Latest                string `json:"latest"`
	ReturnBaseResults     bool   `json:"returnBaseResults"`
}

type backendOptions struct {
	StandardSearchTimeoutSeconds float64 `json:"standardSearchTimeoutSeconds"`
	BaseSearchTimeoutSeconds     float64 `json:"baseSearchTimeoutSeconds"`
	ChainSearchTimeoutSeconds    float64 `json:"chainSearchTimeoutSeconds"`
	BaseSearchCacheTtlMinutes    float64 `json:"baseSearchCacheTtlMinutes"`
	MaxResultRows                int     `json:"maxResultRows"`
	MaxResultPages               int     `json:"maxResultPages"`
	MaxResponseBytes             int64   `json:"maxResponseBytes"`
}

type cachedSearch struct {
	sid       string
	searchID  string
	refID     string
	createdAt time.Time
}

type Datasource struct {
	settings backend.DataSourceInstanceSettings
	client   *http.Client
	options  backendOptions
	standard time.Duration
	base     time.Duration
	chain    time.Duration
	cacheTTL time.Duration
	maxRows  int
	maxPages int
	maxBytes int64

	cacheMu sync.Mutex
	cache   map[string]cachedSearch
}

func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	clientOptions, err := settings.HTTPClientOptions(ctx)
	if err != nil {
		return nil, fmt.Errorf("http client options: %w", err)
	}

	// basicAuthToken was present in the original datasource model. Preserve it
	// for existing provisions while also supporting Grafana's standard basic auth.
	if token := settings.DecryptedSecureJSONData["basicAuthToken"]; token != "" {
		if strings.Contains(token, " ") {
			clientOptions.Header.Set("Authorization", token)
		} else {
			clientOptions.Header.Set("Authorization", "Bearer "+token)
		}
	}

	client, err := httpclient.New(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("create HTTP client: %w", err)
	}

	options := backendOptions{}
	if len(settings.JSONData) > 0 {
		if err := json.Unmarshal(settings.JSONData, &options); err != nil {
			return nil, fmt.Errorf("decode datasource options: %w", err)
		}
	}

	return &Datasource{
		settings: settings,
		client:   client,
		options:  options,
		standard: secondsOrDefault(options.StandardSearchTimeoutSeconds, defaultStandardTimeout),
		base:     secondsOrDefault(options.BaseSearchTimeoutSeconds, defaultBaseTimeout),
		chain:    secondsOrDefault(options.ChainSearchTimeoutSeconds, defaultChainTimeout),
		cacheTTL: minutesOrDefault(options.BaseSearchCacheTtlMinutes, defaultCacheTTL),
		maxRows:  intOrDefault(options.MaxResultRows, defaultMaxRows, 1, 1_000_000),
		maxPages: intOrDefault(options.MaxResultPages, defaultMaxPages, 1, 1000),
		maxBytes: int64OrDefault(options.MaxResponseBytes, defaultMaxBytes, 1024, 500*1024*1024),
		cache:    make(map[string]cachedSearch),
	}, nil
}

func (d *Datasource) Dispose() {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	d.cache = make(map[string]cachedSearch)
}

func secondsOrDefault(value float64, fallback time.Duration) time.Duration {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return fallback
	}
	return time.Duration(value * float64(time.Second))
}

func minutesOrDefault(value float64, fallback time.Duration) time.Duration {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return fallback
	}
	return time.Duration(value * float64(time.Minute))
}

func intOrDefault(value, fallback, minimum, maximum int) int {
	if value == 0 {
		return fallback
	}
	return min(maximum, max(minimum, value))
}

func int64OrDefault(value, fallback, minimum, maximum int64) int64 {
	if value == 0 {
		return fallback
	}
	return min64(maximum, max64(minimum, value))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	form := url.Values{
		"search":      {"search index=_internal * | stats count"},
		"output_mode": {"json"},
		"exec_mode":   {"oneshot"},
	}
	resp, err := d.doFormRequest(ctx, http.MethodPost, "/services/search/jobs", form)
	if err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: responseError(resp)}, nil
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: "Data source is working"}, nil
}

func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	response := backend.NewQueryDataResponse()
	standardQueries, baseQueries, chainQueries := partitionQueries(req.Queries)
	baseJobs := make(map[string]cachedSearch)

	for _, query := range standardQueries {
		response.Responses[query.RefID] = d.queryResponse(ctx, query, req, baseJobs)
	}
	for _, query := range baseQueries {
		response.Responses[query.RefID] = d.queryResponse(ctx, query, req, baseJobs)
	}
	for _, query := range chainQueries {
		response.Responses[query.RefID] = d.queryResponse(ctx, query, req, baseJobs)
	}

	return response, nil
}

func partitionQueries(queries []backend.DataQuery) (standard, base, chain []backend.DataQuery) {
	for _, query := range queries {
		model, err := decodeQuery(query)
		if err != nil {
			standard = append(standard, query)
			continue
		}
		switch effectiveSearchType(model) {
		case "base":
			base = append(base, query)
		case "chain":
			chain = append(chain, query)
		default:
			standard = append(standard, query)
		}
	}
	return
}

func decodeQuery(query backend.DataQuery) (queryModel, error) {
	var model queryModel
	if err := json.Unmarshal(query.JSON, &model); err != nil {
		return model, fmt.Errorf("decode query %q: %w", query.RefID, err)
	}
	return model, nil
}

func effectiveSearchType(model queryModel) string {
	if model.SearchType == "standard" || model.SearchType == "base" || model.SearchType == "chain" {
		return model.SearchType
	}
	if model.Mode == "base" || model.Mode == "chain" {
		return model.Mode
	}
	return "standard"
}

func (d *Datasource) queryResponse(
	ctx context.Context,
	query backend.DataQuery,
	req *backend.QueryDataRequest,
	baseJobs map[string]cachedSearch,
) backend.DataResponse {
	model, err := decodeQuery(query)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}
	if strings.TrimSpace(model.QueryText) == "" {
		return backend.DataResponse{Frames: data.Frames{data.NewFrame(model.QueryText)}}
	}

	switch effectiveSearchType(model) {
	case "base":
		job, err := d.resolveBaseSearch(ctx, query, model, req, baseJobs)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadGateway, err.Error())
		}
		if !model.ReturnBaseResults {
			return backend.DataResponse{Frames: data.Frames{data.NewFrame("base")}}
		}
		result, err := d.getResults(ctx, job.sid, query.RefID)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadGateway, err.Error())
		}
		return d.resultResponse(query.RefID, result)
	case "chain":
		job, ok := baseJobs[model.BaseSearchRefID]
		if !ok {
			job, ok = d.lookupCachedSearch(req, model.BaseSearchRefID, model)
		}
		if !ok {
			return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("chain search %q could not resolve base search %q", query.RefID, model.BaseSearchRefID))
		}
		result, err := d.runSearch(ctx, query, model, job.sid, d.chain)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadGateway, err.Error())
		}
		return d.resultResponse(query.RefID, result.queryResult)
	default:
		result, err := d.runSearch(ctx, query, model, "", d.standard)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadGateway, err.Error())
		}
		return d.resultResponse(query.RefID, result.queryResult)
	}
}

func (d *Datasource) resultResponse(refID string, result queryResult) backend.DataResponse {
	frame := result.frame(refID)
	if result.warning != "" {
		frame.AppendNotices(data.Notice{Severity: data.NoticeSeverityWarning, Text: result.warning, Inspect: data.InspectTypeData})
	}
	return backend.DataResponse{Frames: data.Frames{frame}}
}

type queryResult struct {
	fields  []fieldResult
	rows    []map[string]interface{}
	warning string
}

type fieldResult struct {
	name string
}

func (r queryResult) frame(refID string) *data.Frame {
	frame := data.NewFrame("response")
	frame.RefID = refID

	for _, field := range r.fields {
		frame.Fields = append(frame.Fields, newField(field.name, r.rows))
	}
	return frame
}

func newField(name string, rows []map[string]interface{}) *data.Field {
	if name == "_time" {
		values := make([]*time.Time, len(rows))
		for i, row := range rows {
			if value, ok := parseTime(row[name]); ok {
				values[i] = &value
			}
		}
		return data.NewField(name, nil, values)
	}

	allNumeric := false
	for _, row := range rows {
		if value := row[name]; value != nil && value != "" {
			allNumeric = true
			if _, ok := parseNumber(value); !ok {
				allNumeric = false
				break
			}
		}
	}
	if allNumeric {
		values := make([]*float64, len(rows))
		for i, row := range rows {
			if value, ok := parseNumber(row[name]); ok {
				values[i] = &value
			}
		}
		return data.NewField(name, nil, values)
	}

	values := make([]*string, len(rows))
	for i, row := range rows {
		if value, ok := stringify(row[name]); ok {
			values[i] = &value
		}
	}
	return data.NewField(name, nil, values)
}

func parseNumber(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	default:
		return 0, false
	}
}

func stringify(value interface{}) (string, bool) {
	if value == nil {
		return "", false
	}
	if text, ok := value.(string); ok {
		if text == "" {
			return "", false
		}
		return text, true
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value), true
	}
	return string(encoded), true
}

func parseTime(value interface{}) (time.Time, bool) {
	if value == nil || value == "" {
		return time.Time{}, false
	}
	if number, ok := parseNumber(value); ok {
		if math.Abs(number) < 1e12 {
			seconds := int64(number)
			nanos := int64((number - float64(seconds)) * 1e9)
			return time.Unix(seconds, nanos).UTC(), true
		}
		return time.UnixMilli(int64(number)).UTC(), true
	}
	text, ok := value.(string)
	if !ok {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.000 -0700", "2006-01-02 15:04:05 -0700", "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, text); err == nil {
			return parsed.UTC(), true
		}
	}
	return time.Time{}, false
}

func (d *Datasource) resolveBaseSearch(
	ctx context.Context,
	query backend.DataQuery,
	model queryModel,
	req *backend.QueryDataRequest,
	baseJobs map[string]cachedSearch,
) (cachedSearch, error) {
	identifier := model.SearchID
	if identifier == "" {
		identifier = query.RefID
	}
	key := d.cacheKey(req, query, model, identifier)
	if job, ok := d.getCached(key); ok {
		baseJobs[identifier] = job
		baseJobs[query.RefID] = job
		return job, nil
	}

	sid, err := d.startSearch(ctx, query, model, "", d.base)
	if err != nil {
		return cachedSearch{}, err
	}
	job := cachedSearch{sid: sid, searchID: identifier, refID: query.RefID, createdAt: time.Now()}
	d.putCached(key, job)
	baseJobs[identifier] = job
	baseJobs[query.RefID] = job
	return job, nil
}

func (d *Datasource) cacheKey(req *backend.QueryDataRequest, query backend.DataQuery, model queryModel, identifier string) string {
	rangeKey := query.TimeRange.From.String() + "|" + query.TimeRange.To.String()
	if model.UseDashboardTimeRange != nil && !*model.UseDashboardTimeRange {
		rangeKey = model.Earliest + "|" + model.Latest
	}
	return strings.Join([]string{dashboardNamespace(req), identifier, model.QueryText, rangeKey}, "|")
}

func dashboardNamespace(req *backend.QueryDataRequest) string {
	namespace := req.GetHTTPHeader("X-Dashboard-Uid")
	if namespace == "" {
		namespace = req.GetHTTPHeader("X-Dashboard-Title")
	}
	if namespace == "" {
		namespace = "unknown-dashboard"
	}
	return namespace
}

func (d *Datasource) getCached(key string) (cachedSearch, bool) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	job, ok := d.cache[key]
	if !ok || time.Since(job.createdAt) >= d.cacheTTL {
		delete(d.cache, key)
		return cachedSearch{}, false
	}
	return job, true
}

func (d *Datasource) putCached(key string, job cachedSearch) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	d.cache[key] = job
}

func (d *Datasource) lookupCachedSearch(req *backend.QueryDataRequest, identifier string, model queryModel) (cachedSearch, bool) {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	for key, job := range d.cache {
		if job.searchID == identifier && time.Since(job.createdAt) < d.cacheTTL && strings.HasPrefix(key, dashboardNamespace(req)+"|") {
			return job, true
		}
	}
	return cachedSearch{}, false
}

func (d *Datasource) runSearch(ctx context.Context, query backend.DataQuery, model queryModel, baseSID string, timeout time.Duration) (queryResultWithSID, error) {
	if len(strings.TrimSpace(model.QueryText)) < 1 {
		return queryResultWithSID{queryResult: queryResult{}}, nil
	}

	sid, err := d.startSearch(ctx, query, model, baseSID, timeout)
	if err != nil {
		return queryResultWithSID{}, err
	}
	result, err := d.getResults(ctx, sid, query.RefID)
	if err != nil {
		_ = d.cancelSearch(ctx, sid)
		return queryResultWithSID{}, err
	}
	return queryResultWithSID{queryResult: result, sid: sid}, nil
}

func (d *Datasource) startSearch(ctx context.Context, query backend.DataQuery, model queryModel, baseSID string, timeout time.Duration) (string, error) {
	if len(strings.TrimSpace(model.QueryText)) < 1 {
		return "", nil
	}

	searchText := strings.TrimSpace(model.QueryText)
	if baseSID != "" {
		if strings.HasPrefix(searchText, "|") {
			searchText = "| loadjob " + baseSID + " " + searchText
		} else {
			searchText = "| loadjob " + baseSID + " | " + searchText
		}
	} else if !strings.HasPrefix(searchText, "|") {
		searchText = "search " + searchText
	}

	earliest, latest := queryTimes(query, model)
	form := url.Values{
		"search":        {searchText},
		"output_mode":   {"json"},
		"earliest_time": {earliest},
		"latest_time":   {latest},
	}

	resp, err := d.doFormRequest(ctx, http.MethodPost, "/services/search/jobs", form)
	if err != nil {
		return "", err
	}
	var creation struct {
		SID string `json:"sid"`
	}
	if err := decodeResponse(resp, &creation, d.maxBytes); err != nil {
		return "", err
	}
	if creation.SID == "" {
		return "", errors.New("Splunk search returned no SID")
	}

	status, err := d.waitForCompletion(ctx, creation.SID, timeout)
	if err != nil {
		_ = d.cancelSearch(ctx, creation.SID)
		return "", err
	}
	if status.state == "FAILED" {
		_ = d.cancelSearch(ctx, creation.SID)
		return "", fmt.Errorf("Splunk search failed (sid=%s): %s", creation.SID, strings.Join(status.messages, "; "))
	}
	return creation.SID, nil
}

type queryResultWithSID struct {
	queryResult
	sid string
}

type searchStatus struct {
	state    string
	messages []string
}

func (d *Datasource) waitForCompletion(ctx context.Context, sid string, timeout time.Duration) (searchStatus, error) {
	deadline := time.Now().Add(timeout)
	for {
		status, err := d.searchStatus(ctx, sid)
		if err != nil {
			return searchStatus{}, err
		}
		if status.state == "DONE" || status.state == "PAUSED" || status.state == "FAILED" {
			return status, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return status, fmt.Errorf("Splunk search timed out after %s (sid=%s)", timeout, sid)
		}
		select {
		case <-ctx.Done():
			return searchStatus{}, ctx.Err()
		case <-time.After(minDuration(pollInterval, remaining)):
		}
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (d *Datasource) searchStatus(ctx context.Context, sid string) (searchStatus, error) {
	resp, err := d.doRequest(ctx, http.MethodGet, "/services/search/jobs/"+url.PathEscape(sid)+"?output_mode=json", nil)
	if err != nil {
		return searchStatus{}, err
	}
	var body struct {
		Messages json.RawMessage `json:"messages"`
		Entry    []struct {
			Content struct {
				DispatchState string          `json:"dispatchState"`
				Messages      json.RawMessage `json:"messages"`
			} `json:"content"`
		} `json:"entry"`
	}
	if err := decodeResponse(resp, &body, d.maxBytes); err != nil {
		return searchStatus{}, err
	}
	status := searchStatus{state: "UNKNOWN"}
	if len(body.Entry) > 0 {
		status.state = body.Entry[0].Content.DispatchState
		status.messages = append(status.messages, decodeMessages(body.Entry[0].Content.Messages)...)
	}
	status.messages = append(status.messages, decodeMessages(body.Messages)...)
	return status, nil
}

func decodeMessages(raw json.RawMessage) []string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var values []struct {
		Text    string `json:"text"`
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &values) == nil {
		messages := make([]string, 0, len(values))
		for _, value := range values {
			if value.Text != "" {
				messages = append(messages, value.Text)
			} else if value.Message != "" {
				messages = append(messages, value.Message)
			}
		}
		return messages
	}
	var value string
	if json.Unmarshal(raw, &value) == nil && value != "" {
		return []string{value}
	}
	return nil
}

func (d *Datasource) cancelSearch(ctx context.Context, sid string) error {
	form := url.Values{"action": {"cancel"}}
	resp, err := d.doFormRequest(ctx, http.MethodPost, "/services/search/jobs/"+url.PathEscape(sid)+"/control", form)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func (d *Datasource) getResults(ctx context.Context, sid, refID string) (queryResult, error) {
	result := queryResult{}
	offset := 0
	bytesRead := int64(0)
	for page := 0; page < d.maxPages; page++ {
		remaining := d.maxRows - len(result.rows)
		if remaining <= 0 {
			result.warning = fmt.Sprintf("Splunk results truncated at %d rows.", d.maxRows)
			break
		}
		count := min(defaultPageSize, remaining)
		path := fmt.Sprintf("/services/search/jobs/%s/results?output_mode=json&offset=%d&count=%d", url.PathEscape(sid), offset, count)
		resp, err := d.doRequest(ctx, http.MethodGet, path, nil)
		if err != nil {
			return result, err
		}
		body, err := readResponse(resp, d.maxBytes)
		if err != nil {
			return result, err
		}
		bytesRead += int64(len(body))
		if bytesRead > d.maxBytes {
			result.warning = fmt.Sprintf("Splunk results truncated after responses exceeded %d bytes.", d.maxBytes)
			break
		}

		var pageBody struct {
			Fields []struct {
				Name string `json:"name"`
			} `json:"fields"`
			Results []map[string]interface{} `json:"results"`
		}
		if err := json.Unmarshal(body, &pageBody); err != nil {
			return result, fmt.Errorf("decode Splunk results: %w", err)
		}
		if len(result.fields) == 0 {
			for _, field := range pageBody.Fields {
				result.fields = append(result.fields, fieldResult{name: field.Name})
			}
		}
		if len(pageBody.Results) == 0 {
			break
		}
		retainedResults := pageBody.Results
		if len(retainedResults) > remaining {
			retainedResults = retainedResults[:remaining]
			result.warning = fmt.Sprintf("Splunk results truncated at %d rows.", d.maxRows)
		}
		result.rows = append(result.rows, retainedResults...)
		offset += len(pageBody.Results)
		if len(result.rows) >= d.maxRows {
			break
		}
		if len(pageBody.Results) < count {
			break
		}
	}
	if len(result.rows) >= d.maxRows && result.warning == "" {
		result.warning = fmt.Sprintf("Splunk results truncated at %d rows.", d.maxRows)
	}
	return result, nil
}

func queryTimes(query backend.DataQuery, model queryModel) (string, string) {
	useDashboard := model.UseDashboardTimeRange == nil || *model.UseDashboardTimeRange
	if !useDashboard {
		earliest := model.Earliest
		latest := model.Latest
		if earliest == "" {
			earliest = "-30d@d"
		}
		if latest == "" {
			latest = "now"
		}
		return earliest, latest
	}
	return strconv.FormatInt(query.TimeRange.From.Unix(), 10), strconv.FormatInt(query.TimeRange.To.Unix(), 10)
}

func (d *Datasource) baseURL(path string) string {
	return strings.TrimRight(d.settings.URL, "/") + path
}

func (d *Datasource) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, d.baseURL(path), body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	response, err := d.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Splunk request: %w", err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message := responseError(response)
		response.Body.Close()
		return nil, fmt.Errorf("Splunk returned HTTP %d: %s", response.StatusCode, message)
	}
	return response, nil
}

func (d *Datasource) doFormRequest(ctx context.Context, method, path string, values url.Values) (*http.Response, error) {
	return d.doRequest(ctx, method, path, strings.NewReader(values.Encode()))
}

func decodeResponse(response *http.Response, target interface{}, limit int64) error {
	body, err := readResponse(response, limit)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("decode Splunk response: %w", err)
	}
	return nil
}

func readResponse(response *http.Response, limit int64) ([]byte, error) {
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read Splunk response: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("Splunk response exceeded %d bytes", limit)
	}
	return body, nil
}

func responseError(response *http.Response) string {
	body, err := io.ReadAll(io.LimitReader(response.Body, 16*1024))
	if err != nil || len(body) == 0 {
		return response.Status
	}
	return strings.TrimSpace(string(body))
}

var _ backend.QueryDataHandler = (*Datasource)(nil)
var _ backend.CheckHealthHandler = (*Datasource)(nil)
