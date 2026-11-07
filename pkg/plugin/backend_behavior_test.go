package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func standardQuery(refID string) backend.DataQuery {
	return backend.DataQuery{RefID: refID, JSON: json.RawMessage(`{"queryText":"index=main","searchType":"standard"}`)}
}

func TestQueryTimeoutCoversEveryHTTPStageAndCancelsCreatedJobs(t *testing.T) {
	for _, stage := range []string{"creation", "status", "results"} {
		t.Run(stage, func(t *testing.T) {
			cancelled := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch {
				case strings.HasSuffix(request.URL.Path, "/control"):
					if err := request.ParseForm(); err != nil || request.Form.Get("action") != "cancel" {
						t.Errorf("expected cancel control request, got %v %q", err, request.Form.Get("action"))
					}
					cancelled <- struct{}{}
					_, _ = io.WriteString(writer, `{}`)
				case request.URL.Path == "/services/search/jobs":
					if stage == "creation" {
						_, _ = io.Copy(io.Discard, request.Body)
						<-request.Context().Done()
						return
					}
					_, _ = io.WriteString(writer, `{"sid":"timeout-job"}`)
				case strings.HasSuffix(request.URL.Path, "/results"):
					writer.WriteHeader(http.StatusOK)
					writer.(http.Flusher).Flush()
					<-request.Context().Done()
				default:
					if stage == "status" {
						<-request.Context().Done()
						return
					}
					_, _ = io.WriteString(writer, `{"entry":[{"content":{"dispatchState":"DONE"}}]}`)
				}
			}))
			defer server.Close()
			datasource := testDatasource(server)
			datasource.standard = 40 * time.Millisecond
			started := time.Now()
			response, err := datasource.QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{standardQuery("A")}})
			if err != nil {
				t.Fatal(err)
			}
			result := response.Responses["A"]
			if result.Error == nil || result.Status != backend.StatusTimeout || result.ErrorSource != backend.ErrorSourceDownstream {
				t.Fatalf("expected downstream timeout, got %#v", result)
			}
			if time.Since(started) > time.Second {
				t.Fatal("query ignored its configured timeout")
			}
			if stage != "creation" {
				select {
				case <-cancelled:
				case <-time.After(time.Second):
					t.Fatal("timed out search was not cancelled with an independent context")
				}
			}
		})
	}
}

func TestQueryPreservesDownstreamHTTPStatus(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				http.Error(writer, "Splunk refused the request", status)
			}))
			defer server.Close()
			response, err := testDatasource(server).QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{standardQuery("A")}})
			if err != nil {
				t.Fatal(err)
			}
			result := response.Responses["A"]
			if result.Error == nil || int(result.Status) != status || result.ErrorSource != backend.ErrorSourceDownstream {
				t.Fatalf("expected downstream status %d, got %#v", status, result)
			}
		})
	}
}

func TestIndependentQueriesRunWithBoundedConcurrency(t *testing.T) {
	started := make(chan struct{}, 10)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var active, maximum, searches atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost:
			current := active.Add(1)
			for previous := maximum.Load(); current > previous; previous = maximum.Load() {
				if maximum.CompareAndSwap(previous, current) {
					break
				}
			}
			id := searches.Add(1)
			started <- struct{}{}
			select {
			case <-release:
			case <-request.Context().Done():
			}
			active.Add(-1)
			_, _ = fmt.Fprintf(writer, `{"sid":"job-%d"}`, id)
		case strings.HasSuffix(request.URL.Path, "/results"):
			_, _ = io.WriteString(writer, `{"fields":[{"name":"count"}],"results":[{"count":"1"}]}`)
		default:
			_, _ = io.WriteString(writer, `{"entry":[{"content":{"dispatchState":"DONE"}}]}`)
		}
	}))
	defer server.Close()
	datasource := testDatasource(server)
	datasource.standard = 5 * time.Second
	queries := make([]backend.DataQuery, queryConcurrency+2)
	for index := range queries {
		queries[index] = standardQuery(fmt.Sprint(index))
	}
	done := make(chan *backend.QueryDataResponse, 1)
	go func() {
		response, _ := datasource.QueryData(context.Background(), &backend.QueryDataRequest{Queries: queries})
		done <- response
	}()
	for index := 0; index < queryConcurrency; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			releaseOnce.Do(func() { close(release) })
			t.Fatal("independent queries were serialized")
		}
	}
	if got := active.Load(); got != queryConcurrency {
		t.Fatalf("expected %d active queries, got %d", queryConcurrency, got)
	}
	releaseOnce.Do(func() { close(release) })
	response := <-done
	if response == nil || len(response.Responses) != len(queries) {
		t.Fatalf("missing query responses: %#v", response)
	}
	if maximum.Load() > queryConcurrency {
		t.Fatalf("query concurrency exceeded limit: %d", maximum.Load())
	}
}

func TestHTTPClientForwardsRequestIdentityAndPreservesExplicitToken(t *testing.T) {
	for _, explicitToken := range []string{"", "Splunk legacy-token", "bare-token"} {
		t.Run(explicitToken, func(t *testing.T) {
			expected := "Bearer forwarded-user"
			if explicitToken != "" {
				expected = explicitToken
				if !strings.Contains(explicitToken, " ") {
					expected = "Bearer " + explicitToken
				}
			}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if authorization := request.Header.Get("Authorization"); authorization != expected {
					t.Errorf("unexpected authorization scheme/value: %q", authorization)
					writer.WriteHeader(http.StatusUnauthorized)
					return
				}
				if err := request.ParseForm(); err != nil || request.Form.Get("search") != "| makeresults | stats count" {
					t.Errorf("health check should generate its own data, got %v %q", err, request.Form.Get("search"))
				}
				_, _ = io.WriteString(writer, `{}`)
			}))
			defer server.Close()
			instance, err := NewDatasource(context.Background(), backend.DataSourceInstanceSettings{
				URL: server.URL, BasicAuthEnabled: true, BasicAuthUser: "basic-fallback",
				DecryptedSecureJSONData: map[string]string{"basicAuthPassword": "basic-password", "basicAuthToken": explicitToken},
			})
			if err != nil {
				t.Fatal(err)
			}
			datasource := instance.(*Datasource)
			defer datasource.Dispose()
			// This is the same contextual middleware mechanism the SDK's adapter
			// uses for Grafana-authorized OAuth and cookie forwarding.
			ctx := httpclient.WithContextualMiddleware(context.Background(), httpclient.MiddlewareFunc(func(options httpclient.Options, next http.RoundTripper) http.RoundTripper {
				return httpclient.RoundTripperFunc(func(request *http.Request) (*http.Response, error) {
					if options.ForwardHTTPHeaders && request.Header.Get("Authorization") == "" {
						request.Header.Set("Authorization", "Bearer forwarded-user")
					}
					return next.RoundTrip(request)
				})
			}))
			result, err := datasource.CheckHealth(ctx, &backend.CheckHealthRequest{})
			if err != nil || result.Status != backend.HealthStatusOk {
				t.Fatalf("health check did not forward configured identity: %#v %v", result, err)
			}
		})
	}
}

func TestDisposeClosesTheSDKHTTPConnectionPool(t *testing.T) {
	closed := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, `{}`)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			closed <- struct{}{}
		}
	}
	server.Start()
	defer server.Close()
	instance, err := NewDatasource(context.Background(), backend.DataSourceInstanceSettings{URL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	datasource := instance.(*Datasource)
	result, err := datasource.CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil || result.Status != backend.HealthStatusOk {
		t.Fatalf("health check failed: %#v %v", result, err)
	}
	datasource.Dispose()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("disposed datasource retained its idle SDK HTTP connection")
	}
}

func TestDurationOptionsAreFiniteAndBounded(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), -1, 0} {
		if got := secondsOrDefault(value, defaultStandardTimeout); got != defaultStandardTimeout {
			t.Errorf("invalid seconds %v produced %v", value, got)
		}
	}
	if secondsOrDefault(math.MaxFloat64, defaultStandardTimeout) != time.Hour || minutesOrDefault(math.MaxFloat64, defaultCacheTTL) != time.Hour {
		t.Fatal("oversized provisioned duration overflowed its supported maximum")
	}
}

func TestDuplicateBaseAliasesCannotRunOrFallBackToCachedResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		t.Error("invalid base aliases must be rejected before contacting Splunk")
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	response, err := testDatasource(server).QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{
		{RefID: "B", JSON: json.RawMessage(`{"queryText":"index=one","searchType":"base","searchId":" shared "}`)},
		{RefID: "C", JSON: json.RawMessage(`{"queryText":"index=two","searchType":"base","searchId":"shared"}`)},
		{RefID: "D", JSON: json.RawMessage(`{"queryText":"| stats count","searchType":"chain","baseSearchRefId":"shared"}`)},
		{RefID: "E", JSON: json.RawMessage(`{"queryText":"| stats count","searchType":"chain","baseSearchRefId":"B"}`)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, refID := range []string{"B", "C", "D", "E"} {
		if result := response.Responses[refID]; result.Error == nil || result.Status != backend.StatusBadRequest {
			t.Errorf("query %s should reject conflicting base aliases: %#v", refID, result)
		}
	}
}

func TestVariableFramesPreserveIdentifiersAndPrimitiveValues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost:
			_, _ = io.WriteString(writer, `{"sid":"variable-job"}`)
		case strings.HasSuffix(request.URL.Path, "/results"):
			_, _ = io.WriteString(writer, `{"fields":[{"name":"__text"},{"name":"__value"},{"name":"enabled"},{"name":"zero"}],"results":[{"__text":"001","__value":9007199254740993,"enabled":false,"zero":0}]}`)
		default:
			_, _ = io.WriteString(writer, `{"entry":[{"content":{"dispatchState":"DONE"}}]}`)
		}
	}))
	defer server.Close()
	query := standardQuery("A")
	query.QueryType = "variable"
	response, err := testDatasource(server).QueryData(context.Background(), &backend.QueryDataRequest{Queries: []backend.DataQuery{query}})
	if err != nil || response.Responses["A"].Error != nil {
		t.Fatalf("variable query failed: %#v %v", response, err)
	}
	frame := response.Responses["A"].Frames[0]
	for index, expected := range []string{"001", "9007199254740993", "false", "0"} {
		value, exists := frame.Fields[index].ConcreteAt(0)
		if frame.Fields[index].Type() != data.FieldTypeNullableString || !exists || value != expected {
			t.Errorf("variable field %s lost formatting: %#v, want %q", frame.Fields[index].Name, value, expected)
		}
	}
}

func TestResultPageLimitReportsTruncationOnlyForFullPages(t *testing.T) {
	for _, rows := range []int{defaultPageSize, 2} {
		t.Run(fmt.Sprint(rows), func(t *testing.T) {
			results := make([]map[string]string, rows)
			for index := range results {
				results[index] = map[string]string{"count": "1"}
			}
			payload, err := json.Marshal(map[string]interface{}{"fields": []map[string]string{{"name": "count"}}, "results": results})
			if err != nil {
				t.Fatal(err)
			}
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				requests.Add(1)
				if request.URL.Query().Get("offset") != "0" || request.URL.Query().Get("count") != fmt.Sprint(defaultPageSize) {
					t.Errorf("unexpected result page request: %s", request.URL.RawQuery)
				}
				_, _ = writer.Write(payload)
			}))
			defer server.Close()
			datasource := testDatasource(server)
			datasource.maxPages = 1
			datasource.maxRows = defaultMaxRows
			datasource.maxBytes = defaultMaxBytes
			result, err := datasource.getResults(context.Background(), "page-limit", "A")
			if err != nil || len(result.rows) != rows || requests.Load() != 1 {
				t.Fatalf("unexpected limited pagination result: rows=%d requests=%d err=%v", len(result.rows), requests.Load(), err)
			}
			if rows == defaultPageSize && !strings.Contains(result.warning, "page limit") {
				t.Fatal("a full final page was silently presented as complete results")
			}
			if rows < defaultPageSize && result.warning != "" {
				t.Fatalf("an exhausted short page was incorrectly marked as truncated: %s", result.warning)
			}
		})
	}
}
