package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func testDatasource(server *httptest.Server) *Datasource {
	return &Datasource{
		settings: backend.DataSourceInstanceSettings{URL: server.URL},
		client:   server.Client(),
		standard: time.Second,
		base:     time.Second,
		chain:    time.Second,
		cacheTTL: time.Minute,
		maxRows:  100,
		maxPages: 10,
		maxBytes: 1024 * 1024,
		cache:    make(map[string]cachedSearch),
	}
}

func TestQueryDataRunsSplunkSearchAndBuildsTypedFrame(t *testing.T) {
	var receivedSearch string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/services/search/jobs":
			if request.Method != http.MethodPost {
				t.Fatalf("expected POST for search creation, got %s", request.Method)
			}
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			receivedSearch = request.Form.Get("search")
			writer.Header().Set("Content-Type", "application/json")
			_, _ = writer.Write([]byte(`{"sid":"sid-1"}`))
		case "/services/search/jobs/sid-1":
			_, _ = writer.Write([]byte(`{"entry":[{"content":{"dispatchState":"DONE"}}]}`))
		case "/services/search/jobs/sid-1/results":
			if request.URL.Query().Get("offset") != "0" {
				t.Fatalf("expected first result page, got offset %s", request.URL.Query().Get("offset"))
			}
			_, _ = writer.Write([]byte(`{"fields":[{"name":"_time"},{"name":"host"},{"name":"count"}],"results":[{"_time":"2026-08-11T10:00:00Z","host":"api-1","count":"42"}]}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	datasource := testDatasource(server)
	response, err := datasource.QueryData(context.Background(), &backend.QueryDataRequest{
		Headers: map[string]string{"X-Dashboard-Uid": "dashboard-1"},
		Queries: []backend.DataQuery{{
			RefID: "A",
			JSON:  json.RawMessage(`{"queryText":"index=main host=api-1","searchType":"standard"}`),
			TimeRange: backend.TimeRange{
				From: time.Unix(1_000, 0),
				To:   time.Unix(2_000, 0),
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	result := response.Responses["A"]
	if result.Error != nil {
		t.Fatalf("query returned an error: %v", result.Error)
	}
	if receivedSearch != "search index=main host=api-1" {
		t.Fatalf("unexpected submitted search: %q", receivedSearch)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("expected one frame, got %d", len(result.Frames))
	}
	frame := result.Frames[0]
	if frame.RefID != "A" || frame.Rows() != 1 {
		t.Fatalf("unexpected frame identity/rows: refId=%q rows=%d", frame.RefID, frame.Rows())
	}
	if got := frame.Fields[0].Type(); got != data.FieldTypeNullableTime {
		t.Fatalf("expected nullable time field, got %s", got)
	}
	if got := frame.Fields[2].Type(); got != data.FieldTypeNullableFloat64 {
		t.Fatalf("expected nullable number field, got %s", got)
	}
	if got, ok := frame.Fields[2].ConcreteAt(0); !ok || got.(float64) != 42 {
		t.Fatalf("unexpected numeric value: %#v (ok=%t)", got, ok)
	}
}

func TestQueryDataRunsBaseBeforeChainAndKeepsBaseResultsHidden(t *testing.T) {
	var searches []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/services/search/jobs" && request.Method == http.MethodPost:
			if err := request.ParseForm(); err != nil {
				t.Fatal(err)
			}
			search := request.Form.Get("search")
			searches = append(searches, search)
			sid := "sid-base"
			if strings.Contains(search, "loadjob") {
				sid = "sid-chain"
			}
			_, _ = writer.Write([]byte(fmt.Sprintf(`{"sid":%q}`, sid)))
		case strings.HasPrefix(request.URL.Path, "/services/search/jobs/sid-") && !strings.HasSuffix(request.URL.Path, "/results"):
			_, _ = writer.Write([]byte(`{"entry":[{"content":{"dispatchState":"DONE"}}]}`))
		case request.URL.Path == "/services/search/jobs/sid-chain/results":
			_, _ = writer.Write([]byte(`{"fields":[{"name":"count"}],"results":[{"count":"1"}]}`))
		case request.URL.Path == "/services/search/jobs/sid-base/results":
			t.Error("hidden base search should not fetch results")
			http.Error(writer, "unexpected base results request", http.StatusInternalServerError)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	datasource := testDatasource(server)
	response, err := datasource.QueryData(context.Background(), &backend.QueryDataRequest{
		Headers: map[string]string{"X-Dashboard-Uid": "dashboard-1"},
		Queries: []backend.DataQuery{
			{
				RefID: "C",
				JSON:  json.RawMessage(`{"queryText":"| stats count","searchType":"chain","baseSearchRefId":"base-search"}`),
			},
			{
				RefID: "B",
				JSON:  json.RawMessage(`{"queryText":"index=main","searchType":"base","searchId":"base-search"}`),
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Responses["C"].Error != nil {
		t.Fatalf("chain query returned an error: %v", response.Responses["C"].Error)
	}
	if len(response.Responses["B"].Frames) != 1 || response.Responses["B"].Frames[0].Rows() != 0 {
		t.Fatal("expected a hidden base frame without rows")
	}
	if len(searches) != 2 || !strings.Contains(searches[1], "loadjob sid-base") {
		t.Fatalf("expected base search followed by loadjob chain, got %q", searches)
	}
}

func TestCheckHealthUsesSplunkOneshotSearch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/services/search/jobs" || request.Method != http.MethodPost {
			http.NotFound(writer, request)
			return
		}
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if request.Form.Get("exec_mode") != "oneshot" {
			t.Fatalf("expected oneshot health check, got %q", request.Form.Get("exec_mode"))
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	result, err := testDatasource(server).CheckHealth(context.Background(), &backend.CheckHealthRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != backend.HealthStatusOk {
		t.Fatalf("expected healthy datasource, got %s (%s)", result.Status, result.Message)
	}
}

func TestQueryTimesHonorsFixedRange(t *testing.T) {
	useDashboard := false
	earliest, latest := queryTimes(backend.DataQuery{TimeRange: backend.TimeRange{From: time.Unix(1, 0), To: time.Unix(2, 0)}}, queryModel{
		UseDashboardTimeRange: &useDashboard,
		Earliest:              "-7d@d",
		Latest:                "now",
	})
	if earliest != "-7d@d" || latest != "now" {
		t.Fatalf("unexpected fixed range: %q, %q", earliest, latest)
	}
}
