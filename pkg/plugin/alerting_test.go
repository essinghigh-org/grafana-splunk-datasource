package plugin

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

func TestAlertConversionLimitsTimestampAndLabelExpansion(t *testing.T) {
	result := queryResult{fields: []fieldResult{{name: "_time"}, {name: "host"}, {name: "count"}}}
	for index := 0; index < 1001; index++ {
		result.rows = append(result.rows, map[string]interface{}{
			"_time": time.Unix(int64(index), 0).Format(time.RFC3339),
			"host":  fmt.Sprintf("host-%d", index),
			"count": "1",
		})
	}
	if _, err := result.alertingFrame("A"); !errors.Is(err, data.ErrorWideFrameTooLarge) {
		t.Fatalf("expected bounded wide-frame expansion, got %v", err)
	}
}

func TestAlertQueriesConvertSortedLabeledTimeSeriesWithoutChangingDashboardTables(t *testing.T) {
	result := queryResult{
		fields: []fieldResult{{name: "_time"}, {name: "host"}, {name: "count"}},
		rows: []map[string]interface{}{
			{"_time": "2026-11-06T12:01:00Z", "host": "api-1", "count": "42"},
			{"_time": "2026-11-06T12:01:00Z", "host": "api-2", "count": "7"},
			{"_time": "2026-11-06T12:00:00Z", "host": "api-1", "count": "40"},
			{"_time": "2026-11-06T12:00:00Z", "host": "api-2", "count": "6"},
		},
		warning: "Partial results",
	}
	datasource := &Datasource{}
	query := backend.DataQuery{RefID: "A"}
	dashboard := datasource.resultResponse(query, result, &backend.QueryDataRequest{})
	if dashboard.Error != nil || len(dashboard.Frames) != 1 || dashboard.Frames[0].Rows() != 4 || dashboard.Frames[0].Fields[1].Name != "host" {
		t.Fatalf("dashboard table changed: %#v", dashboard)
	}
	if result.rows[0]["_time"] != "2026-11-06T12:01:00Z" {
		t.Fatal("dashboard row order changed")
	}

	request := &backend.QueryDataRequest{Headers: map[string]string{"FromAlert": "true"}}
	alert := datasource.resultResponse(query, result, request)
	if alert.Error != nil || len(alert.Frames) != 1 {
		t.Fatalf("alert conversion failed: %#v", alert)
	}
	frame := alert.Frames[0]
	if frame.RefID != "A" || frame.Rows() != 2 || len(frame.Fields) != 3 || frame.Meta == nil || frame.Meta.Type != data.FrameTypeTimeSeriesWide {
		t.Fatalf("unexpected alert frame shape: %#v", frame)
	}
	if frame.Fields[1].Labels["host"] != "api-1" || frame.Fields[2].Labels["host"] != "api-2" {
		t.Fatalf("missing alert series labels: %#v %#v", frame.Fields[1].Labels, frame.Fields[2].Labels)
	}
	first, ok := frame.Fields[0].ConcreteAt(0)
	if !ok || !first.(time.Time).Equal(time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("alert timestamps are not ascending: %#v", first)
	}
	if value, _ := frame.Fields[1].ConcreteAt(0); value != float64(40) {
		t.Fatalf("alert conversion changed values: %#v", value)
	}
	if len(frame.Meta.Notices) != 1 || frame.Meta.Notices[0].Text != result.warning {
		t.Fatal("alert frame lost its truncation notice")
	}
	if result.rows[0]["_time"] != "2026-11-06T12:01:00Z" {
		t.Fatal("alert conversion mutated the original dashboard row order")
	}
}

func TestAlertQueriesSupportWideSeriesAndNumericTables(t *testing.T) {
	for name, result := range map[string]queryResult{
		"wide": {
			fields: []fieldResult{{name: "_time"}, {name: "count"}},
			rows: []map[string]interface{}{
				{"_time": "2026-11-06T12:01:00Z", "count": "42"},
				{"_time": "2026-11-06T12:00:00Z", "count": "40"},
			},
		},
		"numeric table": {
			fields: []fieldResult{{name: "host"}, {name: "count"}},
			rows:   []map[string]interface{}{{"host": "api-1", "count": "42"}},
		},
		"no data": {},
	} {
		t.Run(name, func(t *testing.T) {
			request := &backend.QueryDataRequest{}
			request.SetHTTPHeader("FromAlert", "true")
			response := (&Datasource{}).resultResponse(backend.DataQuery{RefID: "A"}, result, request)
			if response.Error != nil || len(response.Frames) != 1 || response.Frames[0].RefID != "A" {
				t.Fatalf("supported alert data was rejected: %#v", response)
			}
		})
	}
}

func TestAlertQueriesRejectUnusableTimeSeries(t *testing.T) {
	for name, result := range map[string]queryResult{
		"logs": {
			fields: []fieldResult{{name: "_time"}, {name: "_raw"}},
			rows:   []map[string]interface{}{{"_time": "2026-11-06T12:00:00Z", "_raw": "request failed"}},
		},
		"missing time": {
			fields: []fieldResult{{name: "_time"}, {name: "count"}},
			rows:   []map[string]interface{}{{"_time": nil, "count": "42"}},
		},
		"duplicate time": {
			fields: []fieldResult{{name: "_time"}, {name: "count"}},
			rows: []map[string]interface{}{
				{"_time": "2026-11-06T12:00:00Z", "count": "42"},
				{"_time": "2026-11-06T12:00:00Z", "count": "43"},
			},
		},
		"ambiguous numeric table": {
			fields: []fieldResult{{name: "first"}, {name: "second"}},
			rows:   []map[string]interface{}{{"first": "1", "second": "2"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			response := (&Datasource{}).resultResponse(backend.DataQuery{RefID: "A"}, result, &backend.QueryDataRequest{Headers: map[string]string{"FromAlert": "true"}})
			if response.Error == nil || response.Status != backend.StatusBadRequest || response.ErrorSource != backend.ErrorSourceDownstream {
				t.Fatalf("expected actionable alert query error, got %#v", response)
			}
		})
	}
}
