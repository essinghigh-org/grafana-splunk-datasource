package plugin

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

const maxAlertFrameCells = 1_000_000

func isAlertRequest(req *backend.QueryDataRequest) bool {
	// Grafana preserves this legacy header's casing in the raw Headers map.
	return req != nil && (strings.EqualFold(req.Headers["FromAlert"], "true") || strings.EqualFold(req.GetHTTPHeader("FromAlert"), "true"))
}

func (r queryResult) alertingFrame(refID string) (*data.Frame, error) {
	if len(r.rows) == 0 {
		return r.frame(refID), nil
	}
	hasTime := false
	for _, field := range r.fields {
		if field.name == "_time" {
			hasTime = true
			break
		}
	}
	if hasTime {
		type timedRow struct {
			timestamp time.Time
			row       map[string]interface{}
		}
		rows := make([]timedRow, len(r.rows))
		for index, row := range r.rows {
			timestamp, ok := parseTime(row["_time"])
			if !ok {
				return nil, errors.New("Grafana Alerting requires a valid _time value in every time-series row")
			}
			rows[index] = timedRow{timestamp: timestamp, row: row}
		}
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].timestamp.Before(rows[j].timestamp) })
		r.rows = make([]map[string]interface{}, len(rows))
		for index, row := range rows {
			r.rows[index] = row.row
		}
	}

	frame := r.frame(refID)
	schema := frame.TimeSeriesSchema()
	switch schema.Type {
	case data.TimeSeriesTypeLong:
		wide, err := data.LongToWideWithLimit(frame, nil, maxAlertFrameCells)
		if err != nil {
			return nil, fmt.Errorf("prepare time-series data for Grafana Alerting: %w", err)
		}
		wide.RefID = refID
		return wide, nil
	case data.TimeSeriesTypeWide:
		for index := 1; index < frame.Rows(); index++ {
			previous, _ := frame.Fields[schema.TimeIndex].ConcreteAt(index - 1)
			current, _ := frame.Fields[schema.TimeIndex].ConcreteAt(index)
			if current.(time.Time).Equal(previous.(time.Time)) {
				return nil, errors.New("Grafana Alerting requires one value per timestamp; aggregate duplicate _time values in the search")
			}
		}
		frame.Meta = &data.FrameMeta{Type: data.FrameTypeTimeSeriesWide, TypeVersion: data.FrameTypeVersion{0, 1}}
		return frame, nil
	default:
		if hasTime {
			return nil, errors.New("Grafana Alerting requires numeric values; aggregate the search results with stats or timechart")
		}
		// Grafana also supports a table with one numeric value and string labels.
		numericFields := 0
		for _, field := range frame.Fields {
			if field.Type() == data.FieldTypeNullableFloat64 {
				numericFields++
			}
		}
		if numericFields != 1 {
			return nil, errors.New("Grafana Alerting tables require exactly one numeric column")
		}
		return frame, nil
	}
}
