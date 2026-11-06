import {
  CustomVariableSupport,
  DataFrame,
  DataQueryRequest,
  DataQueryResponse,
  LoadingState,
  MetricFindValue,
  ScopedVars,
  dateTime,
} from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';
import { Observable, defer, lastValueFrom, of } from 'rxjs';
import { map } from 'rxjs/operators';

import { SplunkQuery, SplunkDataSourceOptions } from './types';
import { SplunkVariableQuery, VariableQueryEditor } from './VariableQueryEditor';

const DEFAULT_VARIABLE_QUERY_RANGE_MS = 60 * 60 * 1000;
type VariableQueryInput = SplunkQuery | SplunkVariableQuery | string | Record<string, unknown>;
type SearchType = NonNullable<SplunkQuery['searchType']>;

const isSearchType = (value: unknown): value is SearchType =>
  value === 'standard' || value === 'base' || value === 'chain';

const hasValue = (value: unknown) => value !== undefined && value !== null && value !== '';

const toMetricFindValues = (frames: DataFrame[]): MetricFindValue[] => {
  const values: MetricFindValue[] = [];
  const seen = new Set<string>();

  for (const frame of frames) {
    const textField = frame.fields.find((field) => field.name === '__text');
    const valueField = frame.fields.find((field) => field.name === '__value');
    const fields = textField ? [textField] : frame.fields;

    for (let row = 0; row < frame.length; row++) {
      for (const field of fields) {
        const rawText = field.values[row];
        if (!hasValue(rawText)) {
          continue;
        }

        const text = String(rawText);
        const rawValue = textField ? valueField?.values[row] : undefined;
        const value = hasValue(rawValue) ? (typeof rawValue === 'number' ? rawValue : String(rawValue)) : undefined;
        const key = value === undefined ? text : JSON.stringify([text, value]);
        if (!seen.has(key)) {
          seen.add(key);
          values.push(value === undefined ? { text } : { text, value });
        }
      }
    }
  }

  return values;
};

class SplunkCustomVariableSupport extends CustomVariableSupport<
  DataSource,
  SplunkVariableQuery,
  SplunkQuery,
  SplunkDataSourceOptions
> {
  editor = VariableQueryEditor;

  constructor(private readonly datasource: DataSource) {
    super();
  }

  query(request: DataQueryRequest<SplunkVariableQuery>) {
    return this.datasource.queryVariable(request.targets?.[0] ?? '', request as DataQueryRequest<SplunkQuery>);
  }
}

export class DataSource extends DataSourceWithBackend<SplunkQuery, SplunkDataSourceOptions> {
  variables = new SplunkCustomVariableSupport(this);
  private variableRequestCounter = 0;

  applyTemplateVariables(query: SplunkQuery, scopedVars: ScopedVars): SplunkQuery {
    const templateSrv = getTemplateSrv();
    const replace = (value: string | undefined) =>
      typeof value === 'string' ? templateSrv.replace(value, scopedVars) : undefined;

    return {
      ...query,
      queryText: replace(query.queryText) ?? '',
      earliest: replace(query.earliest),
      latest: replace(query.latest),
      searchId: replace(query.searchId),
      baseSearchRefId: replace(query.baseSearchRefId),
    };
  }

  async metricFindQuery(
    query: VariableQueryInput,
    options?: DataQueryRequest<SplunkQuery>
  ): Promise<MetricFindValue[]> {
    return (await lastValueFrom(this.queryVariable(query, options))).data;
  }

  queryVariable(
    query: VariableQueryInput,
    options?: DataQueryRequest<SplunkQuery>
  ): Observable<{ data: MetricFindValue[] }> {
    const target = this.normalizeVariableQuery(query);
    if (!target) {
      return of({ data: [] });
    }

    // Keep custom variable lookups observable so Grafana can cancel stale requests.
    // Dashboard, Explore, alert and variable searches all use the same Go backend.
    return defer(() => super.query(this.createVariableRequest(target, options))).pipe(
      map((response: DataQueryResponse) => {
        // Grafana still uses the legacy single-error property for HTTP failures.
        // eslint-disable-next-line deprecation/deprecation
        const error = response.errors?.[0] ?? response.error;
        if (error || response.state === LoadingState.Error) {
          throw new Error(error?.message || 'Splunk variable query failed.');
        }
        return { data: toMetricFindValues(response.data) };
      })
    );
  }

  private normalizeVariableQuery(rawQuery: VariableQueryInput): SplunkQuery | null {
    const query = typeof rawQuery === 'string' ? { queryText: rawQuery } : rawQuery;
    if (!query || typeof query !== 'object') {
      return null;
    }

    const record = query as Record<string, unknown>;
    const text = typeof record.queryText === 'string' ? record.queryText : record.query;
    if (typeof text !== 'string' || !text.trim()) {
      return null;
    }

    const searchType = isSearchType(record.searchType)
      ? record.searchType
      : record.mode === 'base' || record.mode === 'chain'
        ? record.mode
        : 'standard';

    return {
      ...(record as Partial<SplunkQuery>),
      refId: typeof record.refId === 'string' && record.refId ? record.refId : 'metricFindQuery',
      queryText: text.trim(),
      queryType: 'variable',
      searchType,
      // Variables need values even when a saved query uses a hidden base search.
      returnBaseResults: searchType === 'base' ? true : record.returnBaseResults === true,
      hide: false,
    };
  }

  private createVariableRequest(
    query: SplunkQuery,
    options?: DataQueryRequest<SplunkQuery>
  ): DataQueryRequest<SplunkQuery> {
    const now = Date.now();
    const fallbackRange: DataQueryRequest<SplunkQuery>['range'] = {
      from: dateTime(now - DEFAULT_VARIABLE_QUERY_RANGE_MS),
      to: dateTime(now),
      raw: { from: 'now-1h', to: 'now' },
    };

    return {
      ...options,
      requestId: options?.requestId || `splunk-variable-${query.refId}-${++this.variableRequestCounter}`,
      app: options?.app ?? 'variable',
      interval: options?.interval ?? '1s',
      intervalMs: options?.intervalMs ?? 1000,
      timezone: options?.timezone ?? 'browser',
      startTime: options?.startTime ?? now,
      scopedVars: options?.scopedVars ?? {},
      // Never execute other panel or variable targets passed by the caller.
      targets: [query],
      range: options?.range?.from && options?.range?.to ? options.range : fallbackRange,
    };
  }
}
