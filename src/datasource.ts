import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { from, lastValueFrom } from 'rxjs';

import {
  CustomVariableSupport,
  DataQueryRequest,
  DataQueryResponse,
  DataSourceApi,
  DataSourceInstanceSettings,
  MetricFindValue,
  PartialDataFrame,
  FieldType,
  dateTime,
} from '@grafana/data';

import { SplunkQuery, SplunkDataSourceOptions, defaultQueryRequestResults, QueryRequestResults, BaseSearchResult } from './types';
import { SplunkVariableQuery, VariableQueryEditor } from './VariableQueryEditor';

const CACHE_TTL = 5 * 60 * 1000; // 5 minutes cache TTL
const DEFAULT_VARIABLE_QUERY_RANGE_MS = 60 * 60 * 1000;
const MAX_QUERY_EXECUTION_CONCURRENCY = 4;
const CHAIN_BASE_SEARCH_RETRY_ATTEMPTS = 3;
const CHAIN_BASE_SEARCH_RETRY_DELAY_MS = 100;
const SEARCH_POLL_INTERVAL_MS = 100;
const SEARCH_POLL_TIMEOUT_MS = 30 * 1000;
const SEARCH_TIMEOUT_ERROR_CODE = 'SPLUNK_SEARCH_TIMEOUT';

class SplunkSearchTimeoutError extends Error {
  readonly code = SEARCH_TIMEOUT_ERROR_CODE;

  constructor(
    readonly sid: string,
    readonly searchType: 'standard' | 'chain',
    readonly timeoutMs: number
  ) {
    super(`Splunk ${searchType} search timed out after ${timeoutMs}ms (sid=${sid}).`);
    this.name = 'SplunkSearchTimeoutError';
  }
}

type VariableQueryInput = SplunkQuery | SplunkVariableQuery | string | Record<string, unknown>;
type EffectiveSearchType = NonNullable<SplunkQuery['searchType']>;

const isSearchType = (value: unknown): value is EffectiveSearchType =>
  value === 'standard' || value === 'base' || value === 'chain';

const isLegacyMode = (value: unknown): value is NonNullable<SplunkQuery['mode']> => value === 'base' || value === 'chain';

const resolveSearchType = (searchType: unknown, mode: unknown): EffectiveSearchType => {
  if (isSearchType(searchType)) {
    return searchType;
  }

  if (isLegacyMode(mode)) {
    return mode;
  }

  return 'standard';
};

const getErrorMessage = (error: unknown): string => {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  if (typeof error === 'string') {
    return error;
  }

  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
};

const mapWithConcurrency = async <T, R>(
  items: T[],
  limit: number,
  mapper: (item: T, index: number) => Promise<R>
): Promise<R[]> => {
  if (items.length === 0) {
    return [];
  }

  const boundedLimit = Math.max(1, Math.min(limit, items.length));
  const results = new Array<R>(items.length);
  let nextIndex = 0;

  const worker = async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= items.length) {
        return;
      }

      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  };

  await Promise.all(Array.from({ length: boundedLimit }, () => worker()));
  return results;
};

export const resetBaseSearchStateForTests = () => {
  // Base-search cache state is scoped to each DataSource instance.
  // This helper is intentionally a no-op to preserve test compatibility.
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
    const variableQuery = request.targets?.[0] ?? '';

    return from(
      this.datasource
        .metricFindQuery(variableQuery, request as unknown as DataQueryRequest<SplunkQuery>)
        .then((metricFindValues) => ({ data: metricFindValues }))
    );
  }
}

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// Generate a cache key that includes query parameters to ensure proper invalidation
function generateCacheKey(query: SplunkQuery, options: DataQueryRequest<SplunkQuery>): string {
  const { range } = options;
  const from = Math.floor(range!.from.valueOf() / 1000);
  const to = Math.floor(range!.to.valueOf() / 1000);
  
  // Include query text, time range, and other relevant parameters in the cache key
  const keyComponents = [
    query.refId || '',
    query.searchId || '',
    query.queryText || '',
    from.toString(),
    to.toString(),
    JSON.stringify(options.scopedVars || {})
  ];
  
  return keyComponents.join('|');
}

export class DataSource extends DataSourceApi<SplunkQuery, SplunkDataSourceOptions> {
  url?: string;
  variables = new SplunkCustomVariableSupport(this);
  private readonly baseSearchCache: Map<string, BaseSearchResult> = new Map();
  private readonly baseSearchInflight: Map<string, Promise<BaseSearchResult>> = new Map();

  constructor(instanceSettings: DataSourceInstanceSettings<SplunkDataSourceOptions>) {
    super(instanceSettings);

    this.url = instanceSettings.url;
  }

  async metricFindQuery(query: VariableQueryInput, options?: DataQueryRequest<SplunkQuery>): Promise<MetricFindValue[]> {
    const normalizedQuery = this.normalizeMetricFindQuery(query);
    if (!normalizedQuery) {
      return [];
    }

    const safeOptions = this.createMetricFindOptions(normalizedQuery, options);
    const response = await this.doRequest(normalizedQuery, safeOptions);

    const frame: MetricFindValue[] = [];
    const seenTexts = new Set<string>();
    response.results.forEach((result: Record<string, unknown>) => {
      response.fields.forEach((field: string) => {
        const value = result[field];
        if (value === undefined || value === null || value === '') {
          return;
        }

        const text = String(value);
        if (seenTexts.has(text)) {
          return;
        }

        seenTexts.add(text);
        frame.push({ text });
      });
    });

    return frame;
  }

  private normalizeMetricFindQuery(rawQuery: VariableQueryInput): SplunkQuery | null {
    if (typeof rawQuery === 'string') {
      const queryText = rawQuery.trim();
      if (!queryText) {
        return null;
      }

      return {
        refId: 'metricFindQuery',
        queryText,
        searchType: 'standard',
      };
    }

    if (!rawQuery || typeof rawQuery !== 'object') {
      return null;
    }

    const queryRecord = rawQuery as Record<string, unknown>;
    const queryTextSource =
      typeof queryRecord.queryText === 'string'
        ? queryRecord.queryText
        : typeof queryRecord.query === 'string'
          ? queryRecord.query
          : '';
    const queryText = queryTextSource.trim();

    if (!queryText) {
      return null;
    }

    const searchType = resolveSearchType(queryRecord.searchType, queryRecord.mode);
    const mode = isLegacyMode(queryRecord.mode) ? queryRecord.mode : undefined;

    return {
      ...(queryRecord as Partial<SplunkQuery>),
      refId: typeof queryRecord.refId === 'string' && queryRecord.refId.length > 0 ? queryRecord.refId : 'metricFindQuery',
      queryText,
      searchType,
      mode,
      baseSearchRefId: typeof queryRecord.baseSearchRefId === 'string' ? queryRecord.baseSearchRefId : undefined,
      searchId: typeof queryRecord.searchId === 'string' ? queryRecord.searchId : undefined,
    };
  }

  private createMetricFindOptions(
    query: SplunkQuery,
    options?: DataQueryRequest<SplunkQuery>
  ): DataQueryRequest<SplunkQuery> {
    const now = Date.now();
    const fallbackRange: DataQueryRequest<SplunkQuery>['range'] = {
      from: dateTime(now - DEFAULT_VARIABLE_QUERY_RANGE_MS),
      to: dateTime(now),
      raw: {
        from: 'now-1h',
        to: 'now',
      },
    };
    const hasRange = Boolean(options?.range?.from && options?.range?.to);
    const safeOptions: Partial<DataQueryRequest<SplunkQuery>> = {
      ...(options ?? {}),
      scopedVars: options?.scopedVars ?? {},
      targets: options?.targets?.length ? options.targets : [query],
      range: hasRange ? options?.range : fallbackRange,
    };

    return safeOptions as DataQueryRequest<SplunkQuery>;
  }

  async query(options: DataQueryRequest<SplunkQuery>): Promise<DataQueryResponse> {
    // Clean up stale cache entries periodically
    this.cleanupStaleCache();
    
    const standardSearches = options.targets.filter(query => this.resolveQuerySearchType(query) === 'standard');
    const baseSearches = options.targets.filter(query => this.resolveQuerySearchType(query) === 'base');
    const chainSearches = options.targets.filter(query => this.resolveQuerySearchType(query) === 'chain');

    // Standard searches run first with bounded concurrency.
    const standardResults = await mapWithConcurrency(
      standardSearches,
      MAX_QUERY_EXECUTION_CONCURRENCY,
      async (query) => {
        const result = await this.doRequest(query, options);
        return this.createDataFrame(query, result);
      }
    );

    // Base searches run afterward with the same bounded concurrency.
    const completedBaseSearchResults = await mapWithConcurrency(
      baseSearches,
      MAX_QUERY_EXECUTION_CONCURRENCY,
      async (query) => this.resolveBaseSearch(query, options)
    );

    const baseResults = completedBaseSearchResults.map((completedResult, index) =>
      this.createDataFrame(baseSearches[index], {
        fields: completedResult.fields,
        results: completedResult.results,
        sid: completedResult.sid,
      })
    );

    // Chain searches are also independent once base searches are available.
    const chainResults = await mapWithConcurrency(
      chainSearches,
      MAX_QUERY_EXECUTION_CONCURRENCY,
      async (query) => {
        const chainResult = await this.executeChainSearch(query, options);
        return this.createDataFrame(query, chainResult);
      }
    );

    const allResults = [...standardResults, ...baseResults, ...chainResults];
    return { data: allResults };
  }

  private resolveQuerySearchType(query: SplunkQuery): EffectiveSearchType {
    return resolveSearchType(query.searchType, query.mode);
  }

  private async resolveBaseSearch(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<BaseSearchResult> {
    const cacheKey = generateCacheKey(query, options);

    const cachedResult = this.findBaseSearchResult(cacheKey);
    if (cachedResult) {
      return cachedResult;
    }

    const inflightPromise = this.baseSearchInflight.get(cacheKey);

    if (inflightPromise) {
      return inflightPromise;
    }

    const executeAndCacheBaseSearch = async (): Promise<BaseSearchResult> => {
      const result = await this.doRequest(query, options);
      const baseResult: BaseSearchResult = {
        sid: result.sid || '',
        searchId: query.searchId || query.refId,
        refId: query.refId,
        fields: result.fields,
        results: result.results,
        timestamp: Date.now(),
        cacheKey,
      };

      this.baseSearchCache.set(cacheKey, baseResult);
      this.baseSearchCache.set(query.refId, baseResult);
      if (query.searchId) {
        this.baseSearchCache.set(query.searchId, baseResult);
      }

      return baseResult;
    };

    let newPromise = executeAndCacheBaseSearch();
    newPromise = newPromise.finally(() => {
      this.baseSearchInflight.delete(cacheKey);
    });

    this.baseSearchInflight.set(cacheKey, newPromise);

    return newPromise;
  }

  private async waitForBaseSearchInflight(
    baseSearchRefId: string,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<BaseSearchResult | null> {
    for (let attempt = 0; attempt < CHAIN_BASE_SEARCH_RETRY_ATTEMPTS; attempt++) {
      const baseQueryTarget = options.targets.find(
        target =>
          this.resolveQuerySearchType(target) === 'base' &&
          (target.searchId === baseSearchRefId || target.refId === baseSearchRefId)
      );

      const inflightPromise = baseQueryTarget
        ? this.baseSearchInflight.get(generateCacheKey(baseQueryTarget, options))
        : undefined;

      if (inflightPromise) {
        try {
          const awaitedBaseSearch = await inflightPromise;
          if (awaitedBaseSearch && this.isCacheValid(awaitedBaseSearch)) {
            return awaitedBaseSearch;
          }
        } catch {
          // Ignore here and continue retrying; a later attempt may discover a fresh inflight base search.
        }
      }

      if (attempt < CHAIN_BASE_SEARCH_RETRY_ATTEMPTS - 1) {
        await delay(CHAIN_BASE_SEARCH_RETRY_DELAY_MS);
      }
    }

    return null;
  }

  private async resolveChainBaseSearch(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<BaseSearchResult> {
    const baseSearchRefId = query.baseSearchRefId?.trim();
    const queryRefId = query.refId || 'unknown';

    if (!baseSearchRefId) {
      throw new Error(`Chain search "${queryRefId}" requires baseSearchRefId and cannot run as a standard query.`);
    }

    const cachedBaseSearch = this.findBaseSearchResultByRefId(baseSearchRefId);
    if (cachedBaseSearch && this.isCacheValid(cachedBaseSearch)) {
      return cachedBaseSearch;
    }

    const awaitedBaseSearch = await this.waitForBaseSearchInflight(baseSearchRefId, options);
    if (awaitedBaseSearch) {
      return awaitedBaseSearch;
    }

    throw new Error(
      `Chain search "${queryRefId}" could not resolve base search "${baseSearchRefId}". ` +
      'No fallback to standalone search is applied by default to avoid semantic drift.'
    );
  }

  private async executeChainSearch(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<QueryRequestResults> {
    const baseSearch = await this.resolveChainBaseSearch(query, options);
    return this.doChainRequest(query, options, baseSearch);
  }
  
  private createDataFrame(query: SplunkQuery, response: QueryRequestResults) {
    // Prepare fields with proper typing
    const fields = response.fields.map((fieldName: any) => {
      const values: any[] = [];
      let fieldType = FieldType.string;
      
      // First pass: collect values
      response.results.forEach((result: any) => {
        if (fieldName === '_time') {
          const rawTime = result['_time'];
          if (rawTime === null || rawTime === undefined || (typeof rawTime === 'string' && rawTime.trim() === '')) {
            values.push(null);
          } else {
            const parsedTime = dateTime(rawTime).valueOf();
            values.push(Number.isFinite(parsedTime) ? parsedTime : null);
          }
        } else {
          values.push(result[fieldName]);
        }
      });
      
      // Determine field type based on content
      if (fieldName === '_time') {
        fieldType = FieldType.time;
      } else {
        // Check if all non-null values are purely numeric (not mixed text/numbers)
        const nonNullValues = values.filter(v => v !== null && v !== undefined && v !== '');
        if (nonNullValues.length > 0) {
          const allNumeric = nonNullValues.every(v => {
            // Convert to string to check if it's purely numeric
            const strValue = String(v).trim();
            // Check if the string contains only digits, decimal points, minus signs, and scientific notation
            const numericPattern = /^-?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
            const isNumericString = numericPattern.test(strValue);
            
            if (isNumericString) {
              const num = parseFloat(strValue);
              return !isNaN(num) && isFinite(num);
            }
            return false;
          });
          
          if (allNumeric) {
            fieldType = FieldType.number;
            // Convert string numbers to actual numbers, preserving precision
            for (let i = 0; i < values.length; i++) {
              if (values[i] !== null && values[i] !== undefined && values[i] !== '') {
                const originalValue = String(values[i]);
                const parsedValue = parseFloat(originalValue);
                // Preserve the original precision for decimal numbers
                values[i] = parsedValue;
              }
            }
          }
        }
      }
      
      return {
        name: fieldName,
        type: fieldType,
        values: values,
      };
    });

    const frame: PartialDataFrame = {
      refId: query.refId,
      fields: fields,
    };

    return frame;
  }
  
  private findBaseSearchResult(cacheKey: string): BaseSearchResult | null {
    const cachedResult = this.baseSearchCache.get(cacheKey);
    if (cachedResult && this.isCacheValid(cachedResult)) {
      return cachedResult;
    } else if (cachedResult && !this.isCacheValid(cachedResult)) {
      // Remove stale cache entry
      this.baseSearchCache.delete(cacheKey);
    }
    return null;
  }
  
  private findBaseSearchResultByRefId(baseSearchRefId: string): BaseSearchResult | null {
    const cachedResult = this.baseSearchCache.get(baseSearchRefId);
    if (cachedResult && this.isCacheValid(cachedResult)) {
      return cachedResult;
    } else if (cachedResult && !this.isCacheValid(cachedResult)) {
      // Remove stale cache entry
      this.baseSearchCache.delete(baseSearchRefId);
    }
    return null;
  }
  
  private isCacheValid(cached: BaseSearchResult): boolean {
    return (Date.now() - cached.timestamp) < CACHE_TTL;
  }
  
  private cleanupStaleCache(): void {
    const now = Date.now();
    const keysToDelete: string[] = [];
    
    for (const [key, result] of this.baseSearchCache.entries()) {
      if ((now - result.timestamp) >= CACHE_TTL) {
        keysToDelete.push(key);
      }
    }
    
    keysToDelete.forEach(key => {
      this.baseSearchCache.delete(key);
    });
  }

  private async waitForSearchCompletion(
    sid: string,
    pollIntervalMs: number = SEARCH_POLL_INTERVAL_MS,
    timeoutMs: number = SEARCH_POLL_TIMEOUT_MS
  ): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      if (await this.doSearchStatusRequest(sid)) {
        return true;
      }

      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        break;
      }

      await delay(Math.min(pollIntervalMs, remainingMs));
    }

    return this.doSearchStatusRequest(sid);
  }

  async testDatasource() {
    const data = new URLSearchParams({
      search: `search index=_internal * | stats count`,
      output_mode: 'json',
      exec_mode: 'oneshot',
    }).toString();

    try {
      await lastValueFrom(
        (getBackendSrv().fetch<any>({
          method: 'POST',
          url: this.url + '/services/search/jobs',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          data: data,
        }) as any)
      );
      return {
        status: 'success',
        message: 'Data source is working',
        title: 'Success',
      };
    } catch (err: any) {
      return {
        status: 'error',
        message: err.statusText,
        title: 'Error',
      };
    }
  }

  async doSearchStatusRequest(sid: string) {
    const response: any = await lastValueFrom(
      (getBackendSrv().fetch<any>({
        method: 'GET',
        url: this.url + '/services/search/jobs/' + sid,
        params: {
          output_mode: 'json',
        },
      }) as any)
    );
    let status = (response.data as any).entry[0].content.dispatchState;
    return status === 'DONE' || status === 'PAUSED' || status === 'FAILED';
  }

  async doSearchRequest(query: SplunkQuery, options: DataQueryRequest<SplunkQuery>): Promise<{sid: string} | null> {
    if ((query.queryText || '').trim().length < 4) {
      return null;
    }
    const { range } = options;
    const from = Math.floor(range!.from.valueOf() / 1000);
    const to = Math.floor(range!.to.valueOf() / 1000);
    const prefix = (query.queryText || ' ')[0].trim() === '|' ? '' : 'search';
    const queryWithVars = getTemplateSrv().replace(`${prefix} ${query.queryText}`.trim(), options.scopedVars);
    const data = new URLSearchParams({
      search: queryWithVars,
      output_mode: 'json',
      earliest_time: from.toString(),
      latest_time: to.toString(),
    }).toString();
    const response: any = await lastValueFrom(
      (getBackendSrv().fetch<any>({
        method: 'POST',
        url: this.url + '/services/search/jobs',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        data: data,
      }) as any)
    );
    const sid: string = (response.data as any).sid;
    return { sid };
  }

  async doGetAllResultsRequest(sid: string) {
    const count = 50000;
    let offset = 0;
    let isFirst = true;
    let isFinished = false;
    let fields: any[] = [];
    let results: any[] = [];

    while (!isFinished) {
      const response: any = await lastValueFrom(
        (getBackendSrv().fetch<any>({
          method: 'GET',
          url: this.url + '/services/search/jobs/' + sid + '/results',
          params: {
            output_mode: 'json',
            offset: offset,
            count: count,
          },
        }) as any)
      );

      const responseData = response.data as any;
      const pageResults: any[] = responseData.results || [];

      if (pageResults.length === 0) {
        isFinished = true;
      } else {
        if (isFirst) {
          isFirst = false;
          fields = (responseData.fields || []).map((field: any) => field['name']);
        }
        results = results.concat(pageResults);
        offset = offset + pageResults.length;
      }
    }

    const index = fields.indexOf('_raw', 0);
    if (index > -1) {
      fields.splice(index, 1);
      fields = fields.reverse();
      fields.push('_raw');
      fields = fields.reverse();
    }

    return { fields: fields, results: results };
  }

  async doRequest(query: SplunkQuery, options: DataQueryRequest<SplunkQuery>): Promise<QueryRequestResults & { sid?: string }> {
    const searchResult = await this.doSearchRequest(query, options);
    const sid: string = searchResult?.sid || '';
    if (sid.length > 0) {
      const isComplete = await this.waitForSearchCompletion(sid);
      if (!isComplete) {
        throw new SplunkSearchTimeoutError(sid, 'standard', SEARCH_POLL_TIMEOUT_MS);
      }

      const result = await this.doGetAllResultsRequest(sid);
      return { ...result, sid };
    }
    return defaultQueryRequestResults;
  }

  async doChainRequest(query: SplunkQuery, options: DataQueryRequest<SplunkQuery>, baseSearch: BaseSearchResult): Promise<QueryRequestResults> {
    if ((query.queryText || '').trim().length < 1) {
      return defaultQueryRequestResults;
    }
    
    const { range } = options;
    const from = Math.floor(range!.from.valueOf() / 1000);
    const to = Math.floor(range!.to.valueOf() / 1000);

    let chainQuery = query.queryText.trim();
    if (baseSearch.sid) {
      const vars = getTemplateSrv().replace(chainQuery, options.scopedVars).trim();
      chainQuery = vars.startsWith('|')
        ? `| loadjob ${baseSearch.sid} ${vars}`
        : `| loadjob ${baseSearch.sid} | ${vars}`;
    } else {
      throw new Error(
        `Chain search "${query.refId || 'unknown'}" could not execute because base search ` +
          `"${query.baseSearchRefId || baseSearch.refId}" has no SID.`
      );
    }

    const data = new URLSearchParams({
      search: chainQuery,
      output_mode: 'json',
      earliest_time: from.toString(),
      latest_time: to.toString(),
    }).toString();

    try {
      const response: any = await lastValueFrom(
        (getBackendSrv().fetch<any>({
          method: 'POST',
          url: this.url + '/services/search/jobs',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          data: data,
        }) as any)
      );
      const sid: string = (response.data as any)?.sid ?? '';
      if (sid.length > 0) {
        const isComplete = await this.waitForSearchCompletion(sid);
        if (!isComplete) {
          throw new SplunkSearchTimeoutError(sid, 'chain', SEARCH_POLL_TIMEOUT_MS);
        }

        const result = await this.doGetAllResultsRequest(sid);
        return result;
      }

      throw new Error(`Chain search "${query.refId || 'unknown'}" returned an empty SID.`);
    } catch (error) {
      const baseRef = query.baseSearchRefId || baseSearch.refId;
      throw new Error(
        `Chain search "${query.refId || 'unknown'}" failed against base search "${baseRef}": ${getErrorMessage(error)}`
      );
    }
  }
}
