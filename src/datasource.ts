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

import {
  SplunkQuery,
  SplunkDataSourceOptions,
  defaultQueryRequestResults,
  QueryRequestResults,
  BaseSearchResult,
  BaseSearchMetadata,
} from './types';
import { SplunkVariableQuery, VariableQueryEditor } from './VariableQueryEditor';

const DEFAULT_VARIABLE_QUERY_RANGE_MS = 60 * 60 * 1000;
const MAX_QUERY_EXECUTION_CONCURRENCY = 4;
const CHAIN_BASE_SEARCH_RETRY_ATTEMPTS = 3;
const CHAIN_BASE_SEARCH_RETRY_DELAY_MS = 100;
const SEARCH_POLL_INTERVAL_MS = 100;
const DEFAULT_STANDARD_SEARCH_TIMEOUT_MS = 30 * 1000;
const DEFAULT_MAX_RESULT_ROWS = 100_000;
const DEFAULT_MAX_RESULT_PAGES = 100;
const DEFAULT_MAX_RESPONSE_BYTES = 50 * 1024 * 1024;
const SEARCH_TIMEOUT_ERROR_CODE = 'SPLUNK_SEARCH_TIMEOUT';
const TERMINAL_SEARCH_STATES = new Set(['DONE', 'PAUSED', 'FAILED']);

interface SearchStatus {
  state: string;
  messages: string[];
}

interface ResolvedBaseSearch {
  result: BaseSearchResult;
  cache: BaseSearchMetadata['cache'];
}

class SplunkSearchTimeoutError extends Error {
  readonly code = SEARCH_TIMEOUT_ERROR_CODE;

  constructor(
    readonly sid: string,
    readonly searchType: 'standard' | 'base' | 'chain',
    readonly timeoutMs: number
  ) {
    super(`Splunk ${searchType} search timed out after ${timeoutMs}ms (sid=${sid}).`);
    this.name = 'SplunkSearchTimeoutError';
  }
}

class SplunkSearchFailedError extends Error {
  constructor(
    readonly sid: string,
    messages: string[]
  ) {
    super(`Splunk search failed (sid=${sid})${messages.length ? `: ${messages.join('; ')}` : '.'}`);
    this.name = 'SplunkSearchFailedError';
  }
}

type VariableQueryInput = SplunkQuery | SplunkVariableQuery | string | Record<string, unknown>;
type EffectiveSearchType = NonNullable<SplunkQuery['searchType']>;

const isSearchType = (value: unknown): value is EffectiveSearchType =>
  value === 'standard' || value === 'base' || value === 'chain';

const isLegacyMode = (value: unknown): value is NonNullable<SplunkQuery['mode']> =>
  value === 'base' || value === 'chain';

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

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const clampSetting = (value: number | undefined, fallback: number, minimum: number, maximum: number) =>
  Number.isFinite(value) ? Math.min(maximum, Math.max(minimum, value!)) : fallback;

const getDashboardNamespace = (options: DataQueryRequest<SplunkQuery>) =>
  options.dashboardUID ?? options.dashboardTitle ?? `${options.app}:${options.panelId ?? 'unknown-panel'}`;

function generateCacheKey(query: SplunkQuery, options: DataQueryRequest<SplunkQuery>): string {
  const expandedQuery = getTemplateSrv()
    .replace(query.queryText || '', options.scopedVars)
    .trim();
  const timeRange =
    query.useDashboardTimeRange === false
      ? [query.earliest || '-30d@d', query.latest || 'now']
      : [
          Math.floor(options.range.from.valueOf() / 1000).toString(),
          Math.floor(options.range.to.valueOf() / 1000).toString(),
        ];

  return [getDashboardNamespace(options), query.searchId || query.refId || '', expandedQuery, ...timeRange].join('|');
}

export class DataSource extends DataSourceApi<SplunkQuery, SplunkDataSourceOptions> {
  url?: string;
  variables = new SplunkCustomVariableSupport(this);
  private readonly baseSearchCache: Map<string, BaseSearchResult> = new Map();
  private readonly baseSearchInflight: Map<string, Promise<BaseSearchResult>> = new Map();
  private readonly standardSearchTimeoutMs: number;
  private readonly baseSearchTimeoutMs: number;
  private readonly chainSearchTimeoutMs: number;
  private readonly cacheTtlMs: number;
  private readonly maxResultRows: number;
  private readonly maxResultPages: number;
  private readonly maxResponseBytes: number;
  private readonly debugLogging: boolean;

  constructor(instanceSettings: DataSourceInstanceSettings<SplunkDataSourceOptions>) {
    super(instanceSettings);

    this.url = instanceSettings.url;
    const settings = instanceSettings.jsonData;
    this.standardSearchTimeoutMs = clampSetting(settings.standardSearchTimeoutSeconds, 30, 1, 3600) * 1000;
    this.baseSearchTimeoutMs = clampSetting(settings.baseSearchTimeoutSeconds, 120, 1, 3600) * 1000;
    this.chainSearchTimeoutMs = clampSetting(settings.chainSearchTimeoutSeconds, 30, 1, 3600) * 1000;
    this.cacheTtlMs = clampSetting(settings.baseSearchCacheTtlMinutes, 5, 0.1, 60) * 60 * 1000;
    this.maxResultRows = clampSetting(settings.maxResultRows, DEFAULT_MAX_RESULT_ROWS, 1, 1_000_000);
    this.maxResultPages = clampSetting(settings.maxResultPages, DEFAULT_MAX_RESULT_PAGES, 1, 1000);
    this.maxResponseBytes = clampSetting(
      settings.maxResponseBytes,
      DEFAULT_MAX_RESPONSE_BYTES,
      1024,
      500 * 1024 * 1024
    );
    this.debugLogging = settings.debugLogging === true;
  }

  private debug(event: string, details: Record<string, unknown>) {
    if (this.debugLogging) {
      // eslint-disable-next-line no-console
      console.debug(`[Splunk datasource] ${event}`, details);
    }
  }

  async metricFindQuery(
    query: VariableQueryInput,
    options?: DataQueryRequest<SplunkQuery>
  ): Promise<MetricFindValue[]> {
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
      refId:
        typeof queryRecord.refId === 'string' && queryRecord.refId.length > 0 ? queryRecord.refId : 'metricFindQuery',
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
    this.validateTargets(options.targets);
    this.cleanupStaleCache();

    const indexedTargets = options.targets.map((query, index) => ({ query, index }));
    const standardSearches = indexedTargets.filter(({ query }) => this.resolveQuerySearchType(query) === 'standard');
    const baseSearches = indexedTargets.filter(({ query }) => this.resolveQuerySearchType(query) === 'base');
    const chainSearches = indexedTargets.filter(({ query }) => this.resolveQuerySearchType(query) === 'chain');
    const resultFrames = new Array<PartialDataFrame | undefined>(options.targets.length);

    // Standard searches run first with bounded concurrency.
    await mapWithConcurrency(standardSearches, MAX_QUERY_EXECUTION_CONCURRENCY, async ({ query, index }) => {
      const result = await this.doRequest(query, options);
      resultFrames[index] = this.createDataFrame(query, result);
    });

    // Base searches run afterward with the same bounded concurrency.
    await mapWithConcurrency(baseSearches, MAX_QUERY_EXECUTION_CONCURRENCY, async ({ query, index }) => {
      const resolved = await this.resolveBaseSearch(query, options);
      const result = query.returnBaseResults
        ? await this.doGetAllResultsRequest(resolved.result.sid, this.getRequestId(options, query, 'base-results'))
        : defaultQueryRequestResults;
      resultFrames[index] = this.createDataFrame(query, {
        ...result,
        sid: resolved.result.sid,
        baseSearch: this.createBaseSearchMetadata(resolved),
      });
    });

    // Chain searches are also independent once base searches are available.
    await mapWithConcurrency(chainSearches, MAX_QUERY_EXECUTION_CONCURRENCY, async ({ query, index }) => {
      const chainResult = await this.executeChainSearch(query, options);
      resultFrames[index] = this.createDataFrame(query, chainResult);
    });

    return { data: resultFrames.filter((frame): frame is PartialDataFrame => Boolean(frame)) };
  }

  private resolveQuerySearchType(query: SplunkQuery): EffectiveSearchType {
    return resolveSearchType(query.searchType, query.mode);
  }

  private validateTargets(targets: SplunkQuery[]) {
    const baseTargets = targets.filter((target) => this.resolveQuerySearchType(target) === 'base');
    const baseIds = new Set<string>();

    for (const target of baseTargets) {
      const refId = target.refId.trim();
      const searchId = target.searchId?.trim();
      if (baseIds.has(refId) || (searchId && searchId !== refId && baseIds.has(searchId))) {
        throw new Error(`Base search identifier "${searchId || refId}" must be unique.`);
      }
      baseIds.add(refId);
      if (searchId) {
        baseIds.add(searchId);
      }

      if (target.baseSearchRefId) {
        throw new Error(`Base search "${searchId || refId}" cannot reference another base search.`);
      }
    }

    for (const target of targets) {
      const searchType = this.resolveQuerySearchType(target);
      if (searchType === 'chain') {
        const baseRef = target.baseSearchRefId?.trim();
        if (!baseRef) {
          throw new Error(`Chain search "${target.refId}" requires a base search reference.`);
        }
      } else if (searchType === 'standard' && (target.baseSearchRefId || target.searchId)) {
        throw new Error(`Standard search "${target.refId}" cannot carry base or chain settings.`);
      }
    }
  }

  private getRequestId(options: DataQueryRequest<SplunkQuery>, query: SplunkQuery, operation: string): string {
    return `splunk:${getDashboardNamespace(options)}:${options.panelId ?? 'unknown-panel'}:${query.refId}:${operation}`;
  }

  private async resolveBaseSearch(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<ResolvedBaseSearch> {
    const cacheKey = generateCacheKey(query, options);

    const cachedResult = this.findBaseSearchResult(cacheKey);
    if (cachedResult) {
      this.debug('base cache hit', { cacheKey, sid: cachedResult.sid });
      return { result: cachedResult, cache: 'hit' };
    }
    this.debug('base cache miss', { cacheKey });

    const inflightPromise = this.baseSearchInflight.get(cacheKey);

    if (inflightPromise) {
      return { result: await inflightPromise, cache: 'inflight' };
    }

    const executeAndCacheBaseSearch = async (): Promise<BaseSearchResult> => {
      const baseResult = await this.executeBaseSearch(query, options, cacheKey);

      this.baseSearchCache.set(cacheKey, baseResult);
      this.baseSearchCache.set(this.getBaseIdentifierKey(options, query.refId), baseResult);
      if (query.searchId) {
        this.baseSearchCache.set(this.getBaseIdentifierKey(options, query.searchId), baseResult);
      }

      return baseResult;
    };

    const newPromise = executeAndCacheBaseSearch();
    const identifierKeys = [
      this.getBaseIdentifierKey(options, query.refId),
      ...(query.searchId ? [this.getBaseIdentifierKey(options, query.searchId)] : []),
    ];
    this.baseSearchInflight.set(cacheKey, newPromise);
    identifierKeys.forEach((key) => this.baseSearchInflight.set(key, newPromise));

    try {
      return { result: await newPromise, cache: 'miss' };
    } finally {
      [cacheKey, ...identifierKeys].forEach((key) => {
        if (this.baseSearchInflight.get(key) === newPromise) {
          this.baseSearchInflight.delete(key);
        }
      });
    }
  }

  private async executeBaseSearch(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>,
    cacheKey: string
  ): Promise<BaseSearchResult> {
    const startedAt = Date.now();
    const requestId = this.getRequestId(options, query, 'base');
    const searchResult = await this.doSearchRequest(query, options, requestId);
    const sid = searchResult?.sid || '';

    if (!sid) {
      throw new Error(`Base search "${query.searchId || query.refId}" returned no SID.`);
    }

    try {
      const status = await this.waitForSearchCompletion(
        sid,
        SEARCH_POLL_INTERVAL_MS,
        this.baseSearchTimeoutMs,
        requestId
      );
      this.assertSearchCompleted(sid, 'base', status, this.baseSearchTimeoutMs);
    } catch (error) {
      await this.cancelSearchJob(sid);
      throw error;
    }

    const result = {
      sid,
      searchId: query.searchId || query.refId,
      refId: query.refId,
      timestamp: Date.now(),
      cacheKey,
      executionMs: Date.now() - startedAt,
    };
    this.debug('base execution complete', {
      sid,
      searchId: result.searchId,
      executionMs: result.executionMs,
    });
    return result;
  }

  private async waitForBaseSearchInflight(
    baseSearchRefId: string,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<BaseSearchResult | null> {
    for (let attempt = 0; attempt < CHAIN_BASE_SEARCH_RETRY_ATTEMPTS; attempt++) {
      const inflightPromise = this.baseSearchInflight.get(this.getBaseIdentifierKey(options, baseSearchRefId));

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

    const cachedBaseSearch = this.findBaseSearchResultByRefId(baseSearchRefId, options);
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
    let baseSearch = await this.resolveChainBaseSearch(query, options);
    let cache: BaseSearchMetadata['cache'] = 'hit';

    if (!(await this.searchJobExists(baseSearch.sid, this.getRequestId(options, query, 'base-status')))) {
      this.debug('cached SID missing', { sid: baseSearch.sid, searchId: baseSearch.searchId });
      this.deleteBaseSearch(baseSearch);
      const baseQuery = options.targets.find(
        (target) =>
          this.resolveQuerySearchType(target) === 'base' &&
          (target.searchId === query.baseSearchRefId || target.refId === query.baseSearchRefId)
      );
      if (!baseQuery) {
        throw new Error(`Base search "${query.baseSearchRefId}" is unavailable and cannot be rerun.`);
      }
      const rerun = await this.resolveBaseSearch(baseQuery, options);
      baseSearch = rerun.result;
      cache = 'miss';
      this.debug('base SID retry', { sid: baseSearch.sid, searchId: baseSearch.searchId });
    }

    const result = await this.doChainRequest(query, options, baseSearch);
    result.baseSearch = this.createBaseSearchMetadata({ result: baseSearch, cache });
    return result;
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
        const nonNullValues = values.filter((v) => v !== null && v !== undefined && v !== '');
        if (nonNullValues.length > 0) {
          const allNumeric = nonNullValues.every((v) => {
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
      meta: {
        custom: response.baseSearch ? { baseSearch: response.baseSearch } : undefined,
        notices: response.warning ? [{ severity: 'warning', text: response.warning, inspect: 'data' }] : undefined,
      },
    };

    return frame;
  }

  private findBaseSearchResult(cacheKey: string): BaseSearchResult | null {
    const cachedResult = this.baseSearchCache.get(cacheKey);
    if (cachedResult && this.isCacheValid(cachedResult)) {
      return cachedResult;
    } else if (cachedResult && !this.isCacheValid(cachedResult)) {
      this.deleteBaseSearch(cachedResult);
    }
    return null;
  }

  private getBaseIdentifierKey(options: DataQueryRequest<SplunkQuery>, identifier: string): string {
    return `${getDashboardNamespace(options)}:${identifier}`;
  }

  private findBaseSearchResultByRefId(
    baseSearchRefId: string,
    options: DataQueryRequest<SplunkQuery>
  ): BaseSearchResult | null {
    const identifierKey = this.getBaseIdentifierKey(options, baseSearchRefId);
    const cachedResult = this.baseSearchCache.get(identifierKey);
    if (cachedResult && this.isCacheValid(cachedResult)) {
      return cachedResult;
    } else if (cachedResult && !this.isCacheValid(cachedResult)) {
      this.deleteBaseSearch(cachedResult);
    }
    return null;
  }

  private createBaseSearchMetadata(resolved: ResolvedBaseSearch): BaseSearchMetadata {
    return {
      searchId: resolved.result.searchId,
      sid: resolved.result.sid,
      cache: resolved.cache,
      ageMs: Date.now() - resolved.result.timestamp,
      executionMs: resolved.result.executionMs,
    };
  }

  private deleteBaseSearch(result: BaseSearchResult) {
    for (const [key, value] of this.baseSearchCache) {
      if (value.cacheKey === result.cacheKey) {
        this.baseSearchCache.delete(key);
      }
    }
  }

  private isCacheValid(cached: BaseSearchResult): boolean {
    return Date.now() - cached.timestamp < this.cacheTtlMs;
  }

  private cleanupStaleCache(): void {
    const now = Date.now();
    const keysToDelete: string[] = [];

    for (const [key, result] of this.baseSearchCache.entries()) {
      if (now - result.timestamp >= this.cacheTtlMs) {
        keysToDelete.push(key);
        this.debug('base cache expired', { sid: result.sid, searchId: result.searchId });
      }
    }

    keysToDelete.forEach((key) => {
      this.baseSearchCache.delete(key);
    });
  }

  private async waitForSearchCompletion(
    sid: string,
    pollIntervalMs: number = SEARCH_POLL_INTERVAL_MS,
    timeoutMs: number = DEFAULT_STANDARD_SEARCH_TIMEOUT_MS,
    requestId?: string
  ): Promise<SearchStatus> {
    const deadline = Date.now() + timeoutMs;
    let status: SearchStatus = { state: 'RUNNING', messages: [] };

    while (Date.now() < deadline) {
      status = await this.doSearchStatusRequest(sid, requestId);
      if (TERMINAL_SEARCH_STATES.has(status.state)) {
        return status;
      }

      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        break;
      }

      await delay(Math.min(pollIntervalMs, remainingMs));
    }

    return this.doSearchStatusRequest(sid, requestId);
  }

  private assertSearchCompleted(
    sid: string,
    searchType: 'standard' | 'base' | 'chain',
    status: SearchStatus,
    timeoutMs: number
  ) {
    if (status.state === 'FAILED') {
      this.debug('Splunk dispatch failure', { sid, messages: status.messages });
      throw new SplunkSearchFailedError(sid, status.messages);
    }
    if (status.state !== 'DONE' && status.state !== 'PAUSED') {
      throw new SplunkSearchTimeoutError(sid, searchType, timeoutMs);
    }
  }

  private async searchJobExists(sid: string, requestId?: string): Promise<boolean> {
    try {
      const status = await this.doSearchStatusRequest(sid, requestId);
      return status.state !== 'FAILED';
    } catch (error: any) {
      if (error?.status === 404 || error?.statusText === 'Not Found') {
        return false;
      }
      throw error;
    }
  }

  private async cancelSearchJob(sid: string) {
    try {
      await lastValueFrom(
        getBackendSrv().fetch({
          method: 'POST',
          url: `${this.url}/services/search/jobs/${encodeURIComponent(sid)}/control`,
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          data: new URLSearchParams({ action: 'cancel' }).toString(),
          showErrorAlert: false,
          requestId: `splunk-cancel:${sid}`,
        })
      );
      this.debug('cancelled Splunk job', { sid });
    } catch {
      this.debug('failed to cancel Splunk job', { sid });
    }
  }

  async testDatasource() {
    const data = new URLSearchParams({
      search: `search index=_internal * | stats count`,
      output_mode: 'json',
      exec_mode: 'oneshot',
    }).toString();

    try {
      await lastValueFrom(
        getBackendSrv().fetch<any>({
          method: 'POST',
          url: this.url + '/services/search/jobs',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          data: data,
        }) as any
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

  async doSearchStatusRequest(sid: string, requestId?: string): Promise<SearchStatus> {
    const response: any = await lastValueFrom(
      getBackendSrv().fetch<any>({
        method: 'GET',
        url: this.url + '/services/search/jobs/' + encodeURIComponent(sid),
        params: {
          output_mode: 'json',
        },
        requestId,
        validatePath: true,
      }) as any
    );
    const entry = response.data?.entry?.[0];
    const messages = [response.data?.messages, entry?.content?.messages]
      .flatMap((value) => (Array.isArray(value) ? value : value ? [value] : []))
      .map((message: any) => message?.text || message?.message || String(message))
      .filter(Boolean);
    return {
      state: entry?.content?.dispatchState || 'UNKNOWN',
      messages,
    };
  }

  async doSearchRequest(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>,
    requestId?: string
  ): Promise<{ sid: string } | null> {
    if ((query.queryText || '').trim().length < 4) {
      return null;
    }
    const useDashboardTimeRange = query.useDashboardTimeRange !== false;
    const from = useDashboardTimeRange
      ? Math.floor(options.range.from.valueOf() / 1000).toString()
      : query.earliest || '-30d@d';
    const to = useDashboardTimeRange ? Math.floor(options.range.to.valueOf() / 1000).toString() : query.latest || 'now';
    const prefix = (query.queryText || ' ')[0].trim() === '|' ? '' : 'search';
    const queryWithVars = getTemplateSrv().replace(`${prefix} ${query.queryText}`.trim(), options.scopedVars);
    const data = new URLSearchParams({
      search: queryWithVars,
      output_mode: 'json',
      earliest_time: from,
      latest_time: to,
    }).toString();
    const response: any = await lastValueFrom(
      getBackendSrv().fetch<any>({
        method: 'POST',
        url: this.url + '/services/search/jobs',
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        data: data,
        requestId,
      }) as any
    );
    const sid: string = (response.data as any).sid;
    return { sid };
  }

  async doGetAllResultsRequest(sid: string, requestId?: string): Promise<QueryRequestResults> {
    const pageSize = 50_000;
    let offset = 0;
    let isFirst = true;
    let isFinished = false;
    let fields: any[] = [];
    let results: any[] = [];
    let pages = 0;
    let responseBytes = 0;
    let warning: string | undefined;

    while (!isFinished) {
      if (pages >= this.maxResultPages) {
        warning = `Splunk results truncated at ${this.maxResultPages} pages.`;
        break;
      }
      const remainingRows = this.maxResultRows - results.length;
      if (remainingRows <= 0) {
        warning = `Splunk results truncated at ${this.maxResultRows} rows.`;
        break;
      }

      const response: any = await lastValueFrom(
        getBackendSrv().fetch<any>({
          method: 'GET',
          url: this.url + '/services/search/jobs/' + encodeURIComponent(sid) + '/results',
          params: {
            output_mode: 'json',
            offset: offset,
            count: Math.min(pageSize, remainingRows),
          },
          requestId,
          validatePath: true,
        }) as any
      );

      const responseData = response.data as any;
      const pageResults: any[] = responseData.results || [];
      pages += 1;
      responseBytes += new Blob([JSON.stringify(responseData)]).size;

      if (responseBytes > this.maxResponseBytes) {
        warning = `Splunk results truncated after responses exceeded ${this.maxResponseBytes} bytes.`;
        break;
      }

      if (pageResults.length === 0) {
        isFinished = true;
      } else {
        if (isFirst) {
          isFirst = false;
          fields = (responseData.fields || []).map((field: any) => field['name']);
        }
        const retainedResults = pageResults.slice(0, remainingRows);
        results = results.concat(retainedResults);
        offset += pageResults.length;
        if (results.length >= this.maxResultRows) {
          warning = `Splunk results truncated at ${this.maxResultRows} rows.`;
          break;
        }
      }
    }

    const index = fields.indexOf('_raw', 0);
    if (index > -1) {
      fields.splice(index, 1);
      fields = fields.reverse();
      fields.push('_raw');
      fields = fields.reverse();
    }

    this.debug('result retrieval complete', { sid, rows: results.length, pages, responseBytes, warning });
    return { fields, results, warning };
  }

  async doRequest(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<QueryRequestResults & { sid?: string }> {
    const requestId = this.getRequestId(options, query, 'standard');
    const searchResult = await this.doSearchRequest(query, options, requestId);
    const sid: string = searchResult?.sid || '';
    if (sid.length > 0) {
      try {
        const status = await this.waitForSearchCompletion(
          sid,
          SEARCH_POLL_INTERVAL_MS,
          this.standardSearchTimeoutMs,
          requestId
        );
        this.assertSearchCompleted(sid, 'standard', status, this.standardSearchTimeoutMs);

        const result = await this.doGetAllResultsRequest(sid, requestId);
        return { ...result, sid };
      } catch (error) {
        await this.cancelSearchJob(sid);
        throw error;
      }
    }
    return defaultQueryRequestResults;
  }

  async doChainRequest(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>,
    baseSearch: BaseSearchResult
  ): Promise<QueryRequestResults> {
    if ((query.queryText || '').trim().length < 1) {
      return defaultQueryRequestResults;
    }

    const from = Math.floor(options.range.from.valueOf() / 1000);
    const to = Math.floor(options.range.to.valueOf() / 1000);
    const requestId = this.getRequestId(options, query, 'chain');
    const startedAt = Date.now();

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
        getBackendSrv().fetch<any>({
          method: 'POST',
          url: this.url + '/services/search/jobs',
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
          },
          data: data,
          requestId,
        }) as any
      );
      const sid: string = (response.data as any)?.sid ?? '';
      if (sid.length > 0) {
        try {
          const status = await this.waitForSearchCompletion(
            sid,
            SEARCH_POLL_INTERVAL_MS,
            this.chainSearchTimeoutMs,
            requestId
          );
          this.assertSearchCompleted(sid, 'chain', status, this.chainSearchTimeoutMs);

          const result = await this.doGetAllResultsRequest(sid, requestId);
          this.debug('chain execution complete', {
            sid,
            baseSid: baseSearch.sid,
            executionMs: Date.now() - startedAt,
            rows: result.results.length,
          });
          return result;
        } catch (error) {
          await this.cancelSearchJob(sid);
          throw error;
        }
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
