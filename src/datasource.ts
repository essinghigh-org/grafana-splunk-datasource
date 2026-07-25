
import { from } from 'rxjs';

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
import { SplunkClient, SearchStatus, SplunkSearchFailedError, SplunkSearchTimeoutError } from './api/SplunkClient';
import {
  delay,
  clampSetting,
  getDashboardNamespace,
  generateCacheKey,
  mapWithConcurrency,
  resolveSearchType,
  getErrorMessage,
} from './utils/searchHelpers';

const DEFAULT_VARIABLE_QUERY_RANGE_MS = 60 * 60 * 1000;
const MAX_QUERY_EXECUTION_CONCURRENCY = 4;
const CHAIN_BASE_SEARCH_RETRY_ATTEMPTS = 3;
const CHAIN_BASE_SEARCH_RETRY_DELAY_MS = 100;
const SEARCH_POLL_INTERVAL_MS = 100;
const DEFAULT_STANDARD_SEARCH_TIMEOUT_MS = 30 * 1000;
const DEFAULT_MAX_RESULT_ROWS = 100_000;
const DEFAULT_MAX_RESULT_PAGES = 100;
const DEFAULT_MAX_RESPONSE_BYTES = 50 * 1024 * 1024;
const TERMINAL_SEARCH_STATES = new Set(['DONE', 'PAUSED', 'FAILED']);

interface ResolvedBaseSearch {
  result: BaseSearchResult;
  cache: BaseSearchMetadata['cache'];
}

type VariableQueryInput = SplunkQuery | SplunkVariableQuery | string | Record<string, unknown>;
type EffectiveSearchType = NonNullable<SplunkQuery['searchType']>;

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

export class DataSource extends DataSourceApi<SplunkQuery, SplunkDataSourceOptions> {
  url?: string;
  variables = new SplunkCustomVariableSupport(this);
  client: SplunkClient;
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

    this.client = new SplunkClient(
      this.url || '',
      this.maxResultPages,
      this.maxResultRows,
      this.maxResponseBytes,
      this.debug.bind(this)
    );
  }

  private debug(event: string, details: Record<string, unknown>) {
    if (this.debugLogging) {
      // eslint-disable-next-line no-console
      console.debug(`[Splunk datasource] ${event}`, details);
    }
  }

  async testDatasource() {
    return this.client.testDatasource();
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
    const mode = searchType === 'base' || searchType === 'chain' ? searchType : undefined;

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
        ? await this.client.doGetAllResultsRequest(resolved.result.sid, this.getRequestId(options, query, 'base-results'))
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
    const searchResult = await this.client.doSearchRequest(query, options, requestId);
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
      await this.client.cancelSearchJob(sid);
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

    const status = await this.client.doSearchStatusRequest(baseSearch.sid, this.getRequestId(options, query, 'base-status'))
      .catch(() => ({ state: 'FAILED', messages: [] }));

    if (status.state === 'FAILED') {
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
      status = await this.client.doSearchStatusRequest(sid, requestId);
      if (TERMINAL_SEARCH_STATES.has(status.state)) {
        return status;
      }

      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0) {
        break;
      }

      await delay(Math.min(pollIntervalMs, remainingMs));
    }

    return this.client.doSearchStatusRequest(sid, requestId);
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

  async doRequest(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>
  ): Promise<QueryRequestResults & { sid?: string }> {
    const requestId = this.getRequestId(options, query, 'standard');
    const searchResult = await this.client.doSearchRequest(query, options, requestId);
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

        const result = await this.client.doGetAllResultsRequest(sid, requestId);
        return { ...result, sid };
      } catch (error) {
        await this.client.cancelSearchJob(sid);
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

    const requestId = this.getRequestId(options, query, 'chain');
    const startedAt = Date.now();

    if (!baseSearch.sid) {
      throw new Error(
        `Chain search "${query.refId || 'unknown'}" could not execute because base search ` +
          `"${query.baseSearchRefId || baseSearch.refId}" has no SID.`
      );
    }

    try {
      const searchResult = await this.client.doChainSearchRequest(query, options, baseSearch, requestId);
      const sid: string = searchResult?.sid || '';

      if (sid.length > 0) {
        try {
          const status = await this.waitForSearchCompletion(
            sid,
            SEARCH_POLL_INTERVAL_MS,
            this.chainSearchTimeoutMs,
            requestId
          );
          this.assertSearchCompleted(sid, 'chain', status, this.chainSearchTimeoutMs);

          const result = await this.client.doGetAllResultsRequest(sid, requestId);
          this.debug('chain execution complete', {
            sid,
            baseSid: baseSearch.sid,
            executionMs: Date.now() - startedAt,
            rows: result.results.length,
          });
          return result;
        } catch (error) {
          await this.client.cancelSearchJob(sid);
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
