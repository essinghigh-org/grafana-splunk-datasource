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
    response.results.forEach((result: Record<string, unknown>) => {
      response.fields.forEach((field: string) => {
        const value = result[field];
        if (value === undefined || value === null || value === '') {
          return;
        }

        frame.push({ text: String(value) });
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

    const searchType =
      queryRecord.searchType === 'base' || queryRecord.searchType === 'chain' || queryRecord.searchType === 'standard'
        ? queryRecord.searchType
        : 'standard';
    const mode = queryRecord.mode === 'base' || queryRecord.mode === 'chain' ? queryRecord.mode : undefined;

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
    
    const standardSearches = options.targets.filter(query => query.searchType === 'standard' || !query.searchType);
    const baseSearches = options.targets.filter(query => query.searchType === 'base');
    const chainSearches = options.targets.filter(query => query.searchType === 'chain');

    // Handle standard searches first - these are independent
    const standardResults: any[] = [];
    for (const query of standardSearches) {
      const result = await this.doRequest(query, options);
      standardResults.push(this.createDataFrame(query, result));
    }

    const baseSearchPromises: Array<Promise<BaseSearchResult>> = [];
    const baseResults: any[] = [];

    for (const query of baseSearches) {
      const cacheKey = generateCacheKey(query, options);
      const primaryKey = query.refId; // For compatibility with chain searches
      const searchIdKey = query.searchId;

      // 1. Check cache first using the proper cache key
      let cachedResult = this.findBaseSearchResult(cacheKey);

      if (cachedResult) {
        baseSearchPromises.push(Promise.resolve(cachedResult));
      } else {
        // 2. Check for existing in-flight promise
        let inflightPromise = this.baseSearchInflight.get(cacheKey);

        if (inflightPromise) {
          baseSearchPromises.push(inflightPromise);
        } else {
          // 3. No cached result, no in-flight promise: Execute new search
          const executeAndCacheBaseSearch = async (): Promise<BaseSearchResult> => {
            const result = await this.doRequest(query, options);
            const baseResult: BaseSearchResult = {
              sid: result.sid || '',
              searchId: query.searchId || query.refId, // Ensure searchId is populated
              refId: query.refId,
              fields: result.fields,
              results: result.results,
              timestamp: Date.now(),
              cacheKey: cacheKey, // Store the cache key for reference
            };
            
            // Store in cache with the proper cache key
            this.baseSearchCache.set(cacheKey, baseResult);
            // Also store with refId and searchId for chain search compatibility
            this.baseSearchCache.set(query.refId, baseResult);
            if (query.searchId) {
              this.baseSearchCache.set(query.searchId, baseResult);
            }
            return baseResult;
          };

          let newPromise = executeAndCacheBaseSearch();

          // Wrap promise with finally for cleanup
          newPromise = newPromise.finally(() => {
            this.baseSearchInflight.delete(cacheKey);
            this.baseSearchInflight.delete(primaryKey);
            if (searchIdKey) {
              this.baseSearchInflight.delete(searchIdKey);
            }
          });

          this.baseSearchInflight.set(cacheKey, newPromise);
          this.baseSearchInflight.set(primaryKey, newPromise);
          if (searchIdKey) {
            this.baseSearchInflight.set(searchIdKey, newPromise);
          }
          baseSearchPromises.push(newPromise);
        }
      }
    }

    const completedBaseSearchResults = await Promise.all(baseSearchPromises);

    for (const completedResult of completedBaseSearchResults) {
      // Find the original query corresponding to the result.
      // This is important because query options (like refId) are needed for createDataFrame.
      const originalQuery = baseSearches.find(q => q.refId === completedResult.refId || (completedResult.searchId && q.searchId === completedResult.searchId));
      if (originalQuery) {
        const dataFrame = this.createDataFrame(originalQuery, { fields: completedResult.fields, results: completedResult.results, sid: completedResult.sid });
        baseResults.push(dataFrame);
      } else {
        // This case should ideally not happen if logic is correct
      }
    }
    
    
    // Now execute chain searches
    const chainResults: any[] = [];
    for (const query of chainSearches) {
      if (query.baseSearchRefId) {
        let baseSearch = this.findBaseSearchResultByRefId(query.baseSearchRefId);

        if (!baseSearch || !this.isCacheValid(baseSearch)) {
          let awaitedBaseSearch: BaseSearchResult | null = null;
          let inflightPromise: Promise<BaseSearchResult> | undefined = undefined;
          const maxRetries = 3;
          const retryDelayMs = 100;

          for (let attempt = 0; attempt < maxRetries; attempt++) {
            // Attempt to find the promise in baseSearchInflight
            // Check by query.baseSearchRefId (could be refId or searchId of a base query)
            inflightPromise = this.baseSearchInflight.get(query.baseSearchRefId);

            // If not found, and baseSearchRefId might be a searchId,
            // try to find the original base query by its searchId and use its refId.
            // (Assuming baseSearchInflight is primarily keyed by refId for base queries,
            // but also by searchId if populated for the base query)
            if (!inflightPromise) {
              const baseQueryTarget = options.targets.find(
                t => t.searchType === 'base' && t.searchId === query.baseSearchRefId
              );
              if (baseQueryTarget) {
                // A base query's promise could be stored under its refId or its searchId
                inflightPromise = this.baseSearchInflight.get(baseQueryTarget.refId) || (baseQueryTarget.searchId ? this.baseSearchInflight.get(baseQueryTarget.searchId) : undefined) ;
              }
            }

            if (inflightPromise) {
              try {
                awaitedBaseSearch = await inflightPromise;
                if (awaitedBaseSearch && !this.isCacheValid(awaitedBaseSearch)) {
                  awaitedBaseSearch = null; // Stale data from resolved promise
                }
                if (awaitedBaseSearch) {
                  break; // Successfully got valid data
                }
              } catch (error) {
                awaitedBaseSearch = null; // Ensure null on error
              }
            }

            // If no promise or await failed/stale, and not the last attempt, delay
            if (!awaitedBaseSearch && attempt < maxRetries - 1) {
              await delay(retryDelayMs);
            }
          }
          baseSearch = awaitedBaseSearch; // Update baseSearch with the result of retry logic
        }

        if (baseSearch) {
          const chainResult = await this.doChainRequest(query, options, baseSearch);
          chainResults.push(this.createDataFrame(query, chainResult));
        } else {
          // Fallback: Execute as a regular search if no valid baseSearch could be obtained
          const result = await this.doRequest(query, options);
          chainResults.push(this.createDataFrame(query, result));
        }
      } else {
        // No baseSearchRefId, execute as regular search
        const result = await this.doRequest(query, options);
        chainResults.push(this.createDataFrame(query, result));
      }
    }
    
    const allResults = [...standardResults, ...baseResults, ...chainResults];
    return { data: allResults };
  }
  
  private createDataFrame(query: SplunkQuery, response: QueryRequestResults) {
    const moment = require('moment');
    
    // Prepare fields with proper typing
    const fields = response.fields.map((fieldName: any) => {
      const values: any[] = [];
      let fieldType = FieldType.string;
      
      // First pass: collect values
      response.results.forEach((result: any) => {
        if (fieldName === '_time') {
          const time = moment(result['_time']).format('YYYY-MM-DDTHH:mm:ssZ');
          values.push(time);
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
      throw new Error(`Chain search requires a base search SID (refId=${query.refId || 'unknown'}).`);
    }

    const data = new URLSearchParams({
      search: chainQuery,
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
    if (sid.length > 0) {
      const isComplete = await this.waitForSearchCompletion(sid);
      if (!isComplete) {
        throw new SplunkSearchTimeoutError(sid, 'chain', SEARCH_POLL_TIMEOUT_MS);
      }

      const result = await this.doGetAllResultsRequest(sid);
      return result;
    }

    throw new Error(`Chain search failed to return a SID (refId=${query.refId || 'unknown'}).`);
  }
}
