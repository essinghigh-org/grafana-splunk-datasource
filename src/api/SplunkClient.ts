import { getBackendSrv, getTemplateSrv } from '@grafana/runtime';
import { lastValueFrom } from 'rxjs';
import { DataQueryRequest } from '@grafana/data';
import { SplunkQuery, QueryRequestResults, BaseSearchResult } from '../types';

export const SEARCH_TIMEOUT_ERROR_CODE = 'SPLUNK_SEARCH_TIMEOUT';

export interface SearchStatus {
  state: string;
  messages: string[];
}


export class SplunkSearchTimeoutError extends Error {
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
export class SplunkSearchFailedError extends Error {
  constructor(
    readonly sid: string,
    messages: string[]
  ) {
    super(`Splunk search failed (sid=${sid})${messages.length ? `: ${messages.join('; ')}` : '.'}`);
    this.name = 'SplunkSearchFailedError';

  }
}
export class SplunkClient {
  constructor(
    private readonly url: string,
    private readonly maxResultPages: number,
    private readonly maxResultRows: number,
    private readonly maxResponseBytes: number,
    private readonly debugLogger: (event: string, details: Record<string, unknown>) => void
  ) {}

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

  async doChainSearchRequest(
    query: SplunkQuery,
    options: DataQueryRequest<SplunkQuery>,
    baseSearch: BaseSearchResult,
    requestId?: string
  ): Promise<{ sid: string } | null> {
    const from = Math.floor(options.range.from.valueOf() / 1000);
    const to = Math.floor(options.range.to.valueOf() / 1000);

    let chainQuery = (query.queryText || '').trim();
    if (baseSearch.sid) {
      const vars = getTemplateSrv().replace(chainQuery, options.scopedVars).trim();
      chainQuery = vars.startsWith('|')
        ? `| loadjob ${baseSearch.sid} ${vars}`
        : `| loadjob ${baseSearch.sid} | ${vars}`;
    }

    const data = new URLSearchParams({
      search: chainQuery,
      output_mode: 'json',
      earliest_time: from.toString(),
      latest_time: to.toString(),
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
    const sid: string = (response.data as any)?.sid ?? '';
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

    this.debugLogger('result retrieval complete', { sid, rows: results.length, pages, responseBytes, warning });
    return { fields, results, warning };
  }

  async cancelSearchJob(sid: string) {
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
      this.debugLogger('cancelled Splunk job', { sid });
    } catch {
      this.debugLogger('failed to cancel Splunk job', { sid });
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

  async searchJobExists(sid: string, requestId?: string): Promise<boolean> {
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
}
