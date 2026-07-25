import type { DataSourceInstanceSettings } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
import { of } from 'rxjs';

import { DataSource } from '../datasource';
import { QueryRequestResults, SplunkDataSourceOptions } from '../types';

jest.mock('../VariableQueryEditor', () => ({
  VariableQueryEditor: () => null,
}));

jest.mock('@grafana/data', () => {
  class DataSourceApi {
    constructor(_instanceSettings: unknown) {}
  }

  class CustomVariableSupport {
    getType() {
      return 'custom';
    }
  }

  return {
    CustomVariableSupport,
    DataSourceApi,
    FieldType: {
      string: 'string',
      time: 'time',
      number: 'number',
    },
    dateTime: (value: number | string | null | undefined) => ({
      valueOf: () => {
        const fixedNow = Date.parse('2026-03-02T00:00:00Z');

        if (typeof value === 'number') {
          return value;
        }

        if (value === null || value === undefined) {
          return fixedNow;
        }

        if (typeof value === 'string' && value.trim() === '') {
          return fixedNow;
        }

        return Date.parse(String(value));
      },
    }),
  };
});

jest.mock('@grafana/runtime', () => ({
  getBackendSrv: jest.fn(),
  getTemplateSrv: jest.fn(() => ({
    replace: (value: string, scopedVars: Record<string, { value?: unknown }> = {}) =>
      value.replace(/\$\{([^}]+)\}/g, (match, name) =>
        scopedVars[name]?.value === undefined ? match : String(scopedVars[name].value)
      ),
  })),
}));

const createDataSource = (jsonData: SplunkDataSourceOptions = {}) => {
  const settings = {
    id: 1,
    uid: 'splunk-test',
    type: 'essinghigh-splunk-datasource',
    name: 'Splunk',
    access: 'proxy',
    url: 'http://localhost',
    jsonData,
  } as DataSourceInstanceSettings<SplunkDataSourceOptions>;

  return new DataSource(settings);
};

const createQueryRequest = (targets: any[] = []) =>
  ({
    app: 'dashboard',
    dashboardUID: 'dashboard-1',
    requestId: 'runtime-test',
    timezone: 'utc',
    interval: '1m',
    intervalMs: 60_000,
    maxDataPoints: 1000,
    range: {
      from: { valueOf: () => 0 },
      to: { valueOf: () => 60_000 },
      raw: {
        from: 'now-1m',
        to: 'now',
      },
    },
    scopedVars: {},
    startTime: Date.now(),
    targets,
  }) as any;

const mockedGetBackendSrv = getBackendSrv as unknown as jest.Mock;

describe('DataSource.metricFindQuery', () => {
  it('exposes CustomVariableSupport from datasource.variables', () => {
    const datasource = createDataSource();

    expect(datasource.variables).toBeDefined();
    expect(datasource.variables?.getType()).toBe('custom');
    expect((datasource.variables as any).editor).toBeDefined();
  });

  it('normalizes string queries and fills safe fallback options', async () => {
    const datasource = createDataSource();
    const doRequestSpy = jest.spyOn(datasource, 'doRequest').mockResolvedValue({
      fields: ['host'],
      results: [{ host: 'web-1' }],
    } as QueryRequestResults);

    const result = await datasource.metricFindQuery('index=_internal | fields host');

    expect(result).toEqual([{ text: 'web-1' }]);
    expect(doRequestSpy).toHaveBeenCalledTimes(1);

    const [queryArg, optionsArg] = doRequestSpy.mock.calls[0];
    expect(queryArg).toEqual(
      expect.objectContaining({
        refId: 'metricFindQuery',
        queryText: 'index=_internal | fields host',
        searchType: 'standard',
      })
    );
    expect(optionsArg.scopedVars).toEqual({});
    expect(optionsArg.targets).toEqual([queryArg]);
    expect(optionsArg.range.from).toBeDefined();
    expect(optionsArg.range.to).toBeDefined();
  });

  it('preserves caller-provided range and scopedVars in options', async () => {
    const datasource = createDataSource();
    const doRequestSpy = jest.spyOn(datasource, 'doRequest').mockResolvedValue({
      fields: ['host'],
      results: [{ host: 'web-2' }],
    } as QueryRequestResults);

    const range = {
      from: { valueOf: () => 1000 },
      to: { valueOf: () => 2000 },
      raw: {
        from: 'now-5m',
        to: 'now',
      },
    } as any;
    const scopedVars = {
      host: {
        text: 'web-2',
        value: 'web-2',
      },
    } as any;

    await datasource.metricFindQuery('index=_internal | fields host', { range, scopedVars } as any);

    expect(doRequestSpy).toHaveBeenCalledTimes(1);
    const [, optionsArg] = doRequestSpy.mock.calls[0];
    expect(optionsArg.range).toBe(range);
    expect(optionsArg.scopedVars).toBe(scopedVars);
  });

  it('filters undefined/null/empty values but keeps falsey non-empty values', async () => {
    const datasource = createDataSource();
    jest.spyOn(datasource, 'doRequest').mockResolvedValue({
      fields: ['host'],
      results: [{ host: undefined }, { host: null }, { host: '' }, { host: 0 }, { host: false }, { host: 'api-1' }],
    } as QueryRequestResults);

    const result = await datasource.metricFindQuery('index=_internal | fields host');

    expect(result).toEqual([{ text: '0' }, { text: 'false' }, { text: 'api-1' }]);
  });

  it('deduplicates metricFindQuery values while preserving first-seen order', async () => {
    const datasource = createDataSource();
    jest.spyOn(datasource, 'doRequest').mockResolvedValue({
      fields: ['host', 'source'],
      results: [
        { host: 'api-1', source: 'src-a' },
        { host: 'api-1', source: 'src-b' },
        { host: 'api-2', source: 'src-a' },
        { host: 'api-1', source: 'src-b' },
      ],
    } as QueryRequestResults);

    const result = await datasource.metricFindQuery('index=_internal | fields host, source');

    expect(result).toEqual([{ text: 'api-1' }, { text: 'src-a' }, { text: 'src-b' }, { text: 'api-2' }]);
  });

  it('supports legacy variable-query input that uses query', async () => {
    const datasource = createDataSource();
    const doRequestSpy = jest.spyOn(datasource, 'doRequest').mockResolvedValue({
      fields: ['source'],
      results: [{ source: 'syslog' }],
    } as QueryRequestResults);

    const result = await datasource.metricFindQuery({ query: 'index=os source=*' });

    expect(result).toEqual([{ text: 'syslog' }]);
    expect(doRequestSpy).toHaveBeenCalledTimes(1);
    expect(doRequestSpy.mock.calls[0][0]).toEqual(
      expect.objectContaining({
        queryText: 'index=os source=*',
      })
    );
  });

  it('passes through object query metadata when normalizing variable query input', async () => {
    const datasource = createDataSource();
    const doRequestSpy = jest.spyOn(datasource, 'doRequest').mockResolvedValue({
      fields: ['service'],
      results: [{ service: 'api' }],
    } as QueryRequestResults);

    const rawVariableQuery = {
      query: 'index=prod service=*',
      refId: 'customRef',
      searchType: 'standard',
      source: 'dashboard-variable',
      extraMetadata: {
        owner: 'sre',
      },
    };

    await datasource.metricFindQuery(rawVariableQuery as any);

    const [queryArg] = doRequestSpy.mock.calls[0];
    expect(queryArg).toEqual(
      expect.objectContaining({
        queryText: 'index=prod service=*',
        refId: 'customRef',
        source: 'dashboard-variable',
        extraMetadata: {
          owner: 'sre',
        },
      })
    );
  });

  it('returns an empty list for blank variable queries without calling doRequest', async () => {
    const datasource = createDataSource();
    const doRequestSpy = jest.spyOn(datasource, 'doRequest');

    const result = await datasource.metricFindQuery({ queryText: '   ' });

    expect(result).toEqual([]);
    expect(doRequestSpy).not.toHaveBeenCalled();
  });
});

describe('DataSource runtime pagination', () => {
  beforeEach(() => {
    mockedGetBackendSrv.mockReset();
  });

  it('paginates result offsets sequentially without skipping pages', async () => {
    const datasource = createDataSource();
    const fetchMock = jest.fn(({ params }: any) => {
      if (params.offset === 0) {
        return of({
          data: {
            post_process_count: 2,
            fields: [{ name: '_time' }, { name: 'host' }],
            results: [
              { _time: '2024-01-01T00:00:00Z', host: 'api-1' },
              { _time: '2024-01-01T00:01:00Z', host: 'api-2' },
            ],
          },
        });
      }

      if (params.offset === 2) {
        return of({
          data: {
            post_process_count: 2,
            fields: [{ name: '_time' }, { name: 'host' }],
            results: [
              { _time: '2024-01-01T00:02:00Z', host: 'api-3' },
              { _time: '2024-01-01T00:03:00Z', host: 'api-4' },
            ],
          },
        });
      }

      if (params.offset === 4) {
        return of({
          data: {
            post_process_count: 0,
            fields: [{ name: '_time' }, { name: 'host' }],
            results: [],
          },
        });
      }

      throw new Error(`Unexpected pagination offset: ${params.offset}`);
    });

    mockedGetBackendSrv.mockReturnValue({ fetch: fetchMock });

    const result = await datasource.client.doGetAllResultsRequest('sid-pagination');

    expect(fetchMock.mock.calls.map(([request]) => request.params.offset)).toEqual([0, 2, 4]);
    expect(result.fields).toEqual(['_time', 'host']);
    expect(result.results).toEqual([
      { _time: '2024-01-01T00:00:00Z', host: 'api-1' },
      { _time: '2024-01-01T00:01:00Z', host: 'api-2' },
      { _time: '2024-01-01T00:02:00Z', host: 'api-3' },
      { _time: '2024-01-01T00:03:00Z', host: 'api-4' },
    ]);
  });

  it('stops at the configured row limit and returns a truncation warning', async () => {
    const datasource = createDataSource({ maxResultRows: 3 });
    const fetchMock = jest.fn().mockReturnValue(
      of({
        data: {
          fields: [{ name: 'host' }],
          results: [{ host: 'api-1' }, { host: 'api-2' }, { host: 'api-3' }],
        },
      })
    );
    mockedGetBackendSrv.mockReturnValue({ fetch: fetchMock });

    const result = await datasource.client.doGetAllResultsRequest('sid-limited');

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(result.results).toHaveLength(3);
    expect(result.warning).toBe('Splunk results truncated at 3 rows.');
  });
});

describe('DataSource runtime polling', () => {
  beforeEach(() => {
    mockedGetBackendSrv.mockReset();
  });

  it('times out waiting for search completion using bounded polling', async () => {
    const datasource = createDataSource();
    const statusSpy = jest
      .spyOn(datasource.client, 'doSearchStatusRequest')
      .mockResolvedValue({ state: 'RUNNING', messages: [] });

    const completed = await (datasource as any).waitForSearchCompletion('sid-timeout', 1, 5);

    expect(completed).toEqual({ state: 'RUNNING', messages: [] });
    expect(statusSpy).toHaveBeenCalled();
  });

  it('uses bounded polling helper in standard request flow', async () => {
    const datasource = createDataSource();
    jest.spyOn(datasource.client, 'doSearchRequest').mockResolvedValue({ sid: 'sid-standard' });
    const waitSpy = jest
      .spyOn(datasource as any, 'waitForSearchCompletion')
      .mockResolvedValue({ state: 'RUNNING', messages: [] });
    const getAllSpy = jest.spyOn(datasource.client, 'doGetAllResultsRequest');
    const cancelSpy = jest.spyOn(datasource.client as any, 'cancelSearchJob').mockResolvedValue(undefined);

    await expect(
      datasource.doRequest(
        { refId: 'A', queryText: 'index=_internal', searchType: 'standard' } as any,
        createQueryRequest([{ refId: 'A' }])
      )
    ).rejects.toMatchObject({
      name: 'SplunkSearchTimeoutError',
      code: 'SPLUNK_SEARCH_TIMEOUT',
      sid: 'sid-standard',
      searchType: 'standard',
    });

    expect(waitSpy).toHaveBeenCalledWith('sid-standard', 100, 30_000, 'splunk:dashboard-1:unknown-panel:A:standard');
    expect(getAllSpy).not.toHaveBeenCalled();
    expect(cancelSpy).toHaveBeenCalledWith('sid-standard');
  });

  it('surfaces Splunk failure messages instead of fetching results', async () => {
    const datasource = createDataSource();
    jest.spyOn(datasource.client, 'doSearchRequest').mockResolvedValue({ sid: 'sid-failed' });
    jest
      .spyOn(datasource as any, 'waitForSearchCompletion')
      .mockResolvedValue({ state: 'FAILED', messages: ['Invalid search command'] });
    const getAllSpy = jest.spyOn(datasource.client, 'doGetAllResultsRequest');
    jest.spyOn(datasource.client as any, 'cancelSearchJob').mockResolvedValue(undefined);

    await expect(
      datasource.doRequest(
        { refId: 'A', queryText: 'index=_internal', searchType: 'standard' } as any,
        createQueryRequest([{ refId: 'A' }])
      )
    ).rejects.toThrow('Splunk search failed (sid=sid-failed): Invalid search command');

    expect(getAllSpy).not.toHaveBeenCalled();
  });

  it('sends explicit fixed times for a base search', async () => {
    const datasource = createDataSource();
    const fetchMock = jest.fn().mockReturnValue(of({ data: { sid: 'sid-base' } }));
    mockedGetBackendSrv.mockReturnValue({ fetch: fetchMock });

    await datasource.client.doSearchRequest(
      {
        refId: 'A',
        queryText: 'index=_internal',
        searchType: 'base',
        useDashboardTimeRange: false,
        earliest: '-30d@d',
        latest: 'now',
      },
      createQueryRequest([])
    );

    const body = new URLSearchParams(fetchMock.mock.calls[0][0].data);
    expect(body.get('earliest_time')).toBe('-30d@d');
    expect(body.get('latest_time')).toBe('now');
  });

  it('uses bounded polling helper in chain flow and surfaces timeout failures', async () => {
    const datasource = createDataSource();
    const fetchMock = jest.fn().mockReturnValue(of({ data: { sid: 'sid-chain' } }));
    mockedGetBackendSrv.mockReturnValue({ fetch: fetchMock });
    const waitSpy = jest
      .spyOn(datasource as any, 'waitForSearchCompletion')
      .mockResolvedValue({ state: 'RUNNING', messages: [] });
    const getAllSpy = jest.spyOn(datasource.client, 'doGetAllResultsRequest');

    const baseSearch = {
      sid: 'sid-base',
      searchId: 'base-search',
      refId: 'A',
      fields: ['host'],
      results: [{ host: 'api-1' }],
      timestamp: Date.now(),
      cacheKey: 'base-cache-key',
      executionMs: 1,
    };

    await expect(
      datasource.doChainRequest(
        { refId: 'B', queryText: '| stats count by host', searchType: 'chain' } as any,
        createQueryRequest([{ refId: 'B', queryText: '| stats count by host', searchType: 'chain' }]),
        baseSearch
      )
    ).rejects.toThrow('Splunk chain search timed out after 30000ms (sid=sid-chain).');

    expect(waitSpy).toHaveBeenCalledWith('sid-chain', 100, 30_000, 'splunk:dashboard-1:unknown-panel:B:chain');
    expect(getAllSpy).not.toHaveBeenCalled();
  });

  it('surfaces chain execution failures instead of returning cached base results', async () => {
    const datasource = createDataSource();
    const expectedError = new Error('splunk unavailable');
    const fetchMock = jest.fn().mockImplementation(() => {
      throw expectedError;
    });
    mockedGetBackendSrv.mockReturnValue({ fetch: fetchMock });

    const baseSearch = {
      sid: 'sid-base',
      searchId: 'base-search',
      refId: 'A',
      fields: ['host'],
      results: [{ host: 'api-1' }],
      timestamp: Date.now(),
      cacheKey: 'base-cache-key',
      executionMs: 1,
    };

    await expect(
      datasource.doChainRequest(
        { refId: 'B', queryText: '| stats count by host', searchType: 'chain' } as any,
        createQueryRequest([{ refId: 'B', queryText: '| stats count by host', searchType: 'chain' }]),
        baseSearch
      )
    ).rejects.toThrow('Chain search "B" failed against base search "A": splunk unavailable');
  });

  it('throws explicit chain SID error when job creation response omits sid', async () => {
    const datasource = createDataSource();
    const fetchMock = jest.fn().mockReturnValue(of({ data: {} }));
    mockedGetBackendSrv.mockReturnValue({ fetch: fetchMock });
    const waitSpy = jest.spyOn(datasource as any, 'waitForSearchCompletion');
    const getAllSpy = jest.spyOn(datasource.client, 'doGetAllResultsRequest');

    const baseSearch = {
      sid: 'sid-base',
      searchId: 'base-search',
      refId: 'A',
      fields: ['host'],
      results: [{ host: 'api-1' }],
      timestamp: Date.now(),
      cacheKey: 'base-cache-key',
      executionMs: 1,
    };

    let thrown: unknown;
    try {
      await datasource.doChainRequest(
        { refId: 'B', queryText: '| stats count by host', searchType: 'chain' } as any,
        createQueryRequest([{ refId: 'B', queryText: '| stats count by host', searchType: 'chain' }]),
        baseSearch
      );
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toBeDefined();
    expect(thrown).not.toBeInstanceOf(TypeError);
    expect((thrown as Error).message).toBe(
      'Chain search "B" failed against base search "A": Chain search "B" returned an empty SID.'
    );
    expect(waitSpy).not.toHaveBeenCalled();
    expect(getAllSpy).not.toHaveBeenCalled();
  });
});

describe('DataSource query orchestration', () => {
  it('runs base searches before chains but returns frames in target order', async () => {
    const datasource = createDataSource();
    const calls: string[] = [];

    jest.spyOn(datasource, 'doRequest').mockImplementation(async (query) => {
      calls.push(`standard:${query.refId}`);
      return { fields: ['value'], results: [{ value: `${query.refId}-value` }] };
    });
    jest.spyOn(datasource as any, 'executeBaseSearch').mockImplementation(async (...args: any[]) => {
      const [query, _options, cacheKey] = args;
      calls.push(`base:${query.refId}`);
      return {
        sid: `sid-${query.refId}`,
        searchId: query.searchId,
        refId: query.refId,
        timestamp: Date.now(),
        cacheKey,
        executionMs: 1,
      };
    });
    jest.spyOn(datasource.client as any, 'searchJobExists').mockResolvedValue(true);
    const chainSpy = jest.spyOn(datasource, 'doChainRequest').mockImplementation(async () => {
      calls.push('chain:C');
      return { fields: ['value'], results: [{ value: 'C-value' }] };
    });

    const response = await datasource.query(
      createQueryRequest([
        {
          refId: 'C',
          queryText: '| stats count by host',
          searchType: 'chain',
          baseSearchRefId: 'base-search',
        },
        {
          refId: 'A',
          queryText: 'index=_internal | head 1',
          searchType: 'standard',
        },
        {
          refId: 'B',
          queryText: 'index=_internal | head 100',
          searchType: 'base',
          searchId: 'base-search',
        },
      ])
    );

    expect(response.data.map((frame) => frame.refId)).toEqual(['C', 'A', 'B']);
    expect(calls).toEqual(['standard:A', 'base:B', 'chain:C']);
    expect(chainSpy).toHaveBeenCalledWith(
      expect.objectContaining({ refId: 'C' }),
      expect.anything(),
      expect.objectContaining({ refId: 'B', searchId: 'base-search', sid: 'sid-B' })
    );
  });

  it('does not download hidden base results and exposes base metadata', async () => {
    const datasource = createDataSource();
    jest.spyOn(datasource.client, 'doSearchRequest').mockResolvedValue({ sid: 'sid-base' });
    jest.spyOn(datasource as any, 'waitForSearchCompletion').mockResolvedValue({ state: 'DONE', messages: [] });
    const getAllSpy = jest.spyOn(datasource.client, 'doGetAllResultsRequest');

    const response = await datasource.query(
      createQueryRequest([
        {
          refId: 'A',
          queryText: 'index=_internal',
          searchType: 'base',
          searchId: 'base-search',
        },
      ])
    );

    expect(getAllSpy).not.toHaveBeenCalled();
    expect(response.data[0].fields).toEqual([]);
    expect(response.data[0].meta?.custom?.baseSearch).toEqual(
      expect.objectContaining({ searchId: 'base-search', sid: 'sid-base', cache: 'miss' })
    );
  });

  it('resolves a base search running in another panel on the same dashboard', async () => {
    const datasource = createDataSource();
    jest.spyOn(datasource as any, 'executeBaseSearch').mockImplementation(
      async (...args: any[]) =>
        new Promise((resolve) =>
          setTimeout(
            () =>
              resolve({
                sid: 'sid-external-base',
                searchId: 'snow-base',
                refId: 'A',
                timestamp: Date.now(),
                cacheKey: args[2],
                executionMs: 10,
              }),
            10
          )
        )
    );
    jest.spyOn(datasource.client as any, 'searchJobExists').mockResolvedValue(true);
    const chainSpy = jest.spyOn(datasource, 'doChainRequest').mockResolvedValue({ fields: [], results: [] });

    await Promise.all([
      datasource.query(
        createQueryRequest([
          { refId: 'A', queryText: 'index=servicenow', searchType: 'base', searchId: 'snow-base' },
        ])
      ),
      datasource.query(
        createQueryRequest([
          {
            refId: 'A',
            queryText: '| stats count by updater',
            searchType: 'chain',
            baseSearchRefId: 'snow-base',
          },
        ])
      ),
    ]);

    expect(chainSpy).toHaveBeenCalledWith(
      expect.objectContaining({ baseSearchRefId: 'snow-base' }),
      expect.anything(),
      expect.objectContaining({ sid: 'sid-external-base', searchId: 'snow-base' })
    );
  });
});

describe('DataSource base-search state isolation', () => {
  it('does not share in-flight base-search promises across datasource instances', async () => {
    const datasourceA = createDataSource();
    const datasourceB = createDataSource();

    let resolveA!: (value: any) => void;
    const pendingA = new Promise<any>((resolve) => {
      resolveA = resolve;
    });

    const executeASpy = jest.spyOn(datasourceA as any, 'executeBaseSearch').mockReturnValue(pendingA);
    const executeBSpy = jest.spyOn(datasourceB as any, 'executeBaseSearch').mockResolvedValue({
      sid: 'sid-b',
      searchId: 'shared-base',
      refId: 'A',
      timestamp: Date.now(),
      cacheKey: 'cache-b',
      executionMs: 1,
    });

    const baseTarget = {
      refId: 'A',
      queryText: 'index=_internal | head 2',
      searchType: 'base',
      searchId: 'shared-base',
    } as any;

    const queryAPromise = datasourceA.query(createQueryRequest([baseTarget]));
    await Promise.resolve();

    const queryBPromise = datasourceB.query(createQueryRequest([baseTarget]));
    await Promise.resolve();

    expect(executeBSpy).toHaveBeenCalledTimes(1);

    resolveA({
      sid: 'sid-a',
      searchId: 'shared-base',
      refId: 'A',
      timestamp: Date.now(),
      cacheKey: 'cache-a',
      executionMs: 1,
    });

    await queryAPromise;
    await queryBPromise;

    expect(executeASpy).toHaveBeenCalledTimes(1);
    expect(executeBSpy).toHaveBeenCalledTimes(1);
  });

  it('does not reuse inflight base search when different base queries share the same searchId', async () => {
    const datasource = createDataSource();

    const executeSpy = jest.spyOn(datasource as any, 'executeBaseSearch').mockImplementation(async (...args: any[]) => {
      const [query, _options, cacheKey] = args;
      return await new Promise<any>((resolve) => {
        const delayMs = query.refId === 'A' ? 40 : 10;

        setTimeout(() => {
          resolve({
            sid: `sid-${query.refId}`,
            searchId: query.searchId,
            refId: query.refId,
            timestamp: Date.now(),
            cacheKey,
            executionMs: delayMs,
          });
        }, delayMs);
      });
    });

    const firstRequest = createQueryRequest([
      {
        refId: 'A',
        queryText: 'index=alpha',
        searchType: 'base',
        searchId: 'shared-search-id',
      },
    ]);

    const secondRequest = createQueryRequest([
      {
        refId: 'B',
        queryText: 'index=beta',
        searchType: 'base',
        searchId: 'shared-search-id',
      },
    ]);

    secondRequest.range = {
      from: { valueOf: () => 3000 },
      to: { valueOf: () => 4000 },
      raw: {
        from: 'now-10m',
        to: 'now-5m',
      },
    };

    await Promise.all([datasource.query(firstRequest), datasource.query(secondRequest)]);

    expect(executeSpy).toHaveBeenCalledTimes(2);
    expect(executeSpy.mock.calls.map(([queryArg]: any[]) => queryArg.refId).sort()).toEqual(['A', 'B']);
  });
});

describe('DataSource base-search cache behavior', () => {
  const baseTarget = {
    refId: 'A',
    queryText: 'index=main host=${baseHost}',
    searchType: 'base',
    searchId: 'shared-base',
    useDashboardTimeRange: false,
    earliest: '-30d@d',
    latest: 'now',
  };
  const chainTarget = {
    refId: 'B',
    queryText: '| search service=${chainService} | stats count',
    searchType: 'chain',
    baseSearchRefId: 'shared-base',
  };

  const setVariables = (request: any, baseHost: string, chainService: string) => {
    request.scopedVars = {
      baseHost: { value: baseHost, text: baseHost },
      chainService: { value: chainService, text: chainService },
    };
    return request;
  };

  const mockBaseAndChain = (datasource: DataSource) => {
    const executeSpy = jest.spyOn(datasource as any, 'executeBaseSearch').mockImplementation(async (...args: any[]) => {
      const [query, _options, cacheKey] = args;
      return {
        sid: `sid-${executeSpy.mock.calls.length}`,
        searchId: query.searchId || query.refId,
        refId: query.refId,
        timestamp: Date.now(),
        cacheKey,
        executionMs: 1,
      };
    });
    jest.spyOn(datasource.client as any, 'searchJobExists').mockResolvedValue(true);
    jest.spyOn(datasource, 'doChainRequest').mockResolvedValue({ fields: [], results: [] });
    return executeSpy;
  };

  it('reuses the base when only a chain variable changes', async () => {
    const datasource = createDataSource();
    const executeSpy = mockBaseAndChain(datasource);

    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api'));
    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'worker'));

    expect(executeSpy).toHaveBeenCalledTimes(1);
  });

  it('reruns the base when a variable referenced by the base changes', async () => {
    const datasource = createDataSource();
    const executeSpy = mockBaseAndChain(datasource);

    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api'));
    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-2', 'api'));

    expect(executeSpy).toHaveBeenCalledTimes(2);
  });

  it('reuses a fixed-range base when the dashboard range changes', async () => {
    const datasource = createDataSource();
    const executeSpy = mockBaseAndChain(datasource);
    const first = setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api');
    const second = setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api');
    second.range = {
      from: { valueOf: () => 3000 },
      to: { valueOf: () => 4000 },
      raw: { from: 'now-10m', to: 'now-5m' },
    };

    await datasource.query(first);
    await datasource.query(second);

    expect(executeSpy).toHaveBeenCalledTimes(1);
  });

  it('does not collide across dashboards that use the same search ID', async () => {
    const datasource = createDataSource();
    const executeSpy = mockBaseAndChain(datasource);
    const first = setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api');
    const second = setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api');
    first.dashboardUID = 'dashboard-a';
    second.dashboardUID = 'dashboard-b';

    await datasource.query(first);
    await datasource.query(second);

    expect(executeSpy).toHaveBeenCalledTimes(2);
  });

  it('reruns an expired base cache entry', async () => {
    const datasource = createDataSource({ baseSearchCacheTtlMinutes: 0.1 });
    const executeSpy = mockBaseAndChain(datasource);
    const nowSpy = jest.spyOn(Date, 'now').mockReturnValue(1_000_000);

    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api'));
    nowSpy.mockReturnValue(1_007_000);
    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api'));

    expect(executeSpy).toHaveBeenCalledTimes(2);
    nowSpy.mockRestore();
  });

  it('deduplicates concurrent base executions', async () => {
    const datasource = createDataSource();
    const executeSpy = jest.spyOn(datasource as any, 'executeBaseSearch').mockImplementation(async (...args: any[]) => {
      const [query, _options, cacheKey] = args;
      return await new Promise((resolve) =>
        setTimeout(
          () =>
            resolve({
              sid: 'sid-base',
              searchId: query.searchId,
              refId: query.refId,
              timestamp: Date.now(),
              cacheKey,
              executionMs: 10,
            }),
          10
        )
      );
    });
    jest.spyOn(datasource.client as any, 'searchJobExists').mockResolvedValue(true);
    jest.spyOn(datasource, 'doChainRequest').mockResolvedValue({ fields: [], results: [] });

    await Promise.all([
      datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api')),
      datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'worker')),
    ]);

    expect(executeSpy).toHaveBeenCalledTimes(1);
  });

  it('reruns a missing cached SID once before retrying the chain', async () => {
    const datasource = createDataSource();
    const executeSpy = jest.spyOn(datasource as any, 'executeBaseSearch').mockImplementation(async (...args: any[]) => {
      const [query, _options, cacheKey] = args;
      return {
        sid: `sid-${executeSpy.mock.calls.length}`,
        searchId: query.searchId,
        refId: query.refId,
        timestamp: Date.now(),
        cacheKey,
        executionMs: 1,
      };
    });
    jest
      .spyOn(datasource.client as any, 'doSearchStatusRequest')
      .mockResolvedValueOnce({ state: 'DONE', messages: [] })
      .mockResolvedValueOnce({ state: 'FAILED', messages: [] });
    const chainSpy = jest.spyOn(datasource, 'doChainRequest').mockResolvedValue({ fields: [], results: [] });

    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'api'));
    await datasource.query(setVariables(createQueryRequest([baseTarget, chainTarget]), 'web-1', 'worker'));

    expect(executeSpy).toHaveBeenCalledTimes(2);
    expect(chainSpy.mock.calls[1][2].sid).toBe('sid-2');
  });
});

describe('DataSource.createDataFrame', () => {
  it('maps missing/undefined/null/empty _time values to null and keeps invalid timestamps null', () => {
    const datasource = createDataSource();

    const frame = (datasource as any).createDataFrame(
      {
        refId: 'A',
        queryText: 'search index=_internal',
      },
      {
        fields: ['_time', 'count'],
        results: [
          { _time: '2024-01-01T00:00:00Z', count: '2' },
          { _time: undefined, count: '3' },
          { _time: null, count: '4' },
          { _time: '', count: '5' },
          { _time: '   ', count: '6' },
          { count: '7' },
          { _time: 'invalid-time', count: '8' },
        ],
      }
    );

    expect(frame.fields[0]).toEqual(
      expect.objectContaining({
        name: '_time',
        type: 'time',
      })
    );
    expect(frame.fields[0].values).toEqual([Date.parse('2024-01-01T00:00:00Z'), null, null, null, null, null, null]);
    expect(frame.fields[1]).toEqual(
      expect.objectContaining({
        name: 'count',
        type: 'number',
        values: [2, 3, 4, 5, 6, 7, 8],
      })
    );
  });
});
