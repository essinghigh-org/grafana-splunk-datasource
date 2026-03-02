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
    dateTime: (value: number) => ({
      valueOf: () => value,
    }),
  };
});

jest.mock('@grafana/runtime', () => ({
  getBackendSrv: jest.fn(),
  getTemplateSrv: jest.fn(() => ({
    replace: (value: string) => value,
  })),
}));

const createDataSource = () => {
  const settings = {
    id: 1,
    uid: 'splunk-test',
    type: 'essinghigh-splunk-datasource',
    name: 'Splunk',
    access: 'proxy',
    url: 'http://localhost',
    jsonData: {},
  } as DataSourceInstanceSettings<SplunkDataSourceOptions>;

  return new DataSource(settings);
};

const createQueryRequest = (targets: any[] = []) =>
  ({
    app: 'dashboard',
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

    const result = await datasource.doGetAllResultsRequest('sid-pagination');

    expect(fetchMock.mock.calls.map(([request]) => request.params.offset)).toEqual([0, 2, 4]);
    expect(result.fields).toEqual(['_time', 'host']);
    expect(result.results).toEqual([
      { _time: '2024-01-01T00:00:00Z', host: 'api-1' },
      { _time: '2024-01-01T00:01:00Z', host: 'api-2' },
      { _time: '2024-01-01T00:02:00Z', host: 'api-3' },
      { _time: '2024-01-01T00:03:00Z', host: 'api-4' },
    ]);
  });
});

describe('DataSource runtime polling', () => {
  beforeEach(() => {
    mockedGetBackendSrv.mockReset();
  });

  it('times out waiting for search completion using bounded polling', async () => {
    const datasource = createDataSource();
    const statusSpy = jest.spyOn(datasource, 'doSearchStatusRequest').mockResolvedValue(false);

    const completed = await (datasource as any).waitForSearchCompletion('sid-timeout', 1, 5);

    expect(completed).toBe(false);
    expect(statusSpy).toHaveBeenCalled();
  });

  it('uses bounded polling helper in standard request flow', async () => {
    const datasource = createDataSource();
    jest.spyOn(datasource, 'doSearchRequest').mockResolvedValue({ sid: 'sid-standard' });
    const waitSpy = jest.spyOn(datasource as any, 'waitForSearchCompletion').mockResolvedValue(false);
    const getAllSpy = jest.spyOn(datasource, 'doGetAllResultsRequest');

    const result = await datasource.doRequest(
      { refId: 'A', queryText: 'index=_internal', searchType: 'standard' } as any,
      createQueryRequest([{ refId: 'A' }])
    );

    expect(waitSpy).toHaveBeenCalledWith('sid-standard');
    expect(getAllSpy).not.toHaveBeenCalled();
    expect(result).toEqual(
      expect.objectContaining({
        sid: 'sid-standard',
        fields: [],
        results: [],
      })
    );
  });

  it('uses bounded polling helper in chain flow and falls back to cached results on timeout', async () => {
    const datasource = createDataSource();
    const fetchMock = jest.fn().mockReturnValue(of({ data: { sid: 'sid-chain' } }));
    mockedGetBackendSrv.mockReturnValue({ fetch: fetchMock });
    const waitSpy = jest.spyOn(datasource as any, 'waitForSearchCompletion').mockResolvedValue(false);
    const getAllSpy = jest.spyOn(datasource, 'doGetAllResultsRequest');

    const baseSearch = {
      sid: 'sid-base',
      searchId: 'base-search',
      refId: 'A',
      fields: ['host'],
      results: [{ host: 'api-1' }],
      timestamp: Date.now(),
      cacheKey: 'base-cache-key',
    };

    const result = await datasource.doChainRequest(
      { refId: 'B', queryText: '| stats count by host', searchType: 'chain' } as any,
      createQueryRequest([{ refId: 'B', queryText: '| stats count by host', searchType: 'chain' }]),
      baseSearch
    );

    expect(waitSpy).toHaveBeenCalledWith('sid-chain');
    expect(getAllSpy).not.toHaveBeenCalled();
    expect(result).toEqual({
      fields: ['host'],
      results: [{ host: 'api-1' }],
    });
  });
});

describe('DataSource base-search state isolation', () => {
  it('does not share in-flight base-search promises across datasource instances', async () => {
    const datasourceA = createDataSource();
    const datasourceB = createDataSource();

    let resolveA!: (value: QueryRequestResults & { sid?: string }) => void;
    const pendingA = new Promise<QueryRequestResults & { sid?: string }>((resolve) => {
      resolveA = resolve;
    });

    const doRequestASpy = jest.spyOn(datasourceA, 'doRequest').mockReturnValue(pendingA as any);
    const doRequestBSpy = jest.spyOn(datasourceB, 'doRequest').mockResolvedValue({
      sid: 'sid-b',
      fields: ['host'],
      results: [{ host: 'api-b' }],
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

    expect(doRequestBSpy).toHaveBeenCalledTimes(1);

    resolveA({
      sid: 'sid-a',
      fields: ['host'],
      results: [{ host: 'api-a' }],
    });

    await queryAPromise;
    await queryBPromise;

    expect(doRequestASpy).toHaveBeenCalledTimes(1);
    expect(doRequestBSpy).toHaveBeenCalledTimes(1);
  });
});
