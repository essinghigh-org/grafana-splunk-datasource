import type { DataSourceInstanceSettings } from '@grafana/data';

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
