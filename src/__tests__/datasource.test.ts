import type { DataSourceInstanceSettings } from '@grafana/data';

import { DataSource } from '../datasource';
import { QueryRequestResults, SplunkDataSourceOptions } from '../types';

jest.mock('@grafana/data', () => {
  class DataSourceApi {
    constructor(_instanceSettings: unknown) {}
  }

  return {
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

  it('returns an empty list for blank variable queries without calling doRequest', async () => {
    const datasource = createDataSource();
    const doRequestSpy = jest.spyOn(datasource, 'doRequest');

    const result = await datasource.metricFindQuery({ queryText: '   ' });

    expect(result).toEqual([]);
    expect(doRequestSpy).not.toHaveBeenCalled();
  });
});
