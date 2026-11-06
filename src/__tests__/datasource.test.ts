import type { DataFrame, DataQueryRequest, DataSourceInstanceSettings } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import { Observable, lastValueFrom, of, throwError } from 'rxjs';

import { DataSource } from '../datasource';
import { SplunkDataSourceOptions, SplunkQuery } from '../types';
import { SplunkVariableQuery } from '../VariableQueryEditor';

const mockBackendQuery = jest.fn();

jest.mock('../VariableQueryEditor', () => ({ VariableQueryEditor: () => null }));

jest.mock('@grafana/data', () => ({
  CustomVariableSupport: class {
    getType() {
      return 'custom';
    }
  },
  LoadingState: { Error: 'Error' },
  dateTime: (value: number) => ({ valueOf: () => value }),
}));

jest.mock('@grafana/runtime', () => ({
  DataSourceWithBackend: class {
    query(request: unknown) {
      return mockBackendQuery(request);
    }
  },
  getTemplateSrv: jest.fn(() => ({
    replace: (value: string, scopedVars: Record<string, { value?: unknown }> = {}) =>
      value.replace(/\$\{([^}]+)\}/g, (match, name) =>
        scopedVars[name]?.value === undefined ? match : String(scopedVars[name].value)
      ),
  })),
}));

const createDataSource = () =>
  new DataSource({
    id: 1,
    uid: 'splunk-test',
    type: 'essinghigh-splunk-datasource',
    name: 'Splunk',
    jsonData: {},
  } as DataSourceInstanceSettings<SplunkDataSourceOptions>);

const frame = (values: Record<string, unknown[]>): DataFrame =>
  ({
    length: Math.max(0, ...Object.values(values).map((field) => field.length)),
    fields: Object.entries(values).map(([name, values]) => ({ name, values })),
  }) as DataFrame;

beforeEach(() => {
  mockBackendQuery.mockReset();
  mockBackendQuery.mockReturnValue(of({ data: [] }));
});

describe('backend variable queries', () => {
  it('uses the custom variable editor', () => {
    const datasource = createDataSource();
    expect(datasource.variables.getType()).toBe('custom');
    expect(datasource.variables.editor).toBeDefined();
  });

  it('runs a legacy string lookup through the backend with a complete fallback request', async () => {
    const datasource = createDataSource();
    mockBackendQuery.mockReturnValue(of({ data: [frame({ host: ['web-1'] })] }));

    expect(await datasource.metricFindQuery('  index=main | fields host  ')).toEqual([{ text: 'web-1' }]);
    expect(mockBackendQuery).toHaveBeenCalledTimes(1);
    const request = mockBackendQuery.mock.calls[0][0];
    expect(request).toMatchObject({
      app: 'variable',
      requestId: expect.stringMatching(/^splunk-variable-metricFindQuery-/),
      scopedVars: {},
      intervalMs: 1000,
      targets: [
        { refId: 'metricFindQuery', queryText: 'index=main | fields host', searchType: 'standard', hide: false },
      ],
    });
    expect(request.range.to.valueOf() - request.range.from.valueOf()).toBe(60 * 60 * 1000);
  });

  it('preserves caller context but executes only the requested variable target', async () => {
    const options = {
      requestId: 'variable-request',
      dashboardUID: 'dashboard-1',
      scopedVars: { service: { text: 'web', value: 'web' } },
      range: { from: { valueOf: () => 1000 }, to: { valueOf: () => 2000 } },
      targets: [{ refId: 'unrelated', queryText: 'index=other' }],
    } as unknown as DataQueryRequest<SplunkQuery>;
    await createDataSource().metricFindQuery({ query: 'index=main', refId: 'custom', extraMetadata: true }, options);
    expect(mockBackendQuery.mock.calls[0][0]).toMatchObject({
      requestId: options.requestId,
      range: options.range,
      dashboardUID: options.dashboardUID,
      scopedVars: options.scopedVars,
      targets: [{ refId: 'custom', queryText: 'index=main', extraMetadata: true }],
    });
  });

  it('normalizes saved legacy base variables and asks the backend for their results', async () => {
    await createDataSource().metricFindQuery({ query: 'index=main', mode: 'base', searchId: 'servers', hide: true });
    expect(mockBackendQuery.mock.calls[0][0].targets[0]).toMatchObject({
      queryText: 'index=main',
      searchType: 'base',
      searchId: 'servers',
      returnBaseResults: true,
      hide: false,
    });
  });

  it.each(['', '   ', { query: '' }, { queryText: ' ' }])('skips a blank variable lookup', async (query) => {
    expect(await createDataSource().metricFindQuery(query)).toEqual([]);
    expect(mockBackendQuery).not.toHaveBeenCalled();
  });

  it('deduplicates ordinary fields in row order and retains zero and false', async () => {
    mockBackendQuery.mockReturnValue(
      of({
        data: [
          frame({ host: ['api', 'api', null, '', 0, false], source: ['logs', 'other', undefined, '', null, null] }),
        ],
      })
    );
    expect(await createDataSource().metricFindQuery('index=main')).toEqual([
      { text: 'api' },
      { text: 'logs' },
      { text: 'other' },
      { text: '0' },
      { text: 'false' },
    ]);
  });

  it('supports __text/__value pairs without turning values or other columns into labels', async () => {
    mockBackendQuery.mockReturnValue(
      of({
        data: [
          frame({
            __text: ['web', 'web', 'web', '', 'worker'],
            __value: [1, 2, 1, 3, 0],
            extra: ['a', 'b', 'c', 'd', 'e'],
          }),
        ],
      })
    );
    expect(await createDataSource().metricFindQuery('index=main')).toEqual([
      { text: 'web', value: 1 },
      { text: 'web', value: 2 },
      { text: 'worker', value: 0 },
    ]);
  });

  it('deduplicates values across returned frames', async () => {
    mockBackendQuery.mockReturnValue(of({ data: [frame({ host: ['a', 'b'] }), frame({ host: ['b', 'c'] })] }));
    expect(await createDataSource().metricFindQuery('index=main')).toEqual([
      { text: 'a' },
      { text: 'b' },
      { text: 'c' },
    ]);
  });

  it('surfaces backend query errors rather than showing a successful empty preview', async () => {
    mockBackendQuery.mockReturnValue(of({ data: [], errors: [{ message: 'Splunk returned HTTP 403' }] }));
    await expect(createDataSource().metricFindQuery('index=main')).rejects.toThrow('Splunk returned HTTP 403');
  });

  it('surfaces Grafana HTTP errors stored in the legacy error field', async () => {
    mockBackendQuery.mockReturnValue(of({ data: [], error: { message: 'Access denied' } }));
    await expect(createDataSource().metricFindQuery('index=main')).rejects.toThrow('Access denied');
  });

  it('propagates transport failures', async () => {
    mockBackendQuery.mockReturnValue(throwError(() => new Error('Network unavailable')));
    await expect(createDataSource().metricFindQuery('index=main')).rejects.toThrow('Network unavailable');
  });

  it('cancels the backend request when Grafana unsubscribes from a custom variable query', () => {
    const cancel = jest.fn();
    mockBackendQuery.mockReturnValue(new Observable(() => cancel));
    const datasource = createDataSource();
    const request = {
      targets: [{ refId: 'variable', queryText: 'index=main' }],
    } as DataQueryRequest<SplunkVariableQuery>;
    const result = datasource.variables.query(request);
    expect(mockBackendQuery).not.toHaveBeenCalled();
    const subscription = result.subscribe();
    expect(mockBackendQuery).toHaveBeenCalledTimes(1);
    subscription.unsubscribe();
    expect(cancel).toHaveBeenCalledTimes(1);
  });

  it('maps the custom variable response to Grafana variable options', async () => {
    mockBackendQuery.mockReturnValue(of({ data: [frame({ host: ['web'] })] }));
    const request = {
      targets: [{ refId: 'variable', queryText: 'index=main' }],
    } as DataQueryRequest<SplunkVariableQuery>;
    expect(await lastValueFrom(createDataSource().variables.query(request))).toEqual({ data: [{ text: 'web' }] });
  });
});

describe('template interpolation', () => {
  it('interpolates query text, fixed times and base identifiers without mutating the saved model', () => {
    const query: SplunkQuery = {
      refId: 'A',
      queryText: 'index=${index}',
      searchId: '${base}',
      baseSearchRefId: '${parent}',
      earliest: '${from}',
      latest: '${to}',
      useDashboardTimeRange: false,
    };
    const scopedVars = Object.fromEntries(
      Object.entries({ index: 'main', base: 'servers', parent: 'parent', from: '-1d', to: 'now' }).map(
        ([name, value]) => [name, { text: value, value }]
      )
    );
    expect(createDataSource().applyTemplateVariables(query, scopedVars)).toMatchObject({
      queryText: 'index=main',
      searchId: 'servers',
      baseSearchRefId: 'parent',
      earliest: '-1d',
      latest: 'now',
    });
    expect(query.queryText).toBe('index=${index}');
    expect(query.earliest).toBe('${from}');
    expect(getTemplateSrv).toHaveBeenCalled();
  });
});
