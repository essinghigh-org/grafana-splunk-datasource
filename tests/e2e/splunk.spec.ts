import { expect, test } from '@grafana/plugin-e2e';

const provisioningFile = 'splunk.yml';

test('configured datasource connects using its secure HTTP header', async ({
  gotoDataSourceConfigPage,
  readProvisionedDataSource,
  page,
}) => {
  const datasource = await readProvisionedDataSource({ fileName: provisioningFile, name: 'Splunk E2E' });
  const configPage = await gotoDataSourceConfigPage(datasource.uid);

  await expect(page.getByRole('textbox', { name: 'Data source connection URL' })).toBeVisible();
  await expect(configPage.saveAndTest()).toBeOK();
  await expect(configPage).toHaveAlert('success', { hasText: 'Data source is working' });
});

test('invalid secure HTTP credentials fail the backend health check', async ({
  gotoDataSourceConfigPage,
  readProvisionedDataSource,
}) => {
  const datasource = await readProvisionedDataSource({
    fileName: provisioningFile,
    name: 'Splunk E2E invalid credentials',
  });
  const configPage = await gotoDataSourceConfigPage(datasource.uid);

  await expect(configPage.saveAndTest()).not.toBeOK();
  await expect(configPage).toHaveAlert('error', { hasText: /401/ });
});

test('authorization tokens are stored securely and can be reset', async ({
  createDataSourceConfigPage,
  readProvisionedDataSource,
  page,
  request,
}) => {
  const datasource = await readProvisionedDataSource({ fileName: provisioningFile, name: 'Splunk E2E' });
  const configPage = await createDataSourceConfigPage({ type: datasource.type });
  await page
    .getByRole('textbox', { name: 'Data source connection URL' })
    .fill(process.env.MOCK_SPLUNK_URL ?? 'http://127.0.0.1:18089');
  await page.getByRole('button', { name: 'Expand section Splunk token' }).click();
  await page.getByLabel('Authorization token').fill('Bearer grafana-e2e-token');
  await expect(configPage.saveAndTest()).toBeOK();

  const settingsResponse = await request.get(`/api/datasources/uid/${configPage.datasource.uid}`);
  const settings = await settingsResponse.json();
  expect(settings.secureJsonFields.basicAuthToken).toBe(true);
  expect(JSON.stringify(settings)).not.toContain('grafana-e2e-token');

  await page.reload();
  await page.getByRole('button', { name: 'Reset', exact: true }).click();
  await expect(configPage.saveAndTest()).not.toBeOK();
  await expect(configPage).toHaveAlert('error', { hasText: /401/ });
});

test('query editor sends a query through the backend and renders typed results', async ({
  panelEditPage,
  readProvisionedDataSource,
  page,
}) => {
  const datasource = await readProvisionedDataSource({ fileName: provisioningFile, name: 'Splunk E2E' });
  await panelEditPage.datasource.set(datasource.name);
  await panelEditPage.setVisualization('Table');

  const editor = panelEditPage.getQueryEditorRow('A').getByRole('textbox', { name: 'Splunk Query' });
  await expect(editor).toBeVisible();
  await editor.fill('| makeresults | eval e2e_metrics=1');
  await page.keyboard.press('Tab');

  await expect(panelEditPage.refreshPanel()).toBeOK();
  await expect(panelEditPage.panel.fieldNames).toContainText(['host', 'count']);
  await expect(panelEditPage.panel.data).toContainText(['api-1', '42', 'api-2', '7']);
});

test('variable query previews labels returned through the backend', async ({
  variableEditPage,
  readProvisionedDataSource,
  page,
}) => {
  const datasource = await readProvisionedDataSource({ fileName: provisioningFile, name: 'Splunk E2E' });
  await variableEditPage.setVariableType('Query');
  await variableEditPage.datasource.set(datasource.name);

  await page.getByRole('textbox', { name: 'Splunk variable query' }).fill('| makeresults | eval e2e_variables=1');
  const queryRequest = variableEditPage.waitForQueryDataRequest((request) =>
    (request.postData() ?? '').includes('e2e_variables')
  );
  await variableEditPage.runQuery();
  const request = await queryRequest;
  expect(request.postDataJSON().queries[0].queryType).toBe('variable');

  await expect(variableEditPage).toDisplayPreviews(['API server 1', 'API server 2']);
});

test('backend results can be evaluated by Grafana alerting', async ({
  alertRuleEditPage,
  readProvisionedDataSource,
  page,
}) => {
  const datasource = await readProvisionedDataSource({ fileName: provisioningFile, name: 'Splunk E2E' });
  const query = await alertRuleEditPage.getQueryRow('A');
  await query.datasource.set(datasource.name);
  await query.locator.getByRole('textbox', { name: 'Splunk Query' }).fill('| makeresults | eval e2e_metrics=1');
  await page.keyboard.press('Tab');

  const response = await alertRuleEditPage.evaluate();
  await expect(Promise.resolve(response)).toBeOK();
  const evaluation = (await response.json()) as {
    results: Record<string, { status: number; error?: string; frames?: unknown[] }>;
  };
  expect(evaluation.results.A.frames).toBeDefined();
  for (const result of Object.values(evaluation.results)) {
    expect(result.status).toBeGreaterThanOrEqual(200);
    expect(result.status).toBeLessThan(300);
    expect(result.error).toBeFalsy();
  }
});
