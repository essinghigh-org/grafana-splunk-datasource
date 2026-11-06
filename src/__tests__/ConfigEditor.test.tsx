import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';

import { ConfigEditor } from '../ConfigEditor';

jest.mock('@grafana/plugin-ui', () => ({
  ConnectionSettings: () => null,
  AdvancedHttpSettings: () => null,
  Auth: () => null,
  AuthMethod: { NoAuth: 'NoAuth', BasicAuth: 'BasicAuth', OAuthForward: 'OAuthForward' },
  ConfigSection: ({ title, children }: { title: string; children: React.ReactNode }) => (
    <section aria-label={title}>{children}</section>
  ),
  convertLegacyAuthProps: () => ({}),
}));

jest.mock('@grafana/ui', () => ({
  Field: ({ label, children }: { label: string; children: React.ReactNode }) => (
    <label>
      {label}
      {children}
    </label>
  ),
  Input: ({ width: _width, ...props }: React.InputHTMLAttributes<HTMLInputElement> & { width?: number }) => (
    <input {...props} />
  ),
  SecretInput: ({
    isConfigured,
    onReset,
    disabled,
    ...props
  }: React.InputHTMLAttributes<HTMLInputElement> & {
    isConfigured: boolean;
    onReset: () => void;
  }) => (
    <>
      <input {...props} disabled={disabled} type="password" />
      {isConfigured && (
        <button onClick={onReset} disabled={disabled}>
          Reset authorization token
        </button>
      )}
    </>
  ),
}));

type ConfigOptions = React.ComponentProps<typeof ConfigEditor>['options'];

const createOptions = () => ({
  id: 1,
  uid: 'splunk-test',
  type: 'essinghigh-splunk-datasource',
  name: 'Splunk',
  access: 'proxy',
  url: 'https://splunk.example.com:8089',
  readOnly: false,
  jsonData: { standardSearchTimeoutSeconds: 45, provisionedOption: 'keep' },
  secureJsonData: { httpHeaderValue1: 'keep-secret' },
  secureJsonFields: { basicAuthToken: true, tlsCACert: true },
});

it('resets a legacy token without deleting other provisioned settings or secrets', () => {
  const options = createOptions();
  const onOptionsChange = jest.fn();
  render(<ConfigEditor options={options as unknown as ConfigOptions} onOptionsChange={onOptionsChange} />);

  fireEvent.click(screen.getByRole('button', { name: 'Reset authorization token' }));
  expect(onOptionsChange).toHaveBeenCalledWith({
    ...options,
    secureJsonData: { ...options.secureJsonData, basicAuthToken: '' },
    secureJsonFields: { ...options.secureJsonFields, basicAuthToken: false },
  });
  expect(options.secureJsonFields.basicAuthToken).toBe(true);
});

it('stores edited tokens only in secureJsonData', () => {
  const options = createOptions();
  const onOptionsChange = jest.fn();
  render(<ConfigEditor options={options as unknown as ConfigOptions} onOptionsChange={onOptionsChange} />);

  fireEvent.change(screen.getByLabelText('Authorization token'), { target: { value: 'Splunk test-token' } });
  const updated = onOptionsChange.mock.calls[0][0];
  expect(updated.secureJsonData).toEqual({ httpHeaderValue1: 'keep-secret', basicAuthToken: 'Splunk test-token' });
  expect(updated.jsonData).toEqual(options.jsonData);
});

it('disables token editing, resetting and query limits for a provisioned read-only data source', () => {
  const options = { ...createOptions(), readOnly: true };
  render(<ConfigEditor options={options as unknown as ConfigOptions} onOptionsChange={jest.fn()} />);
  expect((screen.getByLabelText('Authorization token') as HTMLInputElement).disabled).toBe(true);
  expect((screen.getByRole('button', { name: 'Reset authorization token' }) as HTMLButtonElement).disabled).toBe(true);
  for (const input of screen.getAllByRole('spinbutton')) {
    expect((input as HTMLInputElement).disabled).toBe(true);
  }
});

it('clearing a query limit restores the default without saving a zero value', () => {
  const options = createOptions();
  const onOptionsChange = jest.fn();
  render(<ConfigEditor options={options as unknown as ConfigOptions} onOptionsChange={onOptionsChange} />);
  fireEvent.change(screen.getByLabelText('Standard search timeout (seconds)'), { target: { value: '' } });
  expect(onOptionsChange.mock.calls[0][0].jsonData).toEqual({
    ...options.jsonData,
    standardSearchTimeoutSeconds: undefined,
  });
});
