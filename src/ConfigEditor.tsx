import React from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import {
  AdvancedHttpSettings,
  Auth,
  AuthMethod,
  ConfigSection,
  ConnectionSettings,
  convertLegacyAuthProps,
} from '@grafana/plugin-ui';
import { Field, Input, SecretInput } from '@grafana/ui';

import { SplunkDataSourceOptions, SplunkSecureJsonData } from './types';

type Props = DataSourcePluginOptionsEditorProps<SplunkDataSourceOptions, SplunkSecureJsonData>;

export const ConfigEditor = ({ options, onOptionsChange }: Props) => {
  const update = (key: keyof SplunkDataSourceOptions, value: number | undefined) =>
    onOptionsChange({ ...options, jsonData: { ...options.jsonData, [key]: value } });
  const numberInput = (key: keyof SplunkDataSourceOptions, fallback: number, min: number, max: number, step = 1) => (
    <Input
      type="number"
      value={(options.jsonData[key] as number | undefined) ?? ''}
      placeholder={String(fallback)}
      min={min}
      max={max}
      step={step}
      disabled={options.readOnly}
      onChange={(event) => {
        const value = event.currentTarget.valueAsNumber;
        update(key, Number.isFinite(value) ? value : undefined);
      }}
      width={24}
    />
  );

  return (
    <>
      <ConnectionSettings
        config={options}
        onChange={onOptionsChange}
        urlPlaceholder="https://splunk.example.com:8089"
      />
      <Auth
        {...convertLegacyAuthProps({ config: options, onChange: onOptionsChange })}
        visibleMethods={[AuthMethod.NoAuth, AuthMethod.BasicAuth, AuthMethod.OAuthForward]}
      />
      <ConfigSection
        title="Splunk token"
        isCollapsible
        isInitiallyOpen={Boolean(options.secureJsonFields.basicAuthToken)}
      >
        <Field
          label="Authorization token"
          description="Include the authentication scheme, for example Splunk <token> or Bearer <token>. Leave empty to use the authentication settings above."
        >
          <SecretInput
            value={options.secureJsonData?.basicAuthToken ?? ''}
            isConfigured={Boolean(options.secureJsonFields.basicAuthToken)}
            disabled={options.readOnly}
            onChange={(event) =>
              onOptionsChange({
                ...options,
                secureJsonData: { ...options.secureJsonData, basicAuthToken: event.currentTarget.value },
              })
            }
            onReset={() =>
              onOptionsChange({
                ...options,
                secureJsonData: { ...options.secureJsonData, basicAuthToken: '' },
                secureJsonFields: { ...options.secureJsonFields, basicAuthToken: false },
              })
            }
          />
        </Field>
      </ConfigSection>
      <ConfigSection title="Splunk query limits" isCollapsible isInitiallyOpen={false}>
        <Field label="Standard search timeout (seconds)">
          {numberInput('standardSearchTimeoutSeconds', 30, 1, 3600)}
        </Field>
        <Field label="Base search timeout (seconds)">{numberInput('baseSearchTimeoutSeconds', 120, 1, 3600)}</Field>
        <Field label="Chain search timeout (seconds)">{numberInput('chainSearchTimeoutSeconds', 30, 1, 3600)}</Field>
        <Field label="Base cache TTL (minutes)">{numberInput('baseSearchCacheTtlMinutes', 5, 0.1, 60, 0.1)}</Field>
        <Field label="Maximum result rows">{numberInput('maxResultRows', 100_000, 1, 1_000_000)}</Field>
        <Field label="Maximum result pages">{numberInput('maxResultPages', 100, 1, 1000)}</Field>
        <Field label="Maximum response bytes">
          {numberInput('maxResponseBytes', 50 * 1024 * 1024, 1024, 500 * 1024 * 1024)}
        </Field>
      </ConfigSection>
      <ConfigSection title="Advanced HTTP settings" isCollapsible isInitiallyOpen={false}>
        <AdvancedHttpSettings config={options} onChange={onOptionsChange} />
      </ConfigSection>
    </>
  );
};
