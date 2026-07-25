import React from 'react';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import {
  AdvancedHttpSettings,
  Auth,
  ConfigSection,
  ConnectionSettings,
  convertLegacyAuthProps,
} from '@grafana/plugin-ui';
import { Field, Input, Switch } from '@grafana/ui';

import { SplunkDataSourceOptions } from './types';

type Props = DataSourcePluginOptionsEditorProps<SplunkDataSourceOptions>;

export const ConfigEditor = ({ options, onOptionsChange }: Props) => {
  const update = (key: keyof SplunkDataSourceOptions, value: number | boolean) =>
    onOptionsChange({ ...options, jsonData: { ...options.jsonData, [key]: value } });
  const numberInput = (key: keyof SplunkDataSourceOptions, fallback: number, min: number, max: number) => (
    <Input
      type="number"
      value={(options.jsonData[key] as number | undefined) ?? fallback}
      min={min}
      max={max}
      onChange={(event) => update(key, Number(event.currentTarget.value))}
      width={24}
    />
  );

  return (
    <>
      {ConnectionSettings({ config: options, onChange: onOptionsChange }) as React.ReactElement}
      <Auth {...convertLegacyAuthProps({ config: options, onChange: onOptionsChange })} />
      <ConfigSection title="Splunk query limits" isCollapsible isInitiallyOpen={false}>
        <Field label="Standard search timeout (seconds)">
          {numberInput('standardSearchTimeoutSeconds', 30, 1, 3600)}
        </Field>
        <Field label="Base search timeout (seconds)">{numberInput('baseSearchTimeoutSeconds', 120, 1, 3600)}</Field>
        <Field label="Chain search timeout (seconds)">{numberInput('chainSearchTimeoutSeconds', 30, 1, 3600)}</Field>
        <Field label="Base cache TTL (minutes)">{numberInput('baseSearchCacheTtlMinutes', 5, 0.1, 60)}</Field>
        <Field label="Maximum result rows">{numberInput('maxResultRows', 100_000, 1, 1_000_000)}</Field>
        <Field label="Maximum result pages">{numberInput('maxResultPages', 100, 1, 1000)}</Field>
        <Field label="Maximum response bytes">
          {numberInput('maxResponseBytes', 50 * 1024 * 1024, 1024, 500 * 1024 * 1024)}
        </Field>
        <Field label="Debug logging">
          <Switch
            value={options.jsonData.debugLogging === true}
            onChange={(event) => update('debugLogging', event.currentTarget.checked)}
          />
        </Field>
      </ConfigSection>
      <ConfigSection title="Advanced HTTP settings" isCollapsible isInitiallyOpen={false}>
        {AdvancedHttpSettings({ config: options, onChange: onOptionsChange }) as React.ReactElement}
      </ConfigSection>
    </>
  );
};
