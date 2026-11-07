import React, { useMemo } from 'react';
import { css } from '@emotion/css';
import type { GrafanaTheme2, QueryEditorProps } from '@grafana/data';
import {
  Badge,
  CodeEditor,
  Combobox,
  type ComboboxOption,
  Field,
  Input,
  Switch,
  Tooltip,
  useStyles2,
  useTheme2,
} from '@grafana/ui';

import type { DataSource } from './datasource';
import { defaultQuery, type SplunkDataSourceOptions, type SplunkQuery } from './types';
import { registerSplunkLanguage, SPL_LANGUAGE_ID } from './language/splMonaco';

type Props = QueryEditorProps<DataSource, SplunkQuery, SplunkDataSourceOptions>;
type SearchType = NonNullable<SplunkQuery['searchType']>;

const MIN_LINES = 8;

const searchTypeOptions: Array<ComboboxOption<SearchType>> = [
  {
    label: 'Standard',
    value: 'standard',
    description: 'Standalone search',
    icon: 'search',
  },
  {
    label: 'Base',
    value: 'base',
    description: 'Reusable search for other queries',
    icon: 'cube',
  },
  {
    label: 'Chain',
    value: 'chain',
    description: 'Builds on a base search',
    icon: 'link',
  },
];

const getStyles = (theme: GrafanaTheme2) => ({
  container: css`
    display: flex;
    flex-direction: column;
    gap: ${theme.spacing(2)};
  `,
  headerRow: css`
    display: flex;
    align-items: center;
    gap: ${theme.spacing(1.5)};
    margin-bottom: ${theme.spacing(1)};
  `,
  searchTypeContainer: css`
    display: flex;
    align-items: center;
    gap: ${theme.spacing(1)};
  `,
  queryContainer: css`
    display: flex;
    flex-direction: column;
    gap: ${theme.spacing(1)};
  `,
  queryLabel: css`
    display: flex;
    align-items: center;
    gap: ${theme.spacing(1)};
  `,
  conditionalField: css`
    background: ${theme.colors.background.secondary};
    border: 1px solid ${theme.colors.border.weak};
    border-radius: ${theme.shape.radius.default};
    padding: ${theme.spacing(1.5)};
    margin-bottom: ${theme.spacing(1)};
  `,
  queryField: css`
    width: 100%;
    border: 1px solid ${theme.colors.border.medium};
    border-radius: ${theme.shape.radius.default};
    overflow: hidden;
    position: relative;
  `,
  placeholder: css`
    position: absolute;
    top: ${theme.spacing(0.5)};
    left: ${theme.spacing(5.5)};
    color: ${theme.colors.text.secondary};
    font-family: ${theme.typography.fontFamilyMonospace};
    font-size: ${theme.typography.pxToRem(theme.typography.fontSize)};
    line-height: ${theme.typography.code.lineHeight};
    pointer-events: none;
    z-index: 1;
  `,
});

export const QueryEditor = ({ onChange, onRunQuery, query: rawQuery }: Props) => {
  const theme = useTheme2();
  const styles = useStyles2(getStyles);
  const query = useMemo(
    () =>
      ({
        ...defaultQuery,
        ...rawQuery,
        searchType: rawQuery.searchType ?? rawQuery.mode ?? defaultQuery.searchType,
      }) as SplunkQuery,
    [rawQuery]
  );
  const { queryText = '', searchType = 'standard', searchId, baseSearchRefId } = query;
  const isChainSearch = searchType === 'chain';
  const isBaseSearch = searchType === 'base';
  const currentSearchType = searchTypeOptions.find((option) => option.value === searchType) ?? searchTypeOptions[0];
  const lineHeight = Math.round(theme.typography.fontSize * theme.typography.code.lineHeight);
  const lineCount = Math.max(queryText.split('\n').length, MIN_LINES);
  const editorHeight = `calc(${lineCount * lineHeight}px + ${theme.spacing(1)})`;
  const useDashboardTimeRange = query.useDashboardTimeRange !== false;

  const onQueryTextChange = (value: string) => {
    onChange({ ...query, queryText: value });
  };

  const onSearchTypeChange = (selection: ComboboxOption<SearchType> | null) => {
    if (!selection) {
      return;
    }

    const nextQuery: SplunkQuery = {
      ...query,
      searchType: selection.value,
    };
    delete nextQuery.mode;

    if (selection.value === 'standard' || selection.value === 'base') {
      delete nextQuery.baseSearchRefId;
    }

    if (selection.value === 'standard' || selection.value === 'chain') {
      delete nextQuery.searchId;
    }

    onChange(nextQuery);
  };

  return (
    <div className={styles.container}>
      <div className={styles.headerRow}>
        <div className={styles.searchTypeContainer}>
          <Field label="Search type" noMargin>
            <Combobox<SearchType>
              options={searchTypeOptions}
              value={currentSearchType.value}
              onChange={onSearchTypeChange}
              width={24}
              placeholder="Search type"
            />
          </Field>
          <Tooltip content={currentSearchType.description ?? 'Select search type'}>
            <Badge text={currentSearchType.label ?? 'Standard'} color="blue" />
          </Tooltip>
        </div>
      </div>

      {isBaseSearch && (
        <div className={styles.conditionalField}>
          <Field
            label="Search ID"
            description="Identifier for this base search (used by chain searches)"
          >
            <Input
              value={searchId ?? ''}
              onChange={(event) => onChange({ ...query, searchId: event.currentTarget.value })}
              placeholder="my-base-search"
              width={40}
            />
          </Field>
          <Field label="Use dashboard time range">
            <Switch
              value={useDashboardTimeRange}
              onChange={(event) => onChange({ ...query, useDashboardTimeRange: event.currentTarget.checked })}
            />
          </Field>
          {!useDashboardTimeRange && (
            <>
              <Field label="Earliest">
                <Input
                  value={query.earliest ?? '-30d@d'}
                  onChange={(event) => onChange({ ...query, earliest: event.currentTarget.value })}
                  width={24}
                />
              </Field>
              <Field label="Latest">
                <Input
                  value={query.latest ?? 'now'}
                  onChange={(event) => onChange({ ...query, latest: event.currentTarget.value })}
                  width={24}
                />
              </Field>
            </>
          )}
          <Field label="Return base results" description="Normally off; chain searches only need the base SID.">
            <Switch
              value={query.returnBaseResults === true}
              onChange={(event) => onChange({ ...query, returnBaseResults: event.currentTarget.checked })}
            />
          </Field>
        </div>
      )}

      {isChainSearch && (
        <div className={styles.conditionalField}>
          <Field
            label="Base Search Reference"
            description="Search ID of a base search on this dashboard"
            invalid={!baseSearchRefId?.trim()}
            error={!baseSearchRefId?.trim() ? 'Enter a base search ID.' : undefined}
          >
            <Input
              value={baseSearchRefId ?? ''}
              onChange={(event) => onChange({ ...query, baseSearchRefId: event.currentTarget.value })}
              placeholder="my-base-search"
              width={40}
              invalid={!baseSearchRefId?.trim()}
            />
          </Field>
        </div>
      )}

      <div className={styles.queryContainer}>
        <Field
          label={
            <div className={styles.queryLabel}>
              <span>Splunk Query</span>
              {isChainSearch && <Badge text="Chain" color="orange" />}
              {isBaseSearch && <Badge text="Base" color="green" />}
            </div>
          }
          description={
            isChainSearch
              ? "Commands to apply to the base search (e.g., 'stats count by host')"
              : isBaseSearch
                ? 'Search query that provides results for chain searches'
                : 'Your Splunk search query'
          }
        >
          <div className={styles.queryField}>
            {queryText.trim() === '' && (
              <div className={styles.placeholder} aria-hidden="true">
                {isChainSearch ? '| stats count by host | head 10' : 'index=main sourcetype=access_* | head 100'}
              </div>
            )}
            <CodeEditor
              value={queryText}
              language={SPL_LANGUAGE_ID}
              height={editorHeight}
              onChange={onQueryTextChange}
              onSave={onRunQuery}
              showLineNumbers
              showMiniMap={false}
              onBeforeEditorMount={registerSplunkLanguage}
              monacoOptions={{
                ariaLabel: 'Splunk Query',
                fontSize: theme.typography.fontSize,
                lineHeight,
                scrollBeyondLastLine: false,
                wordWrap: 'on',
                wordWrapColumn: 80,
                wrappingIndent: 'indent',
                minimap: { enabled: false },
                folding: false,
                renderLineHighlight: 'none',
                automaticLayout: true,
                overviewRulerLanes: 0,
                hideCursorInOverviewRuler: true,
                scrollbar: {
                  vertical: 'auto',
                  horizontal: 'auto',
                  useShadows: false,
                },
              }}
            />
          </div>
        </Field>
      </div>
    </div>
  );
};
