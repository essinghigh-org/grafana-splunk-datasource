import React, { useMemo } from 'react';
import { css } from '@emotion/css';
import { QueryEditorProps } from '@grafana/data';
import { Badge, CodeEditor, Combobox, ComboboxOption, Field, Input, Switch, Tooltip } from '@grafana/ui';

import { DataSource } from './datasource';
import { defaultQuery, SplunkDataSourceOptions, SplunkQuery } from './types';
import { registerSplunkLanguage, SPL_LANGUAGE_ID } from './language/splMonaco';

type Props = QueryEditorProps<DataSource, SplunkQuery, SplunkDataSourceOptions>;
type SearchType = NonNullable<SplunkQuery['searchType']>;

const MIN_LINES = 8;
const LINE_HEIGHT = 18;
const EDITOR_VERTICAL_PADDING = 8;

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

const styles = {
  container: css`
    display: flex;
    flex-direction: column;
    gap: 16px;
    margin-left: -24px;
  `,
  headerRow: css`
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 8px;
  `,
  searchTypeContainer: css`
    display: flex;
    align-items: center;
    gap: 8px;
  `,
  badge: css`
    font-size: 11px;
    font-weight: 500;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  `,
  queryContainer: css`
    display: flex;
    flex-direction: column;
    gap: 8px;
  `,
  conditionalField: css`
    background: rgba(204, 204, 220, 0.07);
    border: 1px solid rgba(204, 204, 220, 0.15);
    border-radius: 4px;
    padding: 12px;
    margin-bottom: 8px;
  `,
  queryField: css`
    width: 100%;
    border: 1px solid rgba(204, 204, 220, 0.25);
    border-radius: 6px;
    overflow: hidden;
    position: relative;

    .monaco-editor .scroll-decoration {
      box-shadow: none !important;
    }
  `,
  placeholder: css`
    position: absolute;
    top: 5px;
    left: 45px;
    color: rgba(255, 255, 255, 0.4);
    font-family: Monaco, Menlo, 'Ubuntu Mono', monospace;
    font-size: 13px;
    line-height: 18px;
    pointer-events: none;
    z-index: 1;
  `,
};

const getEditorHeight = (queryText: string) => {
  const lineCount = queryText.length > 0 ? queryText.split('\n').length : 1;
  return Math.max(lineCount, MIN_LINES) * LINE_HEIGHT + EDITOR_VERTICAL_PADDING;
};

export const QueryEditor = ({ onChange, query: rawQuery }: Props) => {
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
  const editorHeight = getEditorHeight(queryText);
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
          <Combobox<SearchType>
            options={searchTypeOptions}
            value={currentSearchType.value}
            onChange={onSearchTypeChange}
            width={24}
            placeholder="Search type"
          />
          <Tooltip content={currentSearchType.description ?? 'Select search type'}>
            <Badge text={currentSearchType.label ?? 'Standard'} color="blue" className={styles.badge} />
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
            invalid={!baseSearchRefId}
            error={!baseSearchRefId ? 'Enter a base search ID.' : undefined}
          >
            <Input
              value={baseSearchRefId ?? ''}
              onChange={(event) => onChange({ ...query, baseSearchRefId: event.currentTarget.value })}
              placeholder="my-base-search"
              width={40}
              invalid={!baseSearchRefId}
            />
          </Field>
        </div>
      )}

      <div className={styles.queryContainer}>
        <Field
          label={
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <span>Splunk Query</span>
              {isChainSearch && <Badge text="Chain" color="orange" className={styles.badge} />}
              {isBaseSearch && <Badge text="Base" color="green" className={styles.badge} />}
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
              <div className={styles.placeholder}>
                {isChainSearch ? '| stats count by host | head 10' : 'index=main sourcetype=access_* | head 100'}
              </div>
            )}
            <CodeEditor
              value={queryText}
              language={SPL_LANGUAGE_ID}
              height={editorHeight}
              onChange={onQueryTextChange}
              showLineNumbers
              showMiniMap={false}
              onBeforeEditorMount={registerSplunkLanguage}
              monacoOptions={{
                fontSize: 13,
                lineHeight: LINE_HEIGHT,
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
