import React from 'react';
import { Field, Input, Switch } from '@grafana/ui';
import { SplunkQuery } from '../types';

interface Props {
  query: SplunkQuery;
  onChange: (query: SplunkQuery) => void;
  styles: Record<string, string>;
}

export const QueryEditorConfig = ({ query, onChange, styles }: Props) => {
  const isBaseSearch = query.searchType === 'base';
  const isChainSearch = query.searchType === 'chain';
  const useDashboardTimeRange = query.useDashboardTimeRange !== false;

  if (isBaseSearch) {
    return (
      <div className={styles.conditionalField}>
        <Field label="Search ID" description="Identifier for this base search (used by chain searches)">
          <Input
            value={query.searchId ?? ''}
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
    );
  }

  if (isChainSearch) {
    return (
      <div className={styles.conditionalField}>
        <Field
          label="Base Search Reference"
          description="Search ID of a base search on this dashboard"
          invalid={!query.baseSearchRefId}
          error={!query.baseSearchRefId ? 'Enter a base search ID.' : undefined}
        >
          <Input
            value={query.baseSearchRefId ?? ''}
            onChange={(event) => onChange({ ...query, baseSearchRefId: event.currentTarget.value })}
            placeholder="my-base-search"
            width={40}
            invalid={!query.baseSearchRefId}
          />
        </Field>
      </div>
    );
  }

  return null;
};
