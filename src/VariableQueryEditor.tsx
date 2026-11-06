import React from 'react';
import type { QueryEditorProps } from '@grafana/data';
import type { DataQuery } from '@grafana/schema';
import { Field, TextArea } from '@grafana/ui';

import type { DataSource } from './datasource';
import type { SplunkDataSourceOptions, SplunkQuery } from './types';

export type SplunkVariableQuery = DataQuery &
  Partial<SplunkQuery> & {
    query?: string;
    [key: string]: unknown;
  };

type VariableQueryProps = QueryEditorProps<DataSource, SplunkQuery, SplunkDataSourceOptions, SplunkVariableQuery>;

const normalizeVariableQuery = (query: SplunkVariableQuery): SplunkVariableQuery => {
  const queryText =
    typeof query.queryText === 'string' ? query.queryText : typeof query.query === 'string' ? query.query : '';

  return {
    ...query,
    queryText,
    query: queryText,
  };
};

export const VariableQueryEditor = ({ onChange, onRunQuery, query }: VariableQueryProps) => {
  const normalizedQuery = normalizeVariableQuery(query);

  const saveQuery = () => {
    onRunQuery();
  };

  const handleChange = (event: React.ChangeEvent<HTMLTextAreaElement>) => {
    const queryText = event.currentTarget.value;
    onChange({
      ...normalizedQuery,
      queryText,
      query: queryText,
    });
  };

  return (
    <Field
      label="Splunk variable query"
      description="Return a field of values, or __text and __value columns for labels and values."
    >
      <TextArea
        aria-label="Splunk variable query"
        name="queryText"
        onBlur={saveQuery}
        onChange={handleChange}
        value={normalizedQuery.queryText ?? ''}
      />
    </Field>
  );
};
