import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface QueryRequestResults {
  fields: any[];
  results: any[];
  sid?: string;
  warning?: string;
  baseSearch?: BaseSearchMetadata;
}

export interface BaseSearchResult {
  sid: string;
  searchId: string;
  refId: string;
  timestamp: number;
  cacheKey: string;
  executionMs: number;
}

export interface BaseSearchMetadata {
  searchId: string;
  sid: string;
  cache: 'hit' | 'miss' | 'inflight';
  ageMs: number;
  executionMs: number;
}

export const defaultQueryRequestResults: QueryRequestResults = {
  fields: [],
  results: [],
};

export interface SplunkQuery extends DataQuery {
  queryText: string;
  searchType?: 'standard' | 'base' | 'chain';
  mode?: 'base' | 'chain'; // For backward compatibility
  baseSearchRefId?: string;
  searchId?: string; // For base searches, this will be used to identify them
  useDashboardTimeRange?: boolean;
  earliest?: string;
  latest?: string;
  returnBaseResults?: boolean;
}

export const defaultQuery: Partial<SplunkQuery> = {
  queryText: '',
  searchType: 'standard',
};

/**
 * These are options configured for each DataSource instance
 */
export interface SplunkDataSourceOptions extends DataSourceJsonData {
  endpoint?: string;
  standardSearchTimeoutSeconds?: number;
  baseSearchTimeoutSeconds?: number;
  chainSearchTimeoutSeconds?: number;
  baseSearchCacheTtlMinutes?: number;
  maxResultRows?: number;
  maxResultPages?: number;
  maxResponseBytes?: number;
  debugLogging?: boolean;
}

/**
 * Value that is used in the backend, but never sent over HTTP to the frontend
 */
export interface SplunkSecureJsonData {
  basicAuthToken?: string;
}

export type EffectiveSearchType = NonNullable<SplunkQuery['searchType']>;
