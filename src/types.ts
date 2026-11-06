import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

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
}

/**
 * Secrets are encrypted by Grafana and are not returned to the browser after saving.
 */
export interface SplunkSecureJsonData {
  basicAuthToken?: string;
}
