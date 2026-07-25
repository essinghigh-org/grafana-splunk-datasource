import { DataQueryRequest } from '@grafana/data';
import { getTemplateSrv } from '@grafana/runtime';
import { SplunkQuery, EffectiveSearchType } from '../types';

export const isSearchType = (value: unknown): value is EffectiveSearchType =>
  value === 'standard' || value === 'base' || value === 'chain';

export const isLegacyMode = (value: unknown): value is NonNullable<SplunkQuery['mode']> =>
  value === 'base' || value === 'chain';

export const resolveSearchType = (searchType: unknown, mode: unknown): EffectiveSearchType => {
  if (isSearchType(searchType)) {
    return searchType;
  }

  if (isLegacyMode(mode)) {
    return mode;
  }

  return 'standard';
};

export const getErrorMessage = (error: unknown): string => {
  if (error instanceof Error && error.message) {
    return error.message;
  }

  if (typeof error === 'string') {
    return error;
  }

  try {
    return JSON.stringify(error);
  } catch {
    return String(error);
  }
};

export const mapWithConcurrency = async <T, R>(
  items: T[],
  limit: number,
  mapper: (item: T, index: number) => Promise<R>
): Promise<R[]> => {
  if (items.length === 0) {
    return [];
  }

  const boundedLimit = Math.max(1, Math.min(limit, items.length));
  const results = new Array<R>(items.length);
  let nextIndex = 0;

  const worker = async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= items.length) {
        return;
      }

      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  };

  await Promise.all(Array.from({ length: boundedLimit }, () => worker()));
  return results;
};

export const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const clampSetting = (value: number | undefined, fallback: number, minimum: number, maximum: number) =>
  Number.isFinite(value) ? Math.min(maximum, Math.max(minimum, value!)) : fallback;

export const getDashboardNamespace = (options: DataQueryRequest<SplunkQuery>) =>
  options.dashboardUID ?? options.dashboardTitle ?? `${options.app}:${options.panelId ?? 'unknown-panel'}`;

export function generateCacheKey(query: SplunkQuery, options: DataQueryRequest<SplunkQuery>): string {
  const expandedQuery = getTemplateSrv()
    .replace(query.queryText || '', options.scopedVars)
    .trim();
  const timeRange =
    query.useDashboardTimeRange === false
      ? [query.earliest || '-30d@d', query.latest || 'now']
      : [
          Math.floor(options.range.from.valueOf() / 1000).toString(),
          Math.floor(options.range.to.valueOf() / 1000).toString(),
        ];

  return [getDashboardNamespace(options), query.searchId || query.refId || '', expandedQuery, ...timeRange].join('|');
}
