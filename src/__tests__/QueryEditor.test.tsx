import React from 'react';
import { render, screen } from '@testing-library/react';

import { QueryEditor } from '../QueryEditor';
import type { DataSource } from '../datasource';

// Grafana 13.2 has the legacy typography variants and no typography.sm.
const mockTheme = {
  spacing: (value: number) => `${value * 8}px`,
  colors: { background: { secondary: '#fff' }, border: { weak: '#aaa', medium: '#888' }, text: { secondary: '#555' } },
  shape: { radius: { default: '6px' } },
  typography: {
    fontSize: 14,
    fontFamilyMonospace: 'monospace',
    pxToRem: (value: number) => `${value / 16}rem`,
    code: { lineHeight: 16 / 14 },
  },
};

jest.mock('@grafana/ui', () => ({
  useTheme2: () => mockTheme,
  useStyles2: (factory: (theme: unknown) => unknown) => factory(mockTheme),
  Badge: ({ text }: { text: string }) => <span>{text}</span>,
  Field: ({ label, children }: { label: React.ReactNode; children: React.ReactNode }) => (
    <div>
      {label}
      {children}
    </div>
  ),
  Tooltip: ({ children }: { children: React.ReactNode }) => children,
  Combobox: ({ value }: { value: string }) => (
    <select aria-label="Search type" defaultValue={value}>
      <option value={value}>{value}</option>
    </select>
  ),
  Input: () => <input />,
  Switch: () => <input type="checkbox" />,
  CodeEditor: ({ value, monacoOptions }: { value: string; monacoOptions: { ariaLabel: string } }) => (
    <textarea aria-label={monacoOptions.ariaLabel} defaultValue={value} />
  ),
}));

it('renders with the minimum supported Grafana theme without 13.3 typography variants', () => {
  render(
    <QueryEditor
      datasource={{} as DataSource}
      query={{ refId: 'A', queryText: 'index=main', searchType: 'base' }}
      onChange={jest.fn()}
      onRunQuery={jest.fn()}
    />
  );
  expect((screen.getByRole('textbox', { name: 'Splunk Query' }) as HTMLTextAreaElement).value).toBe('index=main');
  expect(screen.getByRole('combobox', { name: 'Search type' })).toBeDefined();
});
