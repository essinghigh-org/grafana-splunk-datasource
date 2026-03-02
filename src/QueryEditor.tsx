import defaults from 'lodash/defaults';

import React, { PureComponent } from 'react';
import { CodeEditor, Combobox, Field, Input, Badge, Tooltip } from '@grafana/ui';
import { QueryEditorProps, SelectableValue } from '@grafana/data';
import { css } from '@emotion/css';
import { DataSource } from './datasource';
import { defaultQuery, SplunkDataSourceOptions, SplunkQuery } from './types';
import { splLanguage } from './language/splMonarch';

type Props = QueryEditorProps<DataSource, SplunkQuery, SplunkDataSourceOptions>;

interface State {
  editorHeight: number;
}

interface MonacoEditor {
  getDomNode: () => HTMLElement | null;
}

const searchTypeOptions: Array<SelectableValue<string>> = [
  { 
    label: 'Standard', 
    value: 'standard', 
    description: 'Standalone search',
    icon: 'search'
  },
  { 
    label: 'Base', 
    value: 'base', 
    description: 'Reusable search for other queries',
    icon: 'cube'
  },
  { 
    label: 'Chain', 
    value: 'chain', 
    description: 'Builds on a base search',
    icon: 'link'
  }
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
    
    .monaco-editor .scrollbar {
      display: none !important;
    }
    
    .monaco-editor .overflow-guard {
      overflow: hidden !important;
    }
    
    .monaco-editor .monaco-scrollable-element {
      overflow: hidden !important;
    }
    
    [data-testid="data-testid ReactMonacoEditor editorLazy"] {
      overflow: hidden !important;
    }
  `,
  placeholder: css`
    position: absolute;
    top: 5px;
    left: 45px;
    color: rgba(255, 255, 255, 0.4);
    font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', monospace;
    font-size: 13px;
    line-height: 18px;
    pointer-events: none;
    z-index: 1;
  `
};

export class QueryEditor extends PureComponent<Props, State> {
  private editorInstance: MonacoEditor | null = null;
  private heightCheckInterval: NodeJS.Timeout | null = null;
  private readonly MIN_LINES = 8;
  private readonly LINE_HEIGHT = 18;

  constructor(props: Props) {
    super(props);
    this.state = {
      editorHeight: this.MIN_LINES * this.LINE_HEIGHT,
    };
  }

  componentWillUnmount() {
    if (this.heightCheckInterval) {
      clearInterval(this.heightCheckInterval);
    }
  }

  private startHeightPolling = () => {
    if (this.heightCheckInterval) {
      clearInterval(this.heightCheckInterval);
    }
    this.heightCheckInterval = setInterval(() => {
      this.updateEditorHeight({ preservePosition: true });
    }, 500);
  };

  private updateEditorHeight = (options: { isBottomExpansion?: boolean, preservePosition?: boolean } = {}) => {
    if (!this.editorInstance) {
      return;
    }

    const domNode = this.editorInstance.getDomNode();
    if (!domNode) {
      return;
    }

    const model = (this.editorInstance as any).getModel();
    if (!model) {
      return;
    }

    const currentScrollTop = (this.editorInstance as any).getScrollTop();
    const lineCount = model.getLineCount();
    const requiredLines = Math.max(lineCount, this.MIN_LINES);
    let newHeight;
    const contentHeight = (this.editorInstance as any).getContentHeight?.();
    if (contentHeight && contentHeight > 0) {
      newHeight = Math.max(contentHeight, this.MIN_LINES * this.LINE_HEIGHT);
    } else {
      newHeight = requiredLines * this.LINE_HEIGHT;
    }

    const editorContainer = domNode.closest('[data-testid="data-testid ReactMonacoEditor editorLazy"]') as HTMLElement;
    if (editorContainer) {
      const oldHeight = parseInt(editorContainer.style.height || '0', 10);
      const heightDiff = newHeight - oldHeight;
      editorContainer.style.height = `${newHeight}px`;
      requestAnimationFrame(() => {
        if (options.isBottomExpansion && heightDiff > 0) {
          (this.editorInstance as any).setScrollTop(Math.max(0, currentScrollTop));
        } else if (options.preservePosition) {
          (this.editorInstance as any).setScrollTop(currentScrollTop);
        } else {
          if (currentScrollTop < 10 && newHeight <= this.MIN_LINES * this.LINE_HEIGHT * 1.5) {
            (this.editorInstance as any).setScrollTop(0);
          } else {
            (this.editorInstance as any).setScrollTop(currentScrollTop);
          }
        }
      });
    }

    if (Math.abs(newHeight - this.state.editorHeight) > 5) {
      this.setState({ editorHeight: newHeight });
    }
  };
  onQueryTextChange = (value: string) => {
    const { onChange, query } = this.props;
    onChange({ ...query, queryText: value });
    setTimeout(() => {
      this.updateEditorHeight({ preservePosition: true });
    }, 10);
  };

  onSearchTypeChange = (selection: SelectableValue<string>) => {
    const { onChange, query } = this.props;
    const newQuery = { 
      ...query, 
      searchType: selection.value as 'standard' | 'base' | 'chain'
    };
    if (selection.value === 'standard' || selection.value === 'base') {
      delete newQuery.baseSearchRefId;
    }
    if (selection.value === 'standard' || selection.value === 'chain') {
      delete newQuery.searchId;
    }
    onChange(newQuery);
  };

  onSearchIdChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { onChange, query } = this.props;
    onChange({ ...query, searchId: event.target.value });
  };

  onBaseSearchRefIdChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { onChange, query } = this.props;
    onChange({ ...query, baseSearchRefId: event.target.value });
  };

  render() {
    const query = defaults(this.props.query, defaultQuery);
    const { queryText, searchType, searchId, baseSearchRefId } = query;
    const isChainSearch = searchType === 'chain';
    const isBaseSearch = searchType === 'base';
    const currentSearchType = searchTypeOptions.find(option => option.value === (searchType || 'standard'));

    return (
      <div className={styles.container}>
        <div className={styles.headerRow}>
          <div className={styles.searchTypeContainer}>
            <Combobox
              options={searchTypeOptions.map(opt => ({ 
                label: opt.label, 
                value: opt.value || '', 
                description: opt.description,
                icon: opt.icon
              }))}
              value={currentSearchType?.value || ''}
              onChange={(val) => {
                if (val) {
                  const selected = searchTypeOptions.find(opt => opt.value === val.value);
                  if (selected) {
                    this.onSearchTypeChange(selected);
                  }
                }
              }}
              width={24}
              placeholder="Search type"
            />
            <Tooltip content={currentSearchType?.description || 'Select search type'}>
              <Badge 
                text={currentSearchType?.label || 'Standard'} 
                color="blue" 
                className={styles.badge}
              />
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
                value={searchId || ''}
                onChange={this.onSearchIdChange}
                placeholder="my-base-search"
                width={40}
              />
            </Field>
          </div>
        )}

        {isChainSearch && (
          <div className={styles.conditionalField}>
            <Field 
              label="Base Search Reference" 
              description="RefId of the base search to build upon"
            >
              <Input
                value={baseSearchRefId || ''}
                onChange={this.onBaseSearchRefIdChange}
                placeholder="my-base-search"
                width={40}
              />
            </Field>
          </div>
        )}

        <div className={styles.queryContainer}>
          <Field 
            label={
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span>Splunk Query</span>
                {isChainSearch && (
                  <Badge text="Chain" color="orange" className={styles.badge} />
                )}
                {isBaseSearch && (
                  <Badge text="Base" color="green" className={styles.badge} />
                )}
              </div>
            }
            description={
              isChainSearch ? 
                "Commands to apply to the base search (e.g., 'stats count by host')" :
              isBaseSearch ?
                "Search query that provides results for chain searches" :
                "Your Splunk search query"
            }
          >
            <div className={styles.queryField}>
              {(!queryText || queryText.trim() === '') && (
                <div className={styles.placeholder}>
                  {isChainSearch ? 
                    "| stats count by host | head 10" : 
                    "index=main sourcetype=access_* | head 100"
                  }
                </div>
              )}
              <CodeEditor
                value={queryText || ''}
                language="spl"
                height={this.state.editorHeight}
                onChange={this.onQueryTextChange}
                showLineNumbers={true}
                showMiniMap={false}
                onEditorDidMount={(editor: MonacoEditor) => {
                  this.editorInstance = editor;
                  setTimeout(() => {
                    const domNode = editor.getDomNode();
                    if (domNode) {
                      const editorContainer = domNode.closest('[data-testid="data-testid ReactMonacoEditor editorLazy"]') as HTMLElement;
                      if (editorContainer) {
                        editorContainer.style.height = `${this.state.editorHeight}px`;
                      }
                    }
                    const model = (editor as any).getModel();
                    if (model) {
                      model.onDidChangeContent((e: any) => {
                        const currentPosition = (editor as any).getPosition();
                        const lineCount = model.getLineCount();
                        let isBottomExpansion = false;
                        if (e.changes && e.changes.length > 0) {
                            const hasNewlineAddition = e.changes.some((change: any) => 
                                change.text && change.text.includes('\n') && change.rangeLength === 0
                            );
                            if (hasNewlineAddition && currentPosition) {
                                const cursorLine = currentPosition.lineNumber;
                                const preChangeLineCount = lineCount - 1;
                                if (cursorLine === preChangeLineCount) {
                                    isBottomExpansion = true;
                                } else if (cursorLine === preChangeLineCount - 1) {
                                    const lastLineContent = model.getLineContent(lineCount);
                                    if (!lastLineContent || lastLineContent.trim() === '') {
                                        isBottomExpansion = true;
                                    }
                                }
                            }
                        }
                        if (isBottomExpansion) {
                          this.updateEditorHeight({ isBottomExpansion: true });
                        } else {
                          setTimeout(() => {
                            this.updateEditorHeight({ preservePosition: true });
                          }, 10);
                        }
                      });
                    }
                    this.updateEditorHeight();
                    this.startHeightPolling();
                  }, 100);
                }}
                onBeforeEditorMount={(monaco: any) => {
                  if (!monaco.languages.getLanguages().some((lang: any) => lang.id === 'spl')) {
                    monaco.languages.register({ id: 'spl' });
                  }
                  monaco.languages.setMonarchTokensProvider('spl', splLanguage);
                  monaco.editor.defineTheme('spl-splunk-theme', {
                    base: 'vs-dark',
                    inherit: true,
                    rules: [
                      { token: 'keyword.spl-command', foreground: '789EFF' },
                      { token: 'predefined.spl-agg', foreground: 'D97ED9' },
                      { token: 'predefined.spl-function', foreground: 'D97ED9' },
                      { token: 'identifier.spl-field-name', foreground: '95D640' },
                      { token: 'keyword.spl-clause', foreground: 'F7A45B' },
                      { token: 'operator', foreground: 'FFFFFF' },
                      { token: 'operator.logical', foreground: 'FFFF00' },
                      { token: 'string', foreground: 'CCCCCC' },
                      { token: 'number', foreground: 'ADD8E6' },
                      { token: 'delimiter', foreground: 'FFFFFF' },
                      { token: '@brackets', foreground: 'FFFFFF' },
                      { token: 'identifier', foreground: 'CCCCCC' },
                      { token: 'comment', foreground: 'AAAAAA' },
                      { token: 'white', foreground: 'CCCCCC' },
                    ],
                    colors: {
                      'editor.background': '#2b3033'
                    }
                  });
                  monaco.editor.setTheme('spl-splunk-theme');
                  setTimeout(() => {
                    monaco.editor.setTheme('spl-splunk-theme');
                  }, 100);
                }}
                monacoOptions={{
                  fontSize: 13,
                  lineHeight: 18,
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
                    vertical: 'hidden',
                    horizontal: 'hidden',
                    useShadows: false,
                    verticalScrollbarSize: 0,
                    horizontalScrollbarSize: 0
                  }
                }}
              />
            </div>
          </Field>
        </div>
      </div>
    );
  }
}