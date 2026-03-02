// src/language/splMonarch.ts

export const splLanguage = {
  // Set default token to avoid errors
  defaultToken: 'source.spl',

  // --- Tokenizer Configuration ---
  // Commands (blue)
  // https://help.splunk.com/en/splunk-cloud-platform/search/spl2-search-reference/quick-reference-for-spl2-commands/spl2-command-quick-reference
  splCommands: [
    'addinfo', 'addtotals', 'anomalies', 'anomalousvalue', 'append', 'appendcols', 'appendpipe',
    'associate', 'autoregress', 'bin', 'branch', 'bucket', 'chart', 'cluster', 'collect', 'concurrency', 'contingency',
    'convert', 'correlate', 'dbinspect', 'dedup', 'delete', 'delta', 'diff', 'erex', 'eval', 'eventstats',
    'expand', 'export', 'extract', 'fieldformat', 'fields', 'fieldsummary', 'filldown', 'fillnull', 'findtypes',
    'flatten', 'foreach', 'format', 'from', 'gentimes', 'geom', 'geomfilter', 'head', 'history', 'input', 'inputcsv', 'inputlookup',
    'into', 'iplocation', 'join', 'kmeans', 'kvform', 'loadjob', 'localop', 'lookup', 'makecontinuous', 'makemv',
    'map', 'metadata', 'metasearch', 'multikv', 'mvexpand', 'mvcombine', 'nomv', 'outlier', 'outputcsv',
    'outputlookup', 'outputtext', 'overlap', 'predict', 'rare', 'regex', 'relevancy', 'reltime', 'rename',
    'replace', 'rest', 'return', 'reverse', 'rex', 'run', 'savedsearch', 'script', 'scrub', 'search',
    'select', 'selfjoin', 'sendemail', 'set', 'sichart', 'sirare', 'sistats', 'sitimechart', 'sitruncate', 'sort',
    'spath', 'spl1', 'stats', 'strcat', 'streamstats', 'table', 'tags', 'tail', 'thru', 'timechart', 'timewrap', 'top', 'transaction',
    'transpose', 'trendline', 'tscollect', 'tstats', 'typeahead', 'typelearner', 'typer', 'union', 'uniq',
    'untable', 'where', 'x11', 'xmlkv', 'xpath', 'xyseries'
  ],

  // Clauses (orange) - haven't figured out how to make these case-insensitive yet. This will do for now.
  // Also not sure how many of these are actually clauses, just guessed. If I see any issues I'll commit an update that can be rolled into the next release.
  splClauses: [
    'as', 'AS', 'by', 'BY', 'over', 'OVER', 'in', 'IN', 'where', 'WHERE', 
    'output', 'OUTPUT', 'outputnew', 'OUTPUTNEW', 'on', 'ON', 'using', 'USING', 
    'with', 'WITH', 'for', 'FOR', 'against', 'AGAINST', 'sortby', 'SORTBY',
    'and', 'AND', 'or', 'OR', 'not', 'NOT', 'xor', 'XOR'
  ],

  // Aggregation/statistical functions (pink/purple)
  // https://help.splunk.com/en/splunk-cloud-platform/search/spl2-search-reference/statistical-and-charting-functions/aggregate-functions
  splAggFunctions: [
    'avg', 'count', 'c', 'distinct_count', 'dc', 'estdc', 'estdc_error', 'exactperc',
    'max', 'mean', 'median', 'min', 'mode', 'p', 'perc', 'range', 'stdev', 'stdevp', 'sum',
    'sumsq', 'upperperc', 'var', 'varp',
  ],

  // General-purpose functions (pink/purple)
  // https://docs.splunk.com/Documentation/SplunkCloud/latest/SearchReference/CommonEvalFunctions
  splEvalFunctions: [
    // Bitwise functions
    'bit_and', 'bit_or', 'bit_not', 'bit_xor', 'bit_shift_left', 'bit_shift_right',
    // Comparison and Conditional functions
    'case', 'cidrmatch', 'coalesce', 'false', 'if', 'in', 'like', 'lookup', 'match', 
    'null', 'nullif', 'searchmatch', 'true', 'validate',
    // Conversion functions
    'ipmask', 'printf', 'toarray', 'tobool', 'todouble', 'toint', 'tomv', 'tonumber', 
    'toobject', 'tostring',
    // Cryptographic functions
    'md5', 'sha1', 'sha256', 'sha512',
    // Date and Time functions
    'now', 'relative_time', 'strftime', 'strptime', 'time',
    // Informational functions
    'isarray', 'isbool', 'isdouble', 'isint', 'ismv', 'isnotnull', 'isnull', 'isnum', 
    'isobject', 'isstr', 'typeof',
    // JSON functions
    'json_object', 'json', 'json_append', 'json_array', 'json_array_to_mv', 'json_delete',
    'json_entries', 'json_extend', 'json_extract', 'json_extract_exact', 'json_has_key_exact',
    'json_keys', 'json_set', 'json_set_exact', 'json_valid',
    // Mathematical functions
    'abs', 'ceiling', 'exact', 'exp', 'floor', 'ln', 'log', 'pi', 'pow', 'round', 
    'sigfig', 'sqrt', 'sum',
    // Multivalue eval functions
    'commands', 'mvappend', 'mvcount', 'mvdedup', 'mvfilter', 'mvfind', 'mvindex', 
    'mvjoin', 'mvmap', 'mvrange', 'mvsort', 'mvzip', 'mv_to_json_array', 'split',
    // Statistical eval functions
    'avg', 'max', 'min', 'random',
    // Text functions
    'len', 'lower', 'ltrim', 'replace', 'rtrim', 'spath', 'substr', 'trim', 'upper', 
    'urldecode',
    // Trigonometry and Hyperbolic functions
    'acos', 'acosh', 'asin', 'asinh', 'atan', 'atan2', 'atanh', 'cos', 'cosh', 'hypot', 
    'sin', 'sinh', 'tan', 'tanh'
  ],

  // --- Tokenizer States ---
  tokenizer: {
    root: [
      // A pipe transitions to the command context.
      [/\|/, { token: 'delimiter', next: '@command' }],
      // The query starts in the 'search' context.
      { include: '@searchContext' }
    ],

    // State right after a pipe, expecting a command.
    command: [
      [/\s+/, 'white'],
      // Identify the command and switch to a more specific context.
      [/([a-zA-Z_][\w]*)/, {
        cases: {
          '^(stats|chart|timechart|eventstats)$': { token: 'keyword.spl-command', next: '@statsContext' },
          '^(eval|where)$': { token: 'keyword.spl-command', next: '@evalContext' },
          '^(lookup|inputlookup)$': { token: 'keyword.spl-command', next: '@lookupContext' },
          '^(rename)$': { token: 'keyword.spl-command', next: '@renameContext' },
          '^(fields|table)$': { token: 'keyword.spl-command', next: '@fieldListContext' },
          '^(search)$': { token: 'keyword.spl-command', next: '@searchCommandContext' },
          '@splCommands': { token: 'keyword.spl-command', next: '@generalContext' },
          '@default': { token: 'identifier', next: '@generalContext' }
        }
      }],
    ],

    // Context for initial search (before the first pipe).
    searchContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      // Handle subsearch opening bracket like a pipe - transition to command context
      [/\[/, { token: '@brackets', next: '@command' }],
      // Handle `argument=value` pairs for search arguments like earliest, latest
      [/\b(earliest|latest)(=)/, ['identifier.spl-field-name', 'operator']],
      // Field names are plain identifiers, not green arguments.
      [/[a-zA-Z_][\w\-\.]*/, {
        cases: {
          '@splClauses': 'keyword.spl-clause',
          '@default': 'identifier'
        }
      }],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // Context for stats, chart, timechart commands.
    statsContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      [/\|/, { token: 'delimiter', next: '@command' }],
      // Handle subsearch opening bracket like a pipe - transition to command context
      [/\[/, { token: '@brackets', next: '@command' }],
      // "AS" clause -> switch to color the new field name green.
      [/\b(as)\b/i, { token: 'keyword.spl-clause', next: '@asClause' }],
      // Special case: p/perc followed by digits (e.g., p95, perc99) as a single agg function (pink)
      [/(p|perc)\d+/i, 'predefined.spl-agg'],
      // Functions like `sum(field)`.
      [/(\w+)(?=\s*\()/i, {
        cases: {
          '@splAggFunctions': 'predefined.spl-function',
          '@splEvalFunctions': 'predefined.spl-function',
          '@default': 'identifier'
        }
      }],
      // Bare aggregation functions (count), clauses (by), and fields.
      [/[a-zA-Z_]\w*/, {
        cases: {
          '@splClauses': 'keyword.spl-clause',
          '@splAggFunctions': 'predefined.spl-agg',
          '@default': 'identifier' // Field names
        }
      }],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // Context for explicit `search` command after a pipe.
    searchCommandContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      [/\|/, { token: 'delimiter', next: '@command' }],
      // Handle subsearch opening bracket like a pipe - transition to command context
      [/\[/, { token: '@brackets', next: '@command' }],
      // Handle `argument=value` pairs for search arguments like earliest, latest
      [/\b(earliest|latest)(=)/, ['identifier.spl-field-name', 'operator']],
      // Field names and clauses.
      [/[a-zA-Z_][\w\-\.]*/, {
        cases: {
          '@splClauses': 'keyword.spl-clause',
          '@default': 'identifier'
        }
      }],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // Context for `eval` and `where` commands.
    evalContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      [/\|/, { token: 'delimiter', next: '@command' }],
      // Functions like `case(...)` or `if(...)`.
      [/(\w+)(?=\s*\()/i, {
        cases: {
          '@splEvalFunctions': 'predefined.spl-function',
          '@default': 'identifier'
        }
      }],
      // Field names, clauses, and booleans.
      [/[a-zA-Z_][\w\-\.]*/, {
        cases: {
          '@splClauses': 'keyword.spl-clause',
          '@default': 'identifier'
        }
      }],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // Context for `lookup` command.
    lookupContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      [/\|/, { token: 'delimiter', next: '@command' }],
      // "AS" clause -> switch to color the new field name green.
      [/\b(as)\b/i, { token: 'keyword.spl-clause', next: '@asClause' }],
      // "OUTPUT" clause (case-insensitive)
      [/\b(output|outputnew)\b/i, 'keyword.spl-clause'],
      // Other clauses and fields.
      [/[a-zA-Z_]\w*/, {
        cases: {
          '@splClauses': 'keyword.spl-clause',
          '@default': 'identifier' // Lookup table name, field names
        }
      }],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // Context for `rename` command.
    renameContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      [/\|/, { token: 'delimiter', next: '@command' }],
      // The pattern is `field AS new_field`.
      [/\b(as)\b/i, { token: 'keyword.spl-clause', next: '@asClause' }],
      // Field names, which can include wildcards.
      [/[a-zA-Z_][\w\-\.\*]*/, 'identifier'],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // A temporary state to handle the field name after an "AS" clause.
    asClause: [
      [/\s+/, 'white'],
      // This token is the new field name (regular identifier). Then pop back.
      [/[a-zA-Z_][\w\-\.]+/, { token: 'identifier', next: '@pop' }],
      // If no field name, pop back to be safe.
      [/./, { token: '', next: '@pop' }]
    ],

    // Context for simple field lists (`table`, `fields`).
    fieldListContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      [/\|/, { token: 'delimiter', next: '@command' }],
      // Any word is just a field name. Wildcards are allowed.
      [/[a-zA-Z_][\w\-\.\*]*/, 'identifier'],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // Context for other commands that have `name=value` arguments (`fillnull`, `bin`, etc.).
    generalContext: [
      [/\s+/, 'white'],
      // Handle strings first (before keyword matching) - with escaped quote support
      [/"(?:[^"\\]|\\.)*"/, 'string'],
      [/'(?:[^'\\]|\\.)*'/, 'string'],
      [/\|/, { token: 'delimiter', next: '@command' }],
      // Handle subsearch opening bracket like a pipe - transition to command context
      [/\[/, { token: '@brackets', next: '@command' }],
      // Handle `argument=value` pairs, making the argument name green.
      [/([a-zA-Z_][\w\-]*)(=)/, ['identifier.spl-field-name', 'operator']],
      // Handle clauses like `BY`.
      [/[a-zA-Z_]\w*/, {
        cases: {
          '@splClauses': 'keyword.spl-clause',
          '@default': 'identifier'
        }
      }],
      // Include remaining common tokens (numbers, brackets, operators)
      [/\d[\d\.]*/, 'number'],
      [/[{}()\]]/, '@brackets'],
      [/[=,><!+\-*\/%]+/, 'operator']
    ],

    // A set of common token definitions used across multiple contexts.
    commonTokens: [
      [/"(?:[^"\\]|\\.)*"/, 'string'], // Handle escaped quotes properly
      [/'(?:[^'\\]|\\.)*'/, 'string'], // Handle escaped quotes properly
      [/\d[\d\.]*/, 'number'], // More robust number matching
      [/[{}()\]]/, '@brackets'], // Added closing bracket back
      [/[=,><!+\-*\/%]+/, 'operator']
    ]
  }
};