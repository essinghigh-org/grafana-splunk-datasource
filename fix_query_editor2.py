with open('src/QueryEditor.tsx', 'r') as f:
    content = f.read()

import re

pattern = re.compile(r'\{isBaseSearch && \(\s*<div.*?</div>\s*\)\}\s*\{isChainSearch && \(\s*<div.*?</div>\s*\)\}', re.DOTALL)
replacement = "<QueryEditorConfig query={query} onChange={onChange} styles={styles} />"
content = pattern.sub(replacement, content)

with open('src/QueryEditor.tsx', 'w') as f:
    f.write(content)
