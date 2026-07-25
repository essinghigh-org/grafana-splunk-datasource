with open('src/QueryEditor.tsx', 'r') as f:
    lines = f.readlines()

new_lines = []
import_added = False
for line in lines:
    if "import { Badge, CodeEditor, Combobox, ComboboxOption, Field, Input, Switch, Tooltip } from '@grafana/ui';" in line:
        new_lines.append("import { Badge, CodeEditor, Combobox, ComboboxOption, Field, Tooltip } from '@grafana/ui';\n")
        continue

    if "import { registerSplunkLanguage, SPL_LANGUAGE_ID } from './language/splMonaco';" in line:
        new_lines.append(line)
        new_lines.append("import { QueryEditorConfig } from './components/QueryEditorConfig';\n")
        continue

    new_lines.append(line)

with open('src/QueryEditor.tsx', 'w') as f:
    f.writelines(new_lines)
