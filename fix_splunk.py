with open('src/__tests__/datasource.test.ts', 'r') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    if ".mockResolvedValueOnce(true)" in line and "doSearchStatusRequest" in lines[i-1]:
        lines[i] = "      .mockResolvedValueOnce({ state: 'DONE', messages: [] })\n"
    if ".mockResolvedValueOnce(false)" in line and "doSearchStatusRequest" in lines[i-2]:
        lines[i] = "      .mockResolvedValueOnce({ state: 'FAILED', messages: [] });\n"

with open('src/__tests__/datasource.test.ts', 'w') as f:
    f.writelines(lines)
