import { dirname } from 'node:path';
import { defineConfig, devices } from '@playwright/test';
import type { PluginOptions } from '@grafana/plugin-e2e';

const adminUser = process.env.GRAFANA_ADMIN_USER ?? 'admin';
const adminPassword = process.env.GRAFANA_ADMIN_PASSWORD ?? 'admin';

export default defineConfig<PluginOptions>({
  testDir: './tests/e2e',
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 1 : 0,
  workers: process.env.CI ? 2 : undefined,
  reporter: process.env.CI ? 'github' : 'list',
  use: {
    baseURL: process.env.GRAFANA_URL ?? 'http://localhost:3000',
    provisioningRootDir: './tests/provisioning',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    grafanaAPICredentials: { user: adminUser, password: adminPassword },
    user: { user: adminUser, password: adminPassword },
  },
  projects: [
    {
      name: 'auth',
      testDir: `${dirname(require.resolve('@grafana/plugin-e2e'))}/auth`,
      testMatch: /.*\.js/,
    },
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        storageState: `playwright/.auth/${adminUser}.json`,
      },
      dependencies: ['auth'],
    },
  ],
});
