# Grafana - Splunk Datasource

![Grafana Splunk Datasource](https://github.com/essinghigh-org/grafana-splunk-datasource/actions/workflows/ci.yml/badge.svg?branch=main)

> **NOTE >>>**
>
>This is a hard fork of [efcasado/grafana-plugin-splunk-datasource](https://github.com/efcasado/grafana-plugin-splunk-datasource) as the project has been abandoned for three years. I have updated a significant amount of the repo to the point that it does not have much in common with the original.

> **IMPORTANT NOTICE & DISCLAIMER >>>**
>
> This Splunk datasource plugin for Grafana is an independent project and is not affiliated with, endorsed, or sponsored by Grafana Labs. 
> 
> It was created without any reference to or knowledge of the official, closed-source Splunk plugin available in Grafana's Enterprise plan. This plugin is provided "as-is" under the MIT License, with no warranties of any kind, express or implied.
> 
> As an unsigned plugin, you will need to configure your Grafana instance to allow its use. Please be aware that this plugin under active development, and breaking changes may be introduced. Use in a production environment is not recommended without thorough testing. 

## What is the Grafana - Splunk Datasource

The "Grafana - Splunk Datasource" plugin is a Grafana plugin that
allows you to run SPL queries on Splunk via Grafana.

![image](https://github.com/user-attachments/assets/e7c7ff5e-be86-4bf3-9782-933bb3a846ef)

## Installation

1. Download the latest release of the plugin

2. Extract it in your Grafana's installation plugin directory (`/var/lib/grafana/plugins`)

    ```bash
    tar -zxf essinghigh-splunk-datasource-XXXXX.tar.gz -C YOUR_PLUGIN_DIR
    ```
    
3. As of Grafana v8+ unsigned plugins must be explicitly allowed within Grafana's configuration (`/etc/grafana/grafana.ini`)

    ```allow_loading_unsigned_plugins = essinghigh-splunk-datasource ```
 
## Configuration

The plugin can be configured
by an administrator from Grafana's UI `Configuration --> Datasources --> Add data source`. 

> **NOTE:** By default Splunk's REST API is only available via HTTPS (even if you allow HTTP access on a differen port), usually on: https://splunk:8089

(Example configuration via the Grafana WebUI (Grafana 12.x):

<img src="https://github.com/user-attachments/assets/a5790b24-e1d8-4ed7-8f52-fa6e2df0d511" width="50%" />

## Development verification

Use Node.js 24 LTS, Corepack, Yarn 4 (pinned in `package.json`), and Go 1.26.10 or newer. Run `corepack enable` and `yarn install --immutable` before building. Grafana 13.2.0 or newer is supported; local tests use React 19 to match Grafana.

- Use `yarn start` (or `yarn watch`) for local frontend watch builds.
- Use `yarn server` to launch the local Grafana environment (defaults to `GRAFANA_VERSION=13.3.0`, configurable via env var; exposed at `http://127.0.0.1:3000`).
- The datasource includes a Go backend. Install Go 1.26.10 or newer before running a complete build; `yarn build` emits the frontend bundle and the platform-specific backend executable consumed by Grafana.
- Dashboard, Explore, and alert queries are handled by the backend through Grafana's `/api/ds/query` endpoint. Variable queries use the same backend, including secure authentication, timeouts, and cancellation.
- Before opening a PR, run the same checks gated in CI:

```bash
go test -race ./pkg/...
go vet ./pkg/...
yarn lint
yarn typecheck
yarn test:ci
yarn build
```

## Browser and release verification

CI runs browser tests against Grafana 13.2.0 and 13.3.0 with a local mock Splunk server. These exercise secure configuration, failed authentication, queries, variables, and Grafana Alerting without requiring a live Splunk deployment.

For local browser tests, run `yarn e2e:mock`, start Grafana with `tests/provisioning` mounted as its provisioning directory and `MOCK_SPLUNK_URL` pointing to the mock (default `http://127.0.0.1:18089`), then run `yarn e2e`. Set `GRAFANA_URL` and `GRAFANA_ADMIN_PASSWORD` to match that test instance. The CI workflow contains a complete Docker startup example.

`yarn build` builds the current platform. `yarn build:release` uses the Grafana SDK to build all six supported backend targets and their Go build manifest. `yarn package` produces ZIP and tar.gz archives under `packages/`, rooted at the plugin ID with executable backend binaries. Release commits retain the `release: ` prefix and commit-hash naming; the release workflow waits for browser tests before publishing.

## Testing in Grafana:

### Standard SPL Query:
![image](https://github.com/user-attachments/assets/441535d8-5767-4d45-a150-43c3afd86bb0)


### Using Base / Chain Searches:
![image](https://github.com/user-attachments/assets/a23c5696-ad2c-4504-89e3-119d35ca83c3)
![image](https://github.com/user-attachments/assets/199b07a5-c6ab-4295-b50b-39fa15314777)
![image](https://github.com/user-attachments/assets/f9be5d49-a6e7-4595-819e-862623c2909e)
