# Grafana - Splunk Datasource

![Grafana Splunk Datasource](https://github.com/essinghigh/grafana-splunk-datasource/actions/workflows/ci.yml/badge.svg?branch=main)

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

- Use `yarn start` (or `yarn watch`) for local frontend watch builds.
- Use `yarn server` to launch the local Grafana environment (defaults to `GRAFANA_VERSION=12.0.0`, configurable via env var).
- Before opening a PR, run the same checks gated in CI:

```bash
yarn lint
yarn typecheck
yarn test
yarn build
```

## Testing in Grafana:

### Standard SPL Query:
![image](https://github.com/user-attachments/assets/441535d8-5767-4d45-a150-43c3afd86bb0)


### Using Base / Chain Searches:
![image](https://github.com/user-attachments/assets/a23c5696-ad2c-4504-89e3-119d35ca83c3)
![image](https://github.com/user-attachments/assets/199b07a5-c6ab-4295-b50b-39fa15314777)
![image](https://github.com/user-attachments/assets/f9be5d49-a6e7-4595-819e-862623c2909e)
