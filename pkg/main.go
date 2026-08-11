package main

import (
	"log"
	"os"

	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"

	"github.com/essinghigh-org/grafana-splunk-datasource/pkg/plugin"
)

const pluginID = "essinghigh-splunk-datasource"

func main() {
	if err := datasource.Manage(pluginID, plugin.NewDatasource, datasource.ManageOpts{}); err != nil {
		log.Printf("plugin stopped: %v", err)
		os.Exit(1)
	}
}
