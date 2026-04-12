// Plugin aws-emr-spark provides full EMR and EMR on EKS support for JaisCloud.
// Compiled as a Go plugin (.so):
//
//	go build -buildmode=plugin -o aws-emr-spark.so .
package main

import (
	sdk "github.com/jaiscloud/plugin-sdk"
	"github.com/jaiscloud/plugin-aws-emr-spark/internal/plugin"
)

// Plugin is the symbol the host looks up with plugin.Lookup("Plugin").
var Plugin sdk.SparkPlugin = plugin.New()
