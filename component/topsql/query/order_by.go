package query

import "strings"

const (
	OrderByCPU       = "cpu"
	OrderByNetwork   = "network"
	OrderByLogicalIO = "logical_io"
)

const (
	metricNameNetworkBytes   = "network_bytes"
	metricNameLogicalIoBytes = "logical_io_bytes"
)

// NormalizeOrderBy normalizes user input for the `order_by` / `orderBy` parameter.
// It returns lower-cased canonical values (e.g. "logicalio" -> "logical_io").
//
// Note: empty string is preserved, so callers can apply their own defaults.
func NormalizeOrderBy(orderBy string) string {
	orderBy = strings.ToLower(strings.TrimSpace(orderBy))
	if orderBy == "logicalio" {
		return OrderByLogicalIO
	}
	return orderBy
}
