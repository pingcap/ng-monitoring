package timeseries

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vminsert"
	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect"
	"net/http"
)

var _ http.HandlerFunc = InsertHandler
var _ http.HandlerFunc = SelectHandler

func InsertHandler(writer http.ResponseWriter, request *http.Request) {
	vminsert.RequestHandler(writer, request)
}

func SelectHandler(writer http.ResponseWriter, request *http.Request) {
	vmselect.RequestHandler(writer, request)
}
