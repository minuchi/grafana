package apiserver

import (
	"fmt"
	"net/http"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	data "github.com/grafana/grafana-plugin-sdk-go/experimental/apis/data/v0alpha1"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/component-base/tracing"

	aggregationv0alpha1 "github.com/grafana/grafana/pkg/aggregator/apis/aggregation/v0alpha1"
	query "github.com/grafana/grafana/pkg/apis/query/v0alpha1"
	"github.com/grafana/grafana/pkg/plugins"
	"github.com/grafana/grafana/pkg/web"
)

type pluginHandler struct {
	client                plugins.Client
	mux                   *http.ServeMux
	pluginContextProvider PluginContextProvider
	availableServices     []aggregationv0alpha1.Service
	delegate              http.Handler
}

func newPluginHandler(
	client plugins.Client,
	pluginContextProvider PluginContextProvider,
	proxyPath string,
	availableServices []aggregationv0alpha1.Service,
	delegate http.Handler,
) *pluginHandler {
	mux := http.NewServeMux()
	h := &pluginHandler{
		client:                client,
		mux:                   mux,
		pluginContextProvider: pluginContextProvider,
		delegate:              delegate,
		availableServices:     availableServices,
	}

	for _, service := range availableServices {
		switch service.Type {
		case aggregationv0alpha1.DataServiceType:
			mux.Handle("POST "+proxyPath+"/namespaces/{namespace}/query", h.QueryDataHandler())
		}
	}

	// fallback to the delegate
	mux.Handle("/", delegate)

	return h
}

func (h *pluginHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	h.mux.ServeHTTP(w, req)
}

func (h *pluginHandler) QueryDataHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		span := tracing.SpanFromContext(ctx)
		span.AddEvent("QueryDataHandler")
		responder := &responder{w: w}
		dqr := data.QueryDataRequest{}
		err := web.Bind(req, &dqr)
		if err != nil {
			responder.Error(w, req, err)
			return
		}

		queries, dsRef, err := data.ToDataSourceQueries(dqr)
		if err != nil {
			responder.Error(w, req, err)
			return
		}
		span.AddEvent("GetPluginContext",
			attribute.String("datasource.uid", dsRef.UID),
			attribute.String("datasource.type", dsRef.Type),
		)
		pluginContext, err := h.pluginContextProvider.GetPluginContext(ctx, dsRef.Type, dsRef.UID)
		if err != nil {
			responder.Error(w, req, fmt.Errorf("unable to get plugin context: %w", err))
			return
		}

		if dsRef != nil && pluginContext.DataSourceInstanceSettings != nil && dsRef.UID != pluginContext.DataSourceInstanceSettings.UID {
			responder.Error(w, req, fmt.Errorf("expected query body datasource and request to match"))
			return
		}

		ctx = backend.WithGrafanaConfig(ctx, pluginContext.GrafanaConfig)
		span.AddEvent("QueryData start", attribute.Int("count", len(queries)))
		rsp, err := h.client.QueryData(ctx, &backend.QueryDataRequest{
			Queries:       queries,
			PluginContext: pluginContext,
		})
		if err != nil {
			responder.Error(w, req, err)
			return
		}
		statusCode := query.GetResponseCode(rsp)
		span.AddEvent("QueryData end", attribute.Int("http.response.status_code", statusCode))
		responder.Object(statusCode,
			&query.QueryDataResponse{QueryDataResponse: *rsp},
		)
	}
}
