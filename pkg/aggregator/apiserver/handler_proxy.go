package apiserver

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	data "github.com/grafana/grafana-plugin-sdk-go/experimental/apis/data/v0alpha1"
	"go.opentelemetry.io/otel/attribute"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/component-base/tracing"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	aggregationv0alpha1 "github.com/grafana/grafana/pkg/aggregator/apis/aggregation/v0alpha1"
	query "github.com/grafana/grafana/pkg/apis/query/v0alpha1"
	"github.com/grafana/grafana/pkg/plugins"
	"github.com/grafana/grafana/pkg/web"
)

// proxyHandler provides a http.Handler which will proxy traffic to a plugin client.
type proxyHandler struct {
	localDelegate         http.Handler
	client                plugins.Client
	pluginContextProvider PluginContextProvider
	handlingInfo          atomic.Value
}

type proxyHandlingInfo struct {
	name    string
	handler *pluginHandler
}

func (r *proxyHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	value := r.handlingInfo.Load()
	if value == nil {
		r.localDelegate.ServeHTTP(w, req)
		return
	}

	ctx, span := tracing.Start(
		req.Context(),
		"grafana-aggregator",
		attribute.String("k8s.dataplaneservice.name", value.(proxyHandlingInfo).name),
		attribute.String("http.request.method", req.Method),
		attribute.String("http.request.url", req.URL.String()),
	)
	// log if the span has not ended after a minute
	defer span.End(time.Minute)
	handlingInfo := value.(proxyHandlingInfo)
	handlingInfo.handler.ServeHTTP(w, req.WithContext(ctx))
}

// these methods provide locked access to fields
func (r *proxyHandler) updateDataPlaneService(dataplaneService *aggregationv0alpha1.DataPlaneService) {
	newInfo := proxyHandlingInfo{
		name: dataplaneService.Name,
	}

	proxyPath := fmt.Sprintf("/apis/dataplane/%s/%s", dataplaneService.Spec.Group, dataplaneService.Spec.Version)

	newInfo.handler = newPluginHandler(
		r.client,
		r.pluginContextProvider,
		proxyPath,
		dataplaneService.Spec.Services,
		r.localDelegate,
	)

	r.handlingInfo.Store(newInfo)
}

// responder implements rest.Responder for assisting a connector in writing objects or errors.
type responder struct {
	w http.ResponseWriter
}

// TODO this should properly handle content type negotiation
// if the caller asked for protobuf and you write JSON bad things happen.
func (r *responder) Object(statusCode int, obj runtime.Object) {
	responsewriters.WriteRawJSON(statusCode, obj, r.w)
}

func (r *responder) Error(_ http.ResponseWriter, req *http.Request, err error) {
	tracing.SpanFromContext(req.Context()).RecordError(err)
	http.Error(r.w, err.Error(), http.StatusServiceUnavailable)
}

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
		span.AddEvent("QueryData start", attribute.Int("queries", len(queries)))
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

type PluginContextProvider interface {
	GetPluginContext(ctx context.Context, pluginID, uid string) (backend.PluginContext, error)
}
