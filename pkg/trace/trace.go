package trace

import (
	"context"
	"net/http"

	"github.com/mushanyux/MSIM/pkg/mshttp"
	"github.com/mushanyux/MSIM/pkg/mslog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
)

var GlobalTrace *Trace

func SetGlobalTrace(t *Trace) {
	GlobalTrace = t
}

var (
	tracer = otel.Tracer("trace")
)

type Trace struct {
	opts     *Options
	ctx      context.Context
	shutdown func(context.Context) error

	// Metrics 监控
	Metrics IMetrics
	mslog.Log
}

func New(ctx context.Context, opts *Options) *Trace {
	return &Trace{
		ctx:     ctx,
		opts:    opts,
		Metrics: newMetrics(opts),
		Log:     mslog.NewMSLog("Trace"),
	}
}

func (t *Trace) Start() error {
	shutdown, err := t.setupOTelSDK(t.ctx)
	if err != nil {
		return err
	}
	t.shutdown = shutdown
	return nil
}

func (t *Trace) Stop() {
	t.Debug("stop...")
	if t.shutdown != nil {
		err := t.shutdown(t.ctx)
		if err != nil {
			panic(err)
		}
	}
}

func (t *Trace) Handler() http.Handler {
	return promhttp.Handler()
}

func (t *Trace) Route(r *mshttp.MSHttp) {
	t.Metrics.Route(r)
}
