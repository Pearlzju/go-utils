package middlewares

import (
	utils "github.com/Laisky/go-utils"
	"github.com/Laisky/zap"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
)

var (
	defaultMetricAddr = "localhost:8080"
	defaultMetricPath = "/metrics"
	defaultPProfPath  = "/pprof"
)

// MetricOption metric option argument
type MetricOption struct {
	addr, pprofPath string
}

// NewMetricOption create new default option
func NewMetricOption() *MetricOption {
	return &MetricOption{
		addr:      defaultMetricAddr,
		pprofPath: defaultPProfPath,
	}
}

// MetricsOptFunc option of metrics
type MetricsOptFunc func(*MetricOption)

// WithAddr set option addr
func WithAddr(addr string) MetricsOptFunc {
	return func(opt *MetricOption) {
		opt.addr = addr
	}
}

// WithPprofPath set option pprofPath
func WithPprofPath(path string) MetricsOptFunc {
	return func(opt *MetricOption) {
		opt.pprofPath = path
	}
}

// EnableMetric enable metrics for exsits gin server
func EnableMetric(srv *gin.Engine, options ...MetricsOptFunc) {
	opt := NewMetricOption()
	for _, optf := range options {
		optf(opt)
	}
	pprof.Register(srv, opt.pprofPath)
	BindPrometheus(srv)
}

// StartHTTPMetricSrv start new gin server with metrics api
func StartHTTPMetricSrv(options ...MetricsOptFunc) {
	opt := NewMetricOption()
	for _, optf := range options {
		optf(opt)
	}

	srv := gin.New()
	EnableMetric(srv, options...)
	utils.Logger.Info("listening on http", zap.String("http-addr", opt.addr))
	utils.Logger.Panic("server exit", zap.Error(srv.Run(opt.addr)))
}
