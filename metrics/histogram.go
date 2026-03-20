package metrics

import "github.com/zeromicro/go-zero/core/metric"

var (
	// ServerReqDur 统计请求耗时直方图指标
	ServerReqDur = func(serverNamespace string) metric.HistogramVec {
		return metric.NewHistogramVec(&metric.HistogramVecOpts{
			Namespace: serverNamespace,
			Subsystem: "requests",
			Name:      "duration_ms",
			Help:      serverNamespace + " requests duration(ms).",
			Labels:    []string{"method"},
			Buckets:   []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 5000},
		})
	}
)
