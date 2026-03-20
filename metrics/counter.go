package metrics

import "github.com/zeromicro/go-zero/core/metric"

var (
	// ServerReqCodeTotal 统计请求状态码 Counter
	ServerReqCodeTotal = func(serverNamespace string) metric.CounterVec {
		return metric.NewCounterVec(&metric.CounterVecOpts{
			Namespace: serverNamespace,
			Subsystem: "requests",
			Name:      "code_total",
			Help:      serverNamespace + " requests code count.",
			Labels:    []string{"method", "code", "err"},
		})
	}
)
