package metric

type BasicMetric struct {
	tps float64
}

type MetricsManager struct {
}

func NewMetricsManager() *MetricsManager {
	return &MetricsManager{}
}
