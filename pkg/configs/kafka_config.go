package configs

var KafkaAddr string
var KafkaAddrForFrontend string
var KafkaPartition int

const (
	RealTime string = "realtime" // producer send realtime events according to rate
	Batch    string = "batch"    // producer send all events at one stroke
)

type KafkaProducerConfig struct {
	Nums  int
	Rate  int
	Table TpchTable
	Type  string
}
