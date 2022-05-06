package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	tpchbench "github.com/singularity-data/tpch-bench"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/exec"
	"github.com/singularity-data/tpch-bench/pkg/util"
)

var (
	benchType            string // "tpch-std", "tpch-event"
	qps                  int
	producerQps          int     // qps for single thread producer
	query                int     // only "tpch-std" need
	dataScale            float64 // only "tpch-std" need
	frontendIp           string  // RisingWave frontend addr
	frontendPort         string
	kafkaAddress         string
	kafkaPartition       int // 3 by default
	postgresDBName       string
	postgresDBUser       string
	postgresDBPwd        string
	enableLegacyFrontend bool
	samplingInterval     int //
)

func init() {
	flag.StringVar(&benchType, "type", "", "determine content of benchmark")
	flag.IntVar(&qps, "qps", 300000, "benchmark qps")
	flag.IntVar(&producerQps, "producer", 80000, "")
	flag.IntVar(&query, "query", -1, "tpch query id")
	flag.Float64Var(&dataScale, "scale", 1.0, "dataset scale of tpch")
	flag.StringVar(&frontendIp, "frontend", "localhost", "")
	flag.StringVar(&frontendPort, "frontend-port", "4566", "")
	flag.StringVar(&kafkaAddress, "kafka-addr", "localhost:9092", "")
	flag.IntVar(&kafkaPartition, "partition", 4, "kafka partition numbers per topic")
	flag.StringVar(&postgresDBName, "db-name", "postgres", "db name")
	flag.StringVar(&postgresDBUser, "user", "postgres", "db username")
	flag.StringVar(&postgresDBPwd, "pwd", "postgres", "db password")
	flag.BoolVar(&enableLegacyFrontend, "legacy-frontend", false, "")
	flag.IntVar(&samplingInterval, "i", -1, "interval that view results of the query")
	flag.Parse()
}

func main() {
	// dataSourceName := fmt.Sprintf("host=localhost port=%d user=%s password=%s dbname=%s sslmode=disable",
	// postgresDBPort, postgresDBUser, postgresDBPwd, postgresDBName)

	if enableLegacyFrontend {
		configs.SqlCreatePath = "./assets/data/create.sql"
		frontendPort = "4567"
	}
	dataSourceName := fmt.Sprintf("host=%s port=%s dbname=dev sslmode=disable", frontendIp, frontendPort)
	db, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		util.LogErr("db open failed, %s", err.Error())
	}

	// [debug] qps for single thread producer
	exec.ProducerMaxRate = producerQps

	// kafka address for tpch data producer
	// for now we recommend that users make tpch-bench and kafka server locate at the same server
	configs.KafkaAddr = "localhost:9092"
	// kafka address that will be sent to RisingWave frontend
	configs.KafkaAddrForFrontend = kafkaAddress
	// kafka partition numbers per topic
	configs.KafkaPartition = kafkaPartition

	configs.CheckMVInterval = samplingInterval

	benchmark := tpchbench.NewBenchmark(db)
	switch benchType {
	case "tpch-std":
		benchmark.RunTpchStd(query, qps, dataScale)
	case "tpch-clean":
		benchmark.CleanTpchAll(query)
	case "tpch-k":
		benchmark.RunSendKafka(query, qps, dataScale)
	case "tpch-q":
		benchmark.RunTpchQuery(query)
	default:
		util.LogErr("undefined benchmark type: %s", benchType)
	}

	_ = db.Close()
}
