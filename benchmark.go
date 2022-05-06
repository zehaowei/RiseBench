package tpch_bench

import (
	"database/sql"
	"fmt"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/exec"
	"github.com/singularity-data/tpch-bench/pkg/metric"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"path/filepath"
	"time"
)

type Benchmark struct {
	db             *sql.DB
	metricsManager *metric.MetricsManager
}

func NewBenchmark(db *sql.DB) *Benchmark {
	return &Benchmark{
		db,
		metric.NewMetricsManager(),
	}
}

func (b *Benchmark) RunTpchStd(queryId int, rate int, scale float64) {
	util.LogInfo("------Prepare to run tpch query------")
	sqlConfig := configs.NewTpchSqlConfig(queryId)
	tpchConfig := configs.NewTpchConfig(queryId, rate, scale)

	// create all topics in Kafka
	err := exec.AdminTopics("create")
	if err != nil {
		util.LogErr(err.Error())
	}

	// prepare tpch data generator
	kafkaExec := exec.NewQueryKafkaExecutor(tpchConfig)
	err = kafkaExec.Prepare()
	if err != nil {
		util.LogErr(err.Error())
		return
	}

	// create all source tables in RisingWave
	util.LogInfo("------Create all source tables in RisingWave------")
	paths, err := filepath.Glob(sqlConfig.SqlCreatePathPattern)
	if err != nil {
		util.LogErr("parse sql create file path err: %s", err.Error())
		return
	}
	err = b.runSQLFiles(paths, configs.SQLCreateSource)
	if err != nil {
		util.LogErr("Create source tables error: %s", err.Error())
		return
	}

	// send data rows of small tables in advance
	kafkaExec.SendKafkaBatch()

	// create mv related to a specific tpch query
	util.LogInfo("------Create MV for q%d------", queryId)
	paths, err = filepath.Glob(sqlConfig.SqlQueryPathPattern)
	if err != nil {
		util.LogErr("parse sql mv query file path err: %s", err.Error())
		return
	}
	err = b.runSQLFiles(paths, configs.SQLNormal)
	if err != nil {
		util.LogErr(err.Error())
		return
	}

	// send data rows of main table in realtime
	kafkaExec.SendKafkaRealTime()

	// check results
	err = b.checkResults(queryId)
	if err != nil {
		util.LogErr(err.Error())
	}
}

func (b *Benchmark) CleanTpchAll(queryId int) {
	util.LogInfo("------Prepare to clean RisingWave and Kafka------")

	if queryId != -1 {
		// drop mv related to a specific tpch query
		executor := exec.NewSQLExecutor(b.db)
		err := executor.ExecuteSQLStatement(fmt.Sprintf("DROP MATERIALIZED VIEW tpch_q%d", queryId))
		if err != nil {
			util.LogErr(err.Error())
		}

		// drop all source tables in RisingWave
		sqlConfig := configs.NewTpchSqlConfig(queryId)
		paths, err := filepath.Glob(sqlConfig.SqlDropPathPattern)
		if err != nil {
			util.LogErr("parse sql drop file path err: %s", err.Error())
		}
		_ = b.runSQLFiles(paths, configs.SQLNormal)
	}

	// drop all topics in Kafka
	err := exec.AdminTopics("delete")
	if err != nil {
		util.LogErr(err.Error())
	}
}

func (b *Benchmark) RunSendKafka(query int, rate int, scale float64) {
	util.LogInfo("------Prepare to send all data to Kafka------")

	// create all topics in Kafka
	err := exec.AdminTopics("create")
	if err != nil {
		util.LogErr(err.Error())
	}

	// prepare tpch data generator
	tpchConfig := configs.NewTpchConfig(query, rate, scale)
	kafkaExec := exec.NewQueryKafkaExecutor(tpchConfig)
	err = kafkaExec.Prepare()
	if err != nil {
		util.LogErr(err.Error())
	}

	kafkaExec.SendKafkaBatch()
	kafkaExec.SendKafkaRealTime()
}

func (b *Benchmark) RunTpchQuery(queryId int) {
	util.LogInfo("------Prepare to send tpch query to RisingWave------")
	sqlConfig := configs.NewTpchSqlConfig(queryId)

	// create all source tables in RisingWave
	util.LogInfo("------Create all source tables in RisingWave------")
	paths, err := filepath.Glob(sqlConfig.SqlCreatePathPattern)
	if err != nil {
		util.LogErr("parse sql create file path err: %s", err.Error())
	}
	err = b.runSQLFiles(paths, configs.SQLCreateSource)
	if err != nil {
		return
	}

	// create mv related to a specific tpch query
	util.LogInfo("------Create MV for q%d------", queryId)
	paths, err = filepath.Glob(sqlConfig.SqlQueryPathPattern)
	if err != nil {
		util.LogErr("parse sql mv query file path err: %s", err.Error())
	}
	err = b.runSQLFiles(paths, configs.SQLNormal)
	if err != nil {
		return
	}
}

// Call SQLExecutor to send SQL to frontend
func (b *Benchmark) runSQLFiles(paths []string, typ configs.SQLStmtType) error {
	executor := exec.NewSQLExecutor(b.db)
	for _, path := range paths {
		s := util.ReadFile(path)
		e := executor.ExecuteSQLFile(s, path, typ)
		return e
	}
	return nil
}

func (b *Benchmark) checkResults(queryId int) error {
	if configs.CheckMVInterval != -1 && queryId >= 1 && queryId <= 20 {
		timer := time.NewTicker(time.Duration(configs.CheckMVInterval) * time.Second)
		for {
			select {
			case <-timer.C:
				executor := exec.NewSQLExecutor(b.db)
				err, re := executor.ExecuteSQLQuery(fmt.Sprintf("select * from tpch_q%d", queryId))
				if err != nil {
					timer.Stop()
					return err
				}
				util.LogInfo("---result---\n%s", re)
			}
		}
	}
	return nil
}
