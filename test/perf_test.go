package test

import (
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/exec"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"testing"
)

func TestProducerPerf(t *testing.T) {
	tpchConfig := configs.NewTpchConfig(1, 360000, 2.0)
	kafkaExec := exec.NewQueryKafkaExecutor(tpchConfig)
	err := kafkaExec.Prepare()
	if err != nil {
		util.LogErr(err.Error())
	}
	kafkaExec.SendKafkaRealTime()
}
