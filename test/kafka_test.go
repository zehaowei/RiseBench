package test

import (
	"fmt"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/exec"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"regexp"
	"testing"
)

func TestCreateKafkaTopic(t *testing.T) {
	err := exec.AdminTopics("create")
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestDeleteKafkaTopic(t *testing.T) {
	err := exec.AdminTopics("delete")
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestSendKafka(t *testing.T) {
	util.LogInfo("------Prepare to run tpch query------")
	tpchConfig := configs.NewTpchConfig(5, 100000, 1.0)

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
	}

	// send data rows of small tables in advance
	kafkaExec.SendKafkaBatch()
}

func TestSQLCreateKafkaSource(t *testing.T) {
	sql := "statement\ncreate source supplier (\n    s_suppkey INTEGER NOT NULL,\n    s_name CHAR(25) NOT NULL,\n    " +
		"s_address VARCHAR(40) NOT NULL,\n    s_nationkey INTEGER NOT NULL,\n    s_phone CHAR(15) NOT NULL,\n    s_acctbal NUMERIC NOT NULL,\n    " +
		"s_comment VARCHAR(101) NOT NULL)\n    with (\n    'upstream.source' = 'kafka',\n    'kafka.topic' = 'supplier',\n    " +
		"'kafka.bootstrap.servers' = 'localhost:9092'\n    ) row format 'json'"
	reg := regexp.MustCompile(`localhost:9092`)
	sql = reg.ReplaceAllString(sql, "127.0.0.1:9000")
	fmt.Println(sql)
}
