package exec

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/data"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"time"
)

type KafkaProducer struct {
	id       int
	topic    string
	rate     int64
	sendType string
	curIdx   int64
	producer *kafka.Producer
	dataRows data.JsonIterable
}

func NewKafkaProducer(id int, cf *configs.KafkaProducerConfig, dataRows data.JsonIterable) (*KafkaProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":            configs.KafkaAddr,
		"go.batch.producer":            true,
		"queue.buffering.max.messages": 5_000_000,
		"queue.buffering.max.kbytes":   5_000_000,
		"queue.buffering.max.ms":       "150",
		"go.delivery.reports":          false,
	})
	if err != nil {
		return nil, err
	}
	return &KafkaProducer{
		id,
		string(cf.Table),
		int64(cf.Rate),
		cf.Type,
		0,
		producer,
		dataRows,
	}, nil
}

func (k *KafkaProducer) Size() int64 {
	return k.dataRows.Capacity()
}

func (k *KafkaProducer) Events() chan kafka.Event {
	return k.producer.Events()
}

func (k *KafkaProducer) WriteRowsToKafka() {
	if k.sendType == configs.Batch {
		k.rate = k.dataRows.Capacity()
		k.produce()
	} else {
		timer := time.NewTicker(1 * time.Second)
		for k.curIdx < k.dataRows.Capacity() {
			select {
			case <-timer.C:
				k.produce()
			}
		}
		timer.Stop()
	}
	k.producer.Close()
}

func (k *KafkaProducer) produce() {
	if k.curIdx >= k.dataRows.Capacity() {
		return
	}
	var produceTimer = time.Now()
	for i := k.curIdx; i < k.curIdx+k.rate && i < k.dataRows.Capacity(); i++ {
		err := k.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: kafka.PartitionAny},
			Value:          k.dataRows.Next(),
		}, nil)
		if err != nil {
			util.LogErr(err.Error())
		}
	}
	util.LogInfo("producer[%d] %d events takes %f seconds", k.id, k.rate, time.Now().Sub(produceTimer).Seconds())
	k.curIdx += k.rate
	k.producer.Flush(10 * 1000)
}
