package exec

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/singularity-data/tpch-bench/pkg/configs"
	"github.com/singularity-data/tpch-bench/pkg/data"
	"github.com/singularity-data/tpch-bench/pkg/util"
	"math"
	"sync"
	"time"
)

var ProducerMaxRate int = 100000

type QueryKafkaExecutor struct {
	config      *configs.TpchBenchConfig
	producerCfs []*configs.KafkaProducerConfig
	tableGen    *data.TableGenerator
}

func NewQueryKafkaExecutor(config *configs.TpchBenchConfig) *QueryKafkaExecutor {
	return &QueryKafkaExecutor{
		config,
		make([]*configs.KafkaProducerConfig, 0),
		nil,
	}
}

func AdminTopics(op string) error {
	util.LogInfo("------%s kafka topic------", op)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": configs.KafkaAddr,
	})
	if err != nil {
		return util.Errorf("Create kafka admin client error: %s", err.Error())
	}

	var results []kafka.TopicResult
	if op == "create" {
		topics := make([]kafka.TopicSpecification, 0)
		for _, table := range configs.TpchAllTables {
			topics = append(topics, kafka.TopicSpecification{
				Topic:             string(table),
				NumPartitions:     configs.KafkaPartition,
				ReplicationFactor: 1,
			})
		}
		results, err = client.CreateTopics(ctx, topics)
	} else if op == "delete" {
		topics := make([]string, 0)
		for _, table := range configs.TpchAllTables {
			topics = append(topics, string(table))
		}
		results, err = client.DeleteTopics(ctx, topics)
	} else {
		return util.Errorf("Undefined kafka topic operation: %s", op)
	}

	if err != nil {
		return util.Errorf("%s kafka topic error: %s", op, err.Error())
	}
	for _, result := range results {
		util.LogInfo("%s: %s", op, result.String())
	}

	client.Close()
	return nil
}

func (k *QueryKafkaExecutor) Prepare() error {
	containOrder := false
	containLineItem := false
	for _, table := range k.config.Tables {
		if table == configs.Orders {
			containOrder = true
		} else if table == configs.LineItem {
			containLineItem = true
		}
	}
	if containOrder && containLineItem {
		return k.prepareEventsSpecial()
	}
	return k.prepareEvents()
}

func (k *QueryKafkaExecutor) prepareEventsSpecial() error {
	orderRateSum := int(float64(k.config.Rate) / 5)
	lineitemRateSum := int(4 * float64(k.config.Rate) / 5)

	orderProducers := int(math.Max(float64(orderRateSum/ProducerMaxRate), 1.0))
	orderProducerRate := orderRateSum / orderProducers
	lineitemProducers := int(math.Max(float64(lineitemRateSum/ProducerMaxRate), 1.0))
	lineitemProducerRate := lineitemRateSum / lineitemProducers

	tablePartsMap := map[configs.TpchTable]int{
		configs.LineItem: 0,
		configs.Orders:   0,
		configs.Customer: 0,
		configs.Part:     0,
		configs.Supplier: 0,
		configs.PartSupp: 0,
		configs.Nation:   0,
		configs.Region:   0,
	}

	tablePartsMap[configs.LineItem] = lineitemProducers
	k.producerCfs = append(k.producerCfs, &configs.KafkaProducerConfig{
		Nums:  lineitemProducers,
		Rate:  lineitemProducerRate,
		Table: configs.LineItem,
		Type:  configs.RealTime,
	})
	tablePartsMap[configs.Orders] = orderProducers
	k.producerCfs = append(k.producerCfs, &configs.KafkaProducerConfig{
		Nums:  orderProducers,
		Rate:  orderProducerRate,
		Table: configs.Orders,
		Type:  configs.RealTime,
	})

	for _, table := range k.config.Tables {
		if table != configs.LineItem && table != configs.Orders {
			tablePartsMap[table] = 1
			k.producerCfs = append(k.producerCfs, &configs.KafkaProducerConfig{
				Nums:  1,
				Rate:  -1,
				Table: table,
				Type:  configs.Batch,
			})
		}
	}

	c := &data.TableGeneratorConfig{
		ScaleFactor:   k.config.ScaleFactor,
		TablePartsMap: tablePartsMap,
	}
	k.tableGen = data.NewTableGenerator(c)
	return nil
}

func (k *QueryKafkaExecutor) prepareEvents() error {
	sendRate := k.config.Rate
	producerNums := sendRate / ProducerMaxRate
	if sendRate%ProducerMaxRate != 0 {
		producerNums += 1
	}
	producerRate := sendRate / producerNums

	tablePartsMap := map[configs.TpchTable]int{
		configs.LineItem: 0,
		configs.Orders:   0,
		configs.Customer: 0,
		configs.Part:     0,
		configs.Supplier: 0,
		configs.PartSupp: 0,
		configs.Nation:   0,
		configs.Region:   0,
	}
	for _, table := range k.config.Tables {
		if table == k.config.MainTable {
			tablePartsMap[k.config.MainTable] = producerNums
			k.producerCfs = append(k.producerCfs, &configs.KafkaProducerConfig{
				Nums:  producerNums,
				Rate:  producerRate,
				Table: k.config.MainTable,
				Type:  configs.RealTime,
			})
		} else {
			tablePartsMap[table] = 1
			k.producerCfs = append(k.producerCfs, &configs.KafkaProducerConfig{
				Nums:  1,
				Rate:  -1,
				Table: table,
				Type:  configs.Batch,
			})
		}
	}

	c := &data.TableGeneratorConfig{
		ScaleFactor:   k.config.ScaleFactor,
		TablePartsMap: tablePartsMap,
	}
	k.tableGen = data.NewTableGenerator(c)

	return nil
}

func (k *QueryKafkaExecutor) SendKafkaBatch() {
	util.LogInfo("------Insert small tables in advance------")
	k.send(k.getProducers(configs.Batch))
}

func (k *QueryKafkaExecutor) SendKafkaRealTime() {
	util.LogInfo("------Start benchmark streaming------")
	var timer = time.Now()
	k.send(k.getProducers(configs.RealTime))
	util.LogInfo("------Produce data in real time totally takes %f seconds------", time.Now().Sub(timer).Seconds())
}

func (k *QueryKafkaExecutor) getProducers(sendType string) []*KafkaProducer {
	producers := make([]*KafkaProducer, 0)
	idx := 0
	for _, cf := range k.producerCfs {
		if cf.Type != sendType {
			continue
		}
		for i := 0; i < cf.Nums; i++ {
			producer, err := NewKafkaProducer(idx, cf, k.tableGen.GetSingleTableGenerator(cf.Table, i))
			if err != nil {
				util.LogErr("connect to kafka error: %s", err.Error())
			}
			producers = append(producers, producer)
			idx++
		}
	}
	return producers
}

func (k *QueryKafkaExecutor) send(producers []*KafkaProducer) {
	if len(producers) == 0 {
		return
	}
	util.LogInfo("Producer number[%d]", len(producers))

	for _, producer := range producers {
		go producer.WriteRowsToKafka()
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(len(producers))
	for i := 0; i < len(producers); i++ {
		go func(id int, events chan kafka.Event) {
			for {
				_, ok := <-events
				if !ok {
					util.LogInfo("producer[%d]---finish---Send totally [%d]", id, producers[id].Size())
					break
				}
			}
			waitGroup.Done()
		}(i, producers[i].Events())
	}
	waitGroup.Wait()
}
