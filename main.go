package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type topicStats struct {
	Partitions int32
	Messages   int64
}

func main() {
	var (
		brokersStr      string
		businessRegexp  string
		topicGrep       string
		kafkaVersionStr string
		logVerbose      bool
	)

	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "Comma-separated list of Kafka brokers")
	flag.StringVar(&businessRegexp, "business-regexp", "^[^_].*", "Regexp for business topics (default: not starting with __)")
	flag.StringVar(&topicGrep, "topic-grep", "", "Optional substring filter for topic names")
	flag.StringVar(&kafkaVersionStr, "kafka-version", "2.7.0", "Kafka protocol version (e.g. 2.7.0, 2.8.0, 3.4.0)")
	flag.BoolVar(&logVerbose, "v", false, "Verbose logging to stderr")
	flag.Parse()

	if !logVerbose {
		log.SetOutput(os.Stderr)
	}

	brokers := strings.Split(brokersStr, ",")

	busRe, err := regexp.Compile(businessRegexp)
	if err != nil {
		log.Fatalf("invalid business-regexp: %v", err)
	}

	cfg := sarama.NewConfig()
	cfg.Net.DialTimeout = 5 * time.Second
	cfg.Net.ReadTimeout = 10 * time.Second
	cfg.Net.WriteTimeout = 10 * time.Second
	cfg.Metadata.Retry.Max = 3
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	version, err := parseKafkaVersion(kafkaVersionStr)
	if err != nil {
		log.Fatalf("invalid kafka-version: %v", err)
	}
	cfg.Version = version

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		log.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("failed to create cluster admin: %v", err)
	}
	defer admin.Close()

	// ===== TOPICS =====
	topicsMeta, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("failed to list topics: %v", err)
	}

	var topics []string
	for name := range topicsMeta {
		if !busRe.MatchString(name) {
			continue
		}
		if topicGrep != "" && !strings.Contains(name, topicGrep) {
			continue
		}
		topics = append(topics, name)
	}
	sort.Strings(topics)

	// Если топиков нет — просто заголовок
	fmt.Println("topic,partitions,consumers,messages")
	if len(topics) == 0 {
		return
	}

	if logVerbose {
		log.Printf("found %d business topics", len(topics))
	}

	// ===== TOPIC OFFSETS (для messages) =====
	topicStatsMap := make(map[string]topicStats)

	for _, t := range topics {
		detail := topicsMeta[t]
		var parts int32 = detail.NumPartitions
		if parts <= 0 {
			partitions, err := client.Partitions(t)
			if err != nil {
				log.Printf("WARN: failed to get partitions for topic %s: %v", t, err)
				continue
			}
			parts = int32(len(partitions))
		}

		var earliestSum, latestSum int64

		for p := int32(0); p < parts; p++ {
			earliest, err := client.GetOffset(t, p, sarama.OffsetOldest)
			if err != nil {
				log.Printf("WARN: GetOffset(Oldest) topic=%s partition=%d: %v", t, p, err)
				continue
			}
			latest, err := client.GetOffset(t, p, sarama.OffsetNewest)
			if err != nil {
				log.Printf("WARN: GetOffset(Newest) topic=%s partition=%d: %v", t, p, err)
				continue
			}
			if earliest < 0 {
				earliest = 0
			}
			if latest < 0 {
				latest = 0
			}
			earliestSum += earliest
			latestSum += latest
		}

		messages := latestSum - earliestSum
		if messages < 0 {
			messages = latestSum
		}

		topicStatsMap[t] = topicStats{
			Partitions: parts,
			Messages:   messages,
		}
	}

	// ===== CONSUMER GROUPS → сколько консьюмеров на топик =====
	// Шаг 1: получаем список групп
	groupsMap, err := admin.ListConsumerGroups()
	if err != nil {
		log.Printf("WARN: failed to list consumer groups: %v", err)
	}

	var groupIDs []string
	for g := range groupsMap {
		groupIDs = append(groupIDs, g)
	}
	sort.Strings(groupIDs)

	// Шаг 2: считаем количество активных консьюмеров в группе
	groupConsumers := make(map[string]int64)
	if len(groupIDs) > 0 {
		desc, err := admin.DescribeConsumerGroups(groupIDs)
		if err != nil {
			log.Printf("WARN: DescribeConsumerGroups: %v", err)
		} else {
			for _, d := range desc {
				// активные consumers = кол-во членов
				groupConsumers[d.GroupId] = int64(len(d.Members))
			}
		}
	}

	// Шаг 3: для каждой группы смотрим, какие топики она реально читает
	// (есть коммиты offset >= 0 по хотя бы одной партиции)
	topicConsumers := make(map[string]int64)

	for _, g := range groupIDs {
		consCount := groupConsumers[g]
		if consCount == 0 {
			// у группы нет активных consumer'ов — как в UI эти группы обычно не интересуют
			continue
		}

		offsetsResp, err := admin.ListConsumerGroupOffsets(g, nil)
		if err != nil {
			log.Printf("WARN: ListConsumerGroupOffsets(group=%s): %v", g, err)
			continue
		}

		for topic, partMap := range offsetsResp.Blocks {
			// нас интересуют только наши business-топики
			if _, ok := topicStatsMap[topic]; !ok {
				continue
			}
			hasOffsets := false
			for _, block := range partMap {
				if block == nil {
					continue
				}
				if block.Offset >= 0 {
					hasOffsets = true
					break
				}
			}
			if !hasOffsets {
				continue
			}
			// эта группа реально читает этот топик → добавляем активных consumer'ов
			topicConsumers[topic] += consCount
		}
	}

	// ===== ВЫВОД =====
	for _, t := range topics {
		s := topicStatsMap[t]
		cons := topicConsumers[t] // по умолчанию 0, если никто не читает

		fmt.Printf("%s,%d,%d,%d\n",
			t,
			s.Partitions,
			cons,
			s.Messages,
		)
	}
}

func parseKafkaVersion(v string) (sarama.KafkaVersion, error) {
	switch v {
	case "2.0.0":
		return sarama.V2_0_0_0, nil
	case "2.1.0":
		return sarama.V2_1_0_0, nil
	case "2.2.0":
		return sarama.V2_2_0_0, nil
	case "2.3.0":
		return sarama.V2_3_0_0, nil
	case "2.4.0":
		return sarama.V2_4_0_0, nil
	case "2.5.0":
		return sarama.V2_5_0_0, nil
	case "2.6.0":
		return sarama.V2_6_0_0, nil
	case "2.7.0":
		return sarama.V2_7_0_0, nil
	case "2.8.0":
		return sarama.V2_8_0_0, nil
	case "3.0.0":
		return sarama.V3_0_0_0, nil
	case "3.1.0":
		return sarama.V3_1_0_0, nil
	case "3.2.0":
		return sarama.V3_2_0_0, nil
	case "3.3.0":
		return sarama.V3_3_0_0, nil
	case "3.4.0":
		return sarama.V3_4_0_0, nil
	default:
		return sarama.V2_7_0_0, fmt.Errorf("unsupported version %q, use one of: 2.0.0..3.4.0", v)
	}
}
