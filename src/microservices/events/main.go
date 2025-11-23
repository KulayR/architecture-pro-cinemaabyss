package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
)

const (
	TopicUser    = "user-events"
	TopicPayment = "payment-events"
	TopicMovie   = "movie-events"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "kafka:9092"), ",")
	port := getEnv("PORT", "8082")

	producer, err := setupProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to setup producer: %v", err)
	}
	defer producer.Close()

	go startConsumer(brokers, []string{TopicUser, TopicPayment, TopicMovie})

	r := gin.Default()

	api := r.Group("/api/events")
	{
		api.GET("/health", func(c *gin.Context) {
			c.JSON(http.StatusOK, gin.H{"status": true})
		})

		// Обработчики событий
		api.POST("/user", func(c *gin.Context) {
			handleGenericEvent(c, producer, TopicUser)
		})
		api.POST("/movie", func(c *gin.Context) {
			handleGenericEvent(c, producer, TopicMovie)
		})
		api.POST("/payment", func(c *gin.Context) {
			handleGenericEvent(c, producer, TopicPayment)
		})
	}

	log.Printf("Events Service running on port %s", port)
	if err := r.Run(":" + port); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

func handleGenericEvent(c *gin.Context, producer sarama.SyncProducer, topic string) {
	var payload map[string]interface{}

	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON"})
		return
	}

	msgBytes, err := json.Marshal(payload)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encode JSON"})
		return
	}

	key := ""
	if id, ok := payload["id"].(string); ok {
		key = id
	} else if id, ok := payload["user_id"].(float64); ok {
		key = string(int(id))
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(msgBytes),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("❌ Kafka Error: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send event"})
		return
	}

	log.Printf("📤 Sent to %s [P:%d O:%d]: %s", topic, partition, offset, string(msgBytes))

	c.JSON(http.StatusCreated, gin.H{"status": "success"})
}

func setupProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	var producer sarama.SyncProducer
	var err error
	for i := 0; i < 15; i++ {
		producer, err = sarama.NewSyncProducer(brokers, config)
		if err == nil {
			return producer, nil
		}
		log.Println("Waiting for Kafka...")
		time.Sleep(2 * time.Second)
	}
	return nil, err
}

func startConsumer(brokers []string, topics []string) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	var master sarama.Consumer
	var err error
	for i := 0; i < 15; i++ {
		master, err = sarama.NewConsumer(brokers, config)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Printf("Consumer failed to start: %v", err)
		return
	}
	defer master.Close()

	for _, topic := range topics {
		partitions, _ := master.Partitions(topic)
		for _, partition := range partitions {
			pc, err := master.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				continue
			}
			go func(pc sarama.PartitionConsumer) {
				defer pc.Close()
				for message := range pc.Messages() {
					log.Printf("📥 Event Received: Topic=%s Value=%s", message.Topic, string(message.Value))
				}
			}(pc)
		}
	}
	select {}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
