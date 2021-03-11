package main

import (
	"github.com/titanium-iridium/epam-golang-test-task/common"
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

var appConfig *common.Config

func main() {
	log.SetFlags(0)
	log.SetOutput(new(common.LogWriter))

	// get app configuration
	var err error
	appConfig, err = common.GetConfig()
	if err != nil {
		os.Exit(1)
	}

	master, err := initConsumer()
	if err != nil {
		common.LogError("Failed to init consumer", err)
		os.Exit(1)
	}

	// deferred resource cleanup
	defer func() {
		err := master.Close()
		if err != nil {
			common.LogError("Failed to close consumer", err)
			os.Exit(1)
		}
	}()

	// create partition consumer
	consumer, err := master.ConsumePartition(appConfig.Topic, 0, sarama.OffsetOldest)
	if err != nil {
		common.LogError("Failed to choose a partition", err)
		os.Exit(1)
	}

	log.Println("Consumer initialized")

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	// Signal channel for exit
	exitChannel := make(chan struct{})

	// run main loop in goroutine
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				common.LogError("Consumer error", err)
			case msg := <-consumer.Messages():
				msgTime := msg.Timestamp.Local().Format(common.DateTimeFormat)
				log.Printf("Message #%d (produced at %s): %s", msg.Offset, msgTime, string(msg.Value))
			case <-signals:
				log.Println("Exit signalled")
				exitChannel <- struct{}{}
			}
		}
	}()

	<-exitChannel
}

func initConsumer() (sarama.Consumer, error) {
	// use default logger for consumer
	sarama.Logger = log.Default()

	consumerConfig := sarama.NewConfig()
	consumerConfig.ClientID = "test-golang-consumer"
	consumerConfig.Consumer.Return.Errors = true

	// create new consumer
	cons, err := sarama.NewConsumer(appConfig.Brokers, consumerConfig)
	if err != nil {
		common.LogError("Failed to create message consumer", err)
	}

	return cons, err
}
