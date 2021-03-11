package main

import (
	"bufio"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/titanium-iridium/epam-golang-test-task/common"
	"log"
	"os"
	"os/signal"
	"time"
)

var appConfig *common.Config

func main() {
	// customize logger
	log.SetFlags(0)
	log.SetOutput(new(common.LogWriter))

	// get app configuration
	var err error
	appConfig, err = common.GetConfig()
	if err != nil {
		os.Exit(1)
	}

	// init Kafka producer
	producer, err := initProducer()
	if err != nil {
		common.LogError("Failed to init producer", err)
		os.Exit(1)
	}

	// deferred resource cleanup
	defer func() {
		err := producer.Close()
		if err != nil {
			common.LogError("Failed to close producer", err)
			os.Exit(1)
		}
		log.Println("Done")
	}()

	// trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	// signal channel for exit
	exitChannel := make(chan struct{})

	// run main loop in goroutine
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			// read user input from console
			timeStamp := time.Now().Format(common.DateTimeFormat)
			fmt.Printf("[%s] Message for topic \"%s\": ", timeStamp, appConfig.Topic)
			//log.Printf("Message for topic \"%s\": ", appConfig.Topic)
			userInput, err := reader.ReadString('\n')

			if err != nil {
				common.LogError("Failed to read user input: ", err)
			} else {
				message := &sarama.ProducerMessage{Topic: appConfig.Topic, Value: sarama.StringEncoder(userInput)}

				select {
				case producer.Input() <- message: // publish message
				case err := <-producer.Errors(): // producer error(s)
					common.LogError("Failed to produce message", err)
				case <-signals: // exit signalled
					log.Println("Exit signalled")
					exitChannel <- struct{}{}
				}
			}
		}
	}()

	<-exitChannel
}

func initProducer() (sarama.AsyncProducer, error) {
	// use default logger for producer
	sarama.Logger = log.Default()

	// producer common
	producerConfig := sarama.NewConfig()
	producerConfig.ClientID = "test-golang-producer"
	producerConfig.Producer.Retry.Max = 3
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll

	return sarama.NewAsyncProducer(appConfig.Brokers, producerConfig)
}
