package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hokaccha/go-prettyjson"
	"github.com/imZack/sparkplug-lens/internal/spb"
	"google.golang.org/protobuf/proto"
)

var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("-------- %s --------\n", msg.Topic())
	spbPayload := &spb.Payload{}
	if err := proto.Unmarshal(msg.Payload(), spbPayload); err != nil {
		fmt.Println(err)
	}

	if jsonString, err := prettyjson.Marshal(spbPayload); err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(jsonString))
	}
	fmt.Println("======================================")
}

func main() {
	// mqtt.DEBUG = log.New(os.Stdout, "", 0)
	// mqtt.ERROR = log.New(os.Stdout, "", 0)
	brokerEndpoint := os.Getenv("MQTT_BROKER")
	if brokerEndpoint == "" {
		brokerEndpoint = "tcp://test.mosquitto.org:1883"
		fmt.Println("Using default broker endpoint: ", brokerEndpoint)
	} else {
		fmt.Println("Using broker endpoint: ", brokerEndpoint)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(brokerEndpoint).
		SetKeepAlive(30 * time.Second).
		SetDefaultPublishHandler(f).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			fmt.Println("Connection lost: ", err)
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			fmt.Println("Connected")
		})

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	if token := c.Subscribe("spBv1.0/#", 0, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	fmt.Println("Start listening...")
	fmt.Println("Press Ctrl+C to exit")

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
}
