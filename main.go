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
	fmt.Printf("-------- %s -------- (%d bytes) \n", msg.Topic(), len(msg.Payload()))
	spbPayload := &spb.Payload{}
	if err := proto.Unmarshal(msg.Payload(), spbPayload); err == nil {
		if jsonString, err := prettyjson.Marshal(spbPayload); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(string(jsonString))
		}
		fmt.Println("======================================")
		return
	} else {
		// Can't decode spb payload, try to decode as JSON Object
		// Ignore error
	}

	// Check if msg.Payload() is a JSON string
	if jsonString, err := prettyjson.Format(msg.Payload()); err == nil {
		fmt.Println(string(jsonString))
	} else {
		fmt.Println(err)
	}

	fmt.Println("======================================")
}

func main() {
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
			fmt.Println("[SetConnectionLostHandler] Connection lost: ", err)
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			fmt.Println("[SetOnConnectHandler] Connected")
		})

	if os.Getenv("MQTT_USERNAME") != "" {
		fmt.Println("Using username: ", os.Getenv("MQTT_USERNAME"))
		opts.SetUsername(os.Getenv("MQTT_USERNAME"))
	}

	if os.Getenv("MQTT_PASSWORD") != "" {
		fmt.Println("Using password: **********")
		opts.SetPassword(os.Getenv("MQTT_PASSWORD"))
	}

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
