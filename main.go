package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hokaccha/go-prettyjson"
	"github.com/imZack/sparkplug-lens/internal/spb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

func DecodeAsSPB(msg mqtt.Message) (*spb.Payload, error) {
	payload := &spb.Payload{}
	if err := proto.Unmarshal(msg.Payload(), payload); err != nil {
		return nil, err
	}

	return payload, nil
}

var f mqtt.MessageHandler = func(_ mqtt.Client, msg mqtt.Message) {
	var jsonString []byte
	var spbPayload *spb.Payload
	if spbPayload, err := DecodeAsSPB(msg); err == nil {
		// 1. Try decode as SPB payload -> Print as JSON
		jsonString, err = prettyjson.Marshal(spbPayload)
		if err != nil {
			log.Info("error", err)
			return
		}
	} else {
		// 2. Try decode as JSON payload
		if jsonString, err = prettyjson.Format(msg.Payload()); err != nil {
			log.Info("error", err)
			return
		}
	}

	log.WithFields(log.Fields{
		"topic": msg.Topic(),
		"size":  len(msg.Payload()),
		"isSPB": spbPayload != nil,
	}).Info(string(jsonString))
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	brokerEndpoint := os.Getenv("MQTT_BROKER")
	if brokerEndpoint == "" {
		brokerEndpoint = "tcp://test.mosquitto.org:1883"
		log.Info("Using default broker endpoint: ", brokerEndpoint)
	} else {
		log.Info("Using broker endpoint: ", brokerEndpoint)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(brokerEndpoint).
		SetKeepAlive(30 * time.Second).
		SetDefaultPublishHandler(f).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Warn("[SetConnectionLostHandler] Connection lost: ", err)
		}).
		SetOnConnectHandler(func(_ mqtt.Client) {
			log.Info("[SetOnConnectHandler] Connected")
		})

	if os.Getenv("MQTT_USERNAME") != "" {
		log.Info("Using username: ", os.Getenv("MQTT_USERNAME"))
		opts.SetUsername(os.Getenv("MQTT_USERNAME"))
	}

	if os.Getenv("MQTT_PASSWORD") != "" {
		log.Info("Using password: **********")
		opts.SetPassword(os.Getenv("MQTT_PASSWORD"))
	}

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		log.Panic(token.Error())
	}

	if token := c.Subscribe("spBv1.0/#", 0, nil); token.Wait() && token.Error() != nil {
		log.Error(token.Error())
		os.Exit(1)
	}

	log.Info("Start listening...")
	log.Info("Press Ctrl+C to exit")

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	<-done
}
