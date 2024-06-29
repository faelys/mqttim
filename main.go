package main

import (
	"errors"
	"fmt"
	"github.com/go-mqtt/mqtt"
	"github.com/pelletier/go-toml/v2"
	"github.com/thoj/go-ircevent"
	"log"
	"os"
	"strings"
	"time"
)

type IrcConfig struct {
	Channel  string
	Server   string
	Nick     string
	CmdStart string
	CmdMid   string
	CmdEnd   string
}

type MqttConfig struct {
	Server   string
	UserName string
	Password string
}

type Config struct {
	Irc  IrcConfig
	Mqtt MqttConfig
}

type Msg struct {
	Topic   []byte
	Message []byte
}

func readConfig(path string, config *Config) (err error) {
	var f *os.File
	f, err = os.Open("mqttim.toml")
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	d := toml.NewDecoder(f)
	err = d.Decode(config)
	if err != nil {
		log.Fatal(err)
	}

	return err
}

func main() {
	var err error
	var config Config
	var m *mqtt.Client

	ircQueue := make(chan Msg)

	err = readConfig("mqttim.toml", &config)
	if err != nil {
		return
	}

	m, err = mqtt.VolatileSession("mqttim", &mqtt.Config{
		Dialer:       mqtt.NewDialer("tcp", config.Mqtt.Server),
		PauseTimeout: 4 * time.Second,
		UserName:     config.Mqtt.UserName,
		Password:     []byte(config.Mqtt.Password),
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	go m.Subscribe(nil, "#")

	i := irc.IRC(config.Irc.Nick, "mqttim")
//	i.VerboseCallbackHandler = true
//	i.Debug = true
	i.AddCallback("001", func(e *irc.Event) { i.Join(config.Irc.Channel) })
	i.AddCallback("366", func(e *irc.Event) {})
	i.AddCallback("PRIVMSG", func(e *irc.Event) {
		msg := e.Message()
		if !strings.HasPrefix(msg, config.Irc.CmdStart) || !strings.HasSuffix(msg, config.Irc.CmdEnd) {
			return
		}
		topic, payload, found := strings.Cut(msg, config.Irc.CmdMid)
		if found {
			go m.Publish(nil, []byte(payload), topic)
		}
	})
	err = i.Connect(config.Irc.Server)
	if err != nil {
		fmt.Printf("Err %s", err)
		return
	}
	go mqtt2irc(m, ircQueue, &config)
	go ircSender(&config.Irc, i, ircQueue)
	i.Loop()
}

func dup(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}

func mqtt2irc(m *mqtt.Client, c chan Msg, config *Config) error {
	var big *mqtt.BigMessage

	for {
		message, topic, err := m.ReadSlices()
		switch {
		case err == nil:
			c <- Msg{Topic: dup(topic), Message: dup(message)}
		case errors.As(err, &big):
			c <- Msg{Topic: dup(topic), Message: []byte("<Big Message>")}
		default:
			log.Print(err)
			return err
		}
	}
}

func ircSender(config *IrcConfig, i *irc.Connection, c chan Msg) error {
	for {
		m := <-c
		i.Privmsgf(config.Channel, "%s: %s", m.Topic, m.Message)
	}
}
