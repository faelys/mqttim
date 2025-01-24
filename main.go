package main

import (
	"bytes"
	"database/sql"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/go-mqtt/mqtt"
	"github.com/pelletier/go-toml/v2"
	"github.com/thoj/go-ircevent"
)

type IrcConfig struct {
	Channel    string
	Server     string
	Nick       string
	CmdStart   string
	CmdMid     string
	CmdEnd     string
	MaxLine    int
	ContSuffix string
	ContPrefix string
	Ignored    []string
	Important  []string
	Verbose    bool
}

type LogConfig struct {
	SqlDriver     string
	SqlConnection string
}

type MqttConfig struct {
	Server   string
	Session  string
	UserName string
	Password string
}

type Config struct {
	Irc  IrcConfig
	Log  LogConfig
	Mqtt MqttConfig
}

type Msg struct {
	Topic   []byte
	Message []byte
}

func errMsg(context string, err error) Msg {
	return Msg{
		Topic:   []byte("$mqttim/" + context),
		Message: []byte(err.Error()),
	}
}

func readConfig(path string) Config {
	var config Config

	f, err := os.Open("mqttim.toml")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	d := toml.NewDecoder(f)
	err = d.Decode(config)
	if err != nil {
		log.Fatal(err)
	}

	return config
}

func main() {
	var err error
	var m *mqtt.Client
	var l *mqttLogger

	ircQueue := make(chan Msg)

	config := readConfig("mqttim.toml")

	if len(config.Log.SqlDriver) > 0 {
		db, err := sql.Open(config.Log.SqlDriver, config.Log.SqlConnection)
		if err != nil {
			log.Fatal("sql.Open:", err)
		}

		l, err = logInit(db)
		if err != nil {
			log.Fatal("logInit:", err)
		}
		log.Println("Logging into", config.Log.SqlConnection)
	}

	m, err = mqtt.VolatileSession(config.Mqtt.Session, &mqtt.Config{
		Dialer:       mqtt.NewDialer("tcp", config.Mqtt.Server),
		PauseTimeout: 4 * time.Second,
		UserName:     config.Mqtt.UserName,
		Password:     []byte(config.Mqtt.Password),
	})
	if err != nil {
		log.Fatal("mqtt.VolatileSession:", err)
	}

	i := irc.IRC(config.Irc.Nick, "mqttim")
	if config.Irc.Verbose {
		i.VerboseCallbackHandler = true
		i.Debug = true
	}
	i.AddCallback("001", func(e *irc.Event) { i.Join(config.Irc.Channel) })
	i.AddCallback("366", func(e *irc.Event) {})
	i.AddCallback("PRIVMSG", func(e *irc.Event) {
		msg := e.Message()
		if !strings.HasPrefix(msg, config.Irc.CmdStart) || !strings.HasSuffix(msg, config.Irc.CmdEnd) {
			return
		}
		topic, payload, found := strings.Cut(msg, config.Irc.CmdMid)
		if found {
			logSent(l, []byte(payload), []byte(topic))
			if err := m.Publish(nil, []byte(payload), topic); err != nil {
				ircQueue <- errMsg("Publish", err)
			}
		}
	})
	err = i.Connect(config.Irc.Server)
	if err != nil {
		log.Fatal("irc.Connect:", err)
	}
	go subscribeAll(m, ircQueue)
	go mqtt2irc(m, l, createTopicFilter(&config), ircQueue, &config)
	go ircSender(&config.Irc, i, ircQueue)
	i.Loop()
}

func dup(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}

func mqtt2irc(m *mqtt.Client, l *mqttLogger, f mqttTopicFilter, c chan<- Msg, config *Config) {
	var big *mqtt.BigMessage

	for {
		message, topic, err := m.ReadSlices()
		switch {
		case err == nil:
			msg := Msg{Topic: dup(topic), Message: dup(message)}
			logReceived(l, msg.Message, msg.Topic)
			if !isFiltered(&f, topic) {
				c <- msg
			}

		case errors.As(err, &big):
			msg := Msg{Topic: dup(topic), Message: []byte("<Big Message>")}
			logReceived(l, msg.Message, msg.Topic)
			if !isFiltered(&f, topic) {
				c <- msg
			}

		case errors.Is(err, mqtt.ErrClosed):
			log.Println("mqtt2irc finishing:", err)
			return

		case mqtt.IsConnectionRefused(err):
			c <- errMsg("mqtt2irc2", err)
			time.Sleep(5 * time.Minute)

		default:
			c <- errMsg("mqtt2irc", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func ircSender(config *IrcConfig, i *irc.Connection, c <-chan Msg) {
	var buf bytes.Buffer

	for {
		m := <-c

		if len(m.Topic)+2+len(m.Message) < config.MaxLine {
			i.Privmsgf(config.Channel, "%s: %s", m.Topic, m.Message)
		} else {
			for s := 0; s < len(m.Message); {
				l := len(m.Message) - s
				buf.Reset()
				buf.Write(m.Topic)
				buf.WriteString(": ")
				if s > 0 {
					buf.WriteString(config.ContPrefix)
				}
				if buf.Len()+l <= config.MaxLine {
					buf.Write(m.Message[s:])
				} else {
					l = config.MaxLine - buf.Len() - len(config.ContSuffix)
					buf.Write(m.Message[s : s+l])
					buf.WriteString(config.ContSuffix)
				}
				i.Privmsg(config.Channel, buf.String())
				s += l
			}
		}
	}
}

func subscribeAll(m *mqtt.Client, ircQueue chan<- Msg) {
	for {
		err := m.Subscribe(nil, "#")

		if err != nil {
			ircQueue <- errMsg("Subscribe", err)
			time.Sleep(1 * time.Minute)
		} else {
			return
		}
	}
}

/**************** MQTT Logger Into SQL ****************/

type mqttLogger struct {
	db             *sql.DB
	getTopic       *sql.Stmt
	insertReceived *sql.Stmt
	insertSent     *sql.Stmt
	insertTopic    *sql.Stmt
}

func logClose(l *mqttLogger) {
	if l.insertTopic != nil {
		l.insertTopic.Close()
		l.insertTopic = nil
	}
	if l.insertSent != nil {
		l.insertSent.Close()
		l.insertSent = nil
	}
	if l.insertReceived != nil {
		l.insertReceived.Close()
		l.insertReceived = nil
	}
	if l.getTopic != nil {
		l.getTopic.Close()
		l.getTopic = nil
	}
	if l.db != nil {
		l.db.Close()
		l.db = nil
	}
}

func logInit(db *sql.DB) (*mqttLogger, error) {
	var err error
	result := mqttLogger{db: db}

	for _, cmd := range []string{
		"CREATE TABLE IF NOT EXISTS topics" +
			"(id INTEGER PRIMARY KEY AUTOINCREMENT," +
			" name TEXT NOT NULL);",
		"CREATE UNIQUE INDEX IF NOT EXISTS i_topics ON topics(name);",
		"CREATE TABLE IF NOT EXISTS received" +
			"(timestamp REAL NOT NULL," +
			" topic_id INTEGER NOT NULL," +
			" message TEXT NOT NULL," +
			" FOREIGN KEY (topic_id) REFERENCES topics (id));",
		"CREATE TABLE IF NOT EXISTS sent" +
			"(timestamp REAL NOT NULL," +
			" topic_id INTEGER NOT NULL," +
			" message TEXT NOT NULL," +
			" FOREIGN KEY (topic_id) REFERENCES topics (id));",
		"CREATE INDEX IF NOT EXISTS i_rtime ON received(timestamp);",
		"CREATE INDEX IF NOT EXISTS i_rtopicid ON received(topic_id);",
		"CREATE INDEX IF NOT EXISTS i_stime ON sent(timestamp);",
		"CREATE INDEX IF NOT EXISTS i_stopicid ON sent(topic_id);",
	} {
		if _, err = result.db.Exec(cmd); err != nil {
			logClose(&result)
			return nil, err
		}
	}

	result.getTopic, err = db.Prepare("SELECT id FROM topics WHERE name=?;")
	if err != nil {
		logClose(&result)
		return nil, err
	}

	result.insertTopic, err = db.Prepare("INSERT OR IGNORE INTO topics(name) VALUES (?);")
	if err != nil {
		logClose(&result)
		return nil, err
	}

	result.insertSent, err = db.Prepare("INSERT INTO sent (timestamp, topic_id, message) VALUES (?, ?, ?);")
	if err != nil {
		logClose(&result)
		return nil, err
	}

	result.insertReceived, err = db.Prepare("INSERT INTO received (timestamp, topic_id, message) VALUES (?, ?, ?);")
	if err != nil {
		logClose(&result)
		return nil, err
	}

	return &result, nil
}

func logMessage(l *mqttLogger, stmt *sql.Stmt, message, topic []byte) {
	t := float64(time.Now().UnixNano())/8.64e13 + 2440587.5

	id, err := logTopic(l, topic)
	if err != nil {
		log.Println("logTopic:", err)
		logClose(l)
		return
	}

	_, err = stmt.Exec(t, id, message)
	if err != nil {
		log.Println("logMessage:", err)
		logClose(l)
		return
	}
}

func logReceived(l *mqttLogger, message, topic []byte) {
	if l == nil || l.db == nil {
		return
	}

	logMessage(l, l.insertReceived, message, topic)
}

func logSent(l *mqttLogger, message, topic []byte) {
	if l == nil || l.db == nil {
		return
	}

	logMessage(l, l.insertSent, message, topic)
}

func logTopic(l *mqttLogger, topic []byte) (int64, error) {
	var id int64
	err := l.getTopic.QueryRow(topic).Scan(&id)

	if err == sql.ErrNoRows {
		res, err := l.insertTopic.Exec(topic)

		if err != nil {
			return 0, err
		}

		return res.LastInsertId()
	} else if err != nil {
		return 0, err
	}

	return id, nil
}

/**************** MQTT Topic Filter ****************/

type mqttTopicFilter struct {
	ignored   [][]string
	important [][]string
}

func createTopicFilter(config *Config) mqttTopicFilter {
	result := mqttTopicFilter{
		ignored:   make([][]string, len(config.Irc.Ignored)),
		important: make([][]string, len(config.Irc.Important)),
	}

	for i, s := range config.Irc.Ignored {
		result.ignored[i] = strings.Split(s, "/")
	}

	for i, s := range config.Irc.Important {
		result.important[i] = strings.Split(s, "/")
	}

	return result
}

func isFiltered(filter *mqttTopicFilter, topic []byte) bool {
	t := strings.Split(string(topic), "/")

	for _, pattern := range filter.important {
		if topicMatch(t, pattern) {
			return false
		}
	}

	for _, pattern := range filter.ignored {
		if topicMatch(t, pattern) {
			return true
		}
	}

	return false
}

func topicMatch(actual, filter []string) bool {
	if len(filter) == 0 {
		return len(actual) == 0
	}

	if filter[0] == "#" {
		if len(filter) == 1 {
			return true
		}

		for i := range actual {
			if topicMatch(actual[i:], filter[1:]) {
				return true
			}
		}

		return false
	}

	if len(actual) > 0 && (filter[0] == "+" || filter[0] == actual[0]) {
		return topicMatch(actual[1:], filter[1:])
	}

	return false
}
