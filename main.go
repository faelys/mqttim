package main

import (
	"database/sql"
	"errors"
	"fmt"
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
	SendStart  string
	SendMid    string
	SendEnd    string
	ShowStart  string
	ShowMid    string
	ShowEnd    string
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
	Server    string
	Session   string
	UserName  string
	Password  string
	TLS       bool
	Keepalive int
	Topics    []string
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

type command struct {
	name string
	arg  string
}

func readConfig(path string) Config {
	config := Config{
		Irc: IrcConfig{
			Nick:     "mqttim",
			CmdStart: "!",
			CmdMid:   " ",
			SendMid:  ": ",
			ShowMid:  ": ",
		},
		Mqtt: MqttConfig{
			Topics: []string{"#"},
		},
	}

	f, err := os.Open("mqttim.toml")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	d := toml.NewDecoder(f)
	err = d.Decode(&config)
	if err != nil {
		log.Fatal(err)
	}

	if config.Irc.MaxLine > 0 && len(config.Irc.ContPrefix)+len(config.Irc.ContSuffix) >= config.Irc.MaxLine {
		config.Irc.ContPrefix = ""
		config.Irc.ContSuffix = ""
	}

	return config
}

func dialer(config Config) mqtt.Dialer {
	if config.Mqtt.TLS {
		return mqtt.NewTLSDialer("tcp", config.Mqtt.Server, nil)
	} else {
		return mqtt.NewDialer("tcp", config.Mqtt.Server)
	}
}

func main() {
	var err error
	var m *mqtt.Client
	var l *mqttLogger

	cmdQueue := make(chan command, 10)
	ircQueue := make(chan Msg, 10)

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
		Dialer:       dialer(config),
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
		if strings.HasPrefix(msg, config.Irc.CmdStart) && strings.HasSuffix(msg, config.Irc.CmdEnd) {
			msg = msg[len(config.Irc.CmdStart) : len(msg)-len(config.Irc.CmdEnd)]
			name, arg, found := strings.Cut(msg, config.Irc.CmdMid)
			if !found {
				name = msg
				arg = ""
			}
			if name == "send" {
				msg = arg
			} else {
				cmdQueue <- command{name: name, arg: arg}
				return
			}
		}
		if !strings.HasPrefix(msg, config.Irc.SendStart) || !strings.HasSuffix(msg, config.Irc.SendEnd) {
			return
		}
		msg = msg[len(config.Irc.SendStart) : len(msg)-len(config.Irc.SendEnd)]
		topic, payload, found := strings.Cut(msg, config.Irc.SendMid)
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
	go subscribeAll(m, ircQueue, config.Mqtt.Topics)
	go mqttReader(m, l, ircQueue, &config)
	go ircSender(&config.Irc, i, ircQueue, cmdQueue)
	go mqttKeepalive(m, ircQueue, config.Mqtt.Keepalive)
	i.Loop()
}

func dup(src []byte) []byte {
	res := make([]byte, len(src))
	copy(res, src)
	return res
}

func mqttKeepalive(m *mqtt.Client, c chan<- Msg, keepalive int) {
	if keepalive <= 0 {
		return
	}

	period := time.Duration(keepalive) * time.Second

	for {
		time.Sleep(period)

		if err := m.Ping(nil); err != nil {
			c <- errMsg("mqttPing", err)
		}
	}
}

func mqttReader(m *mqtt.Client, l *mqttLogger, c chan<- Msg, config *Config) {
	var big *mqtt.BigMessage

	for {
		message, topic, err := m.ReadSlices()
		switch {
		case err == nil:
			msg := Msg{Topic: dup(topic), Message: dup(message)}
			logReceived(l, msg.Message, msg.Topic)
			c <- msg

		case errors.As(err, &big):
			msg := Msg{Topic: dup(topic), Message: []byte("<Big Message>")}
			logReceived(l, msg.Message, msg.Topic)
			c <- msg

		case errors.Is(err, mqtt.ErrClosed):
			log.Println("mqttReader finishing:", err)
			return

		case mqtt.IsConnectionRefused(err):
			c <- errMsg("mqttReader2", err)
			time.Sleep(5 * time.Minute)

		default:
			c <- errMsg("mqttReader", err)
			time.Sleep(2 * time.Second)
		}
	}
}

func ircSender(config *IrcConfig, i *irc.Connection, cm <-chan Msg, cc <-chan command) {
	var buf strings.Builder
	f := createTopicFilter(config)

	for {
		select {
		case m := <-cm:
			if !isFiltered(&f, m.Topic) {
				str := config.ShowStart +
					string(m.Topic) +
					config.ShowMid +
					string(m.Message) +
					config.ShowEnd
				ircSend(config, i, str, &buf)
			}
		case cmd := <-cc:
			switch cmd.name {
			case "filters":
				ircSendFilters(config, i, &f, &buf)
			case "ignore":
				filterAddIgnored(&f, cmd.arg)
			case "important":
				filterAddImportant(&f, cmd.arg)
			case "quit":
				log.Println("Quit command", cmd.arg)
				i.QuitMessage = cmd.arg
				i.Quit()
			case "unignore":
				filterDelIgnored(&f, cmd.arg)
			case "unimportant":
				filterDelImportant(&f, cmd.arg)
			default:
				ircSend(config, i, "Unknown command: "+cmd.name, &buf)
			}
		}
	}
}

func ircSend(config *IrcConfig, i *irc.Connection, s string, buf *strings.Builder) {
	if config.MaxLine <= 0 || len(s) < config.MaxLine {
		i.Privmsg(config.Channel, s)
	} else {
		for offset := 0; offset < len(s); {
			l := len(s) - offset
			buf.Reset()
			if offset > 0 {
				buf.WriteString(config.ContPrefix)
			}

			if buf.Len()+l <= config.MaxLine {
				buf.WriteString(s[offset:])
			} else {
				l = config.MaxLine - buf.Len() - len(config.ContSuffix)
				buf.WriteString(s[offset : offset+l])
				buf.WriteString(config.ContSuffix)
			}

			i.Privmsg(config.Channel, buf.String())
			offset += l
		}
	}
}

func ircSendTopicList(config *IrcConfig, i *irc.Connection, name string, topics [][]string, buf *strings.Builder) {
	if len(topics) >= 2 {
		ircSend(config, i, name+" = [", buf)
		for index, topic := range topics {
			suffix := ","
			if index == len(topics)-1 {
				suffix = " ]"
			}
			ircSend(config, i, fmt.Sprintf("  %q%s", strings.Join(topic, "/"), suffix), buf)
		}
	} else if len(topics) == 1 {
		ircSend(config, i, fmt.Sprintf("%s = [%q]", name, strings.Join(topics[0], "/")), buf)
	} else {
		ircSend(config, i, name+" = []", buf)
	}
}

func ircSendFilters(config *IrcConfig, i *irc.Connection, f *mqttTopicFilter, buf *strings.Builder) {
	ircSendTopicList(config, i, "important", f.important, buf)
	ircSendTopicList(config, i, "ignored", f.ignored, buf)
}

func subscribeAll(m *mqtt.Client, ircQueue chan<- Msg, topics []string) {
	for _, topic := range topics {
		for {
			err := m.Subscribe(nil, topic)

			if err != nil {
				ircQueue <- errMsg("Subscribe", err)
				time.Sleep(1 * time.Minute)
			} else {
				break
			}
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

func addPattern(topics [][]string, newPattern string) [][]string {
	return append(topics, strings.Split(newPattern, "/"))
}

func delPattern(topics [][]string, toRemove string) [][]string {
	var result [][]string

	for _, pat := range topics {
		if strings.Join(pat, "/") != toRemove {
			result = append(result, pat)
		}
	}

	return result
}

func createTopicFilter(config *IrcConfig) mqttTopicFilter {
	result := mqttTopicFilter{
		ignored:   make([][]string, len(config.Ignored)),
		important: make([][]string, len(config.Important)),
	}

	for i, s := range config.Ignored {
		result.ignored[i] = strings.Split(s, "/")
	}

	for i, s := range config.Important {
		result.important[i] = strings.Split(s, "/")
	}

	return result
}

func filterAddIgnored(filter *mqttTopicFilter, pattern string) {
	filter.ignored = addPattern(filter.ignored, pattern)
}

func filterAddImportant(filter *mqttTopicFilter, pattern string) {
	filter.important = addPattern(filter.important, pattern)
}

func filterDelIgnored(filter *mqttTopicFilter, pattern string) {
	filter.ignored = delPattern(filter.ignored, pattern)
}

func filterDelImportant(filter *mqttTopicFilter, pattern string) {
	filter.important = delPattern(filter.important, pattern)
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
