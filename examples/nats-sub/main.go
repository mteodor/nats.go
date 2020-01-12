// Copyright 2012-2019 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/nats-io/nats.go"
)

// NOTE: Can test with demo servers.
// nats-sub -s demo.nats.io <subject>
// nats-sub -s demo.nats.io:4443 <subject> (TLS version)

const (
	password = "b7e6e968-ff42-413e-a360-9118c8bb90f0"
	username = "8cd9817e-55be-4e7b-8d6b-46897e9facc1"
	channel  = "cfb02ccf-6703-4603-a042-2271532801f4"
	topic    = "channels/cfb02ccf-6703-4603-a042-2271532801f4/messages"
	mqttHost = "tcp://localhost:18831"
)

var client mqtt.Client
var nc *nats.Conn
var sub *nats.Subscription
var i = 0

func usage() {
	log.Printf("Usage: nats-sub [-s server] [-creds file] [-t] <subject>\n")
	flag.PrintDefaults()
}

func showUsageAndExit(exitcode int) {
	usage()
	os.Exit(exitcode)
}

func printMsg(m *nats.Msg, i int) {
	log.Printf("[#%d] Received on [%s]: '%s'", i, m.Subject, string(m.Data))
	client.Publish(topic, 0, false, m.Data)
}

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var userCreds = flag.String("creds", "", "User Credentials File")
	var showTime = flag.Bool("t", false, "Display timestamps")
	var showHelp = flag.Bool("h", false, "Show help message")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *showHelp {
		showUsageAndExit(0)
	}

	args := flag.Args()
	if len(args) != 1 {
		showUsageAndExit(1)
	}

	// Connect Options.
	opts := []nats.Option{nats.Name("NATS Sample Subscriber")}
	opts = setupConnOptions(opts)

	// Use UserCredentials
	if *userCreds != "" {
		opts = append(opts, nats.UserCredentials(*userCreds))
	}

	// connect to mqtt
	client, _ = mqttConnect(args[0], *urls, opts...)
	// Connect to NATS
	nc, err := nats.Connect(*urls, opts...)
	if err != nil {
		log.Fatal(err)
	}

	subj, i := args[0], 0

	sub, err = nc.QueueSubscribe(subj, "test", func(msg *nats.Msg) {
		i += 1
		printMsg(msg, i)
	})
	nc.Flush()

	if err := nc.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening on [%s]", subj)
	if *showTime {
		log.SetFlags(log.LstdFlags)
	}
	errs := make(chan error)

	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT)
		errs <- fmt.Errorf("%s", <-c)
	}()

	err = <-errs

}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 10 * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to:%s, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected [%s]", nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		fmt.Println(fmt.Sprintf("Exiting: %v", nc.LastError()))
	}))
	return opts
}

func mqttConnect(subj, urls string, options ...nats.Option) (mqtt.Client, error) {
	conn := func(client mqtt.Client) {
		fmt.Println(fmt.Sprintf("Client %s connected", "EXPORT"))
		nc, _ = nats.Connect(urls, options...)
		sub, _ = nc.QueueSubscribe(subj, "test", func(msg *nats.Msg) {
			i += 1
			printMsg(msg, i)
		})
		nc.Flush()

		if err := nc.LastError(); err != nil {
			log.Fatal(err)
		}
	}

	lost := func(client mqtt.Client, err error) {
		fmt.Println(fmt.Sprintf("Client %s disconnected", "EXPORT"))
		sub.Unsubscribe()
		nc.Close()
	}

	opts := mqtt.NewClientOptions().
		AddBroker(mqttHost).
		SetClientID("EXPORT").
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetOnConnectHandler(conn).
		SetConnectionLostHandler(lost)

	if username != "" && password != "" {
		opts.SetUsername(username)
		opts.SetPassword(password)
	}

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		fmt.Println(fmt.Sprintf("Client %s had error connecting to the broker: %s\n", "EXPORT", token.Error().Error()))
		return nil, token.Error()
	}
	return client, nil
}
