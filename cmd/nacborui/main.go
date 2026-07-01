package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/akhenakh/nacbor/internal/natsconn"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"charm.land/bubbles/v2/list"
	"charm.land/bubbles/v2/viewport"
	tea "charm.land/bubbletea/v2"
)

// setupFlags defines the same connection/auth flags as the nacbor CLI and
// binds them to viper so they can also be supplied via NATSBOR_* environment
// variables or the ~/.nacbor.yaml config file.
func setupFlags() {
	fs := pflag.NewFlagSet("nacborui", pflag.ContinueOnError)

	fs.StringP("server", "s", "nats://localhost:4222", "NATS server URL")
	fs.String("nkey", "", "path to NKEY seed file for authentication")
	fs.String("creds", "", "path to NATS credentials (JWT+seed) file")
	fs.String("user", "", "NATS username")
	fs.String("password", "", "NATS password")
	fs.String("token", "", "NATS bearer token")
	fs.String("tls-cert", "", "client TLS certificate file")
	fs.String("tls-key", "", "client TLS private key file")
	fs.String("tls-ca", "", "TLS CA bundle file for verifying the server")
	fs.Bool("insecure", false, "skip TLS certificate verification")
	fs.Duration("timeout", 10*time.Second, "connection timeout")

	if err := fs.Parse(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "nacborui: %v\n", err)
		fs.Usage()
		os.Exit(2)
	}

	for _, key := range []string{
		"server", "nkey", "creds", "user", "password", "token",
		"tls-cert", "tls-key", "tls-ca", "insecure", "timeout",
	} {
		_ = viper.BindPFlag(key, fs.Lookup(key))
	}
}

func initConfig() error {
	if home, err := os.UserHomeDir(); err == nil {
		viper.AddConfigPath(home)
	}
	viper.AddConfigPath(".")
	viper.SetConfigName(".nacbor")
	viper.SetConfigType("yaml")

	viper.SetEnvPrefix("NATSBOR")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	_ = viper.ReadInConfig()
	return nil
}

func main() {
	setupFlags()

	if err := initConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}

	cfg := natsconn.FromViper()
	nc, js, err := natsconn.JS(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nats connection error: %v\n", err)
		os.Exit(1)
	}
	defer nc.Drain()

	items := []list.Item{
		menuItem{title: "KV Buckets", desc: "Browse Key-Value store entries"},
		menuItem{title: "JetStream Streams", desc: "Browse recent stream messages"},
		menuItem{title: "NATS Core", desc: "Subscribe to a NATS subject"},
	}

	mList := list.New(items, list.NewDefaultDelegate(), 0, 0)
	mList.Title = "nacbor TUI"
	mList.DisableQuitKeybindings()

	bList := list.New(nil, list.NewDefaultDelegate(), 0, 0)
	bList.DisableQuitKeybindings()

	iList := list.New(nil, list.NewDefaultDelegate(), 0, 0)
	iList.DisableQuitKeybindings()

	m := appModel{
		nc:         nc,
		js:         js,
		state:      stateMainMenu,
		mainList:   mList,
		browseList: bList,
		itemList:   iList,
		vp:         viewport.New(),
		logCh:      make(chan string, 100),
	}

	p := tea.NewProgram(m)
	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error running TUI: %v\n", err)
		os.Exit(1)
	}
}
