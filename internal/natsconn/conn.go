// Package natsconn builds *nats.Conn and jetstream.JetStream instances from
// the global flags defined on the root command.
package natsconn

import (
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/viper"
)

// Config holds the connection options sourced from flags/env/config.
type Config struct {
	Server       string
	Nkey         string
	Creds        string
	User         string
	Password     string
	Token        string
	TLSCert      string
	TLSKey       string
	TLSCA        string
	TLSSkipTrust bool
	Timeout      time.Duration
}

// FromViper reads the global connection config from viper keys.
func FromViper() Config {
	return Config{
		Server:       viper.GetString("server"),
		Nkey:         viper.GetString("nkey"),
		Creds:        viper.GetString("creds"),
		User:         viper.GetString("user"),
		Password:     viper.GetString("password"),
		Token:        viper.GetString("token"),
		TLSCert:      viper.GetString("tls-cert"),
		TLSKey:       viper.GetString("tls-key"),
		TLSCA:        viper.GetString("tls-ca"),
		TLSSkipTrust: viper.GetBool("insecure"),
		Timeout:      viper.GetDuration("timeout"),
	}
}

// Connect builds a *nats.Conn honoring auth preferences in order:
// NKEY seed, creds file, then user/password, then token.
func Connect(cfg Config) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("nacbor"),
		nats.Timeout(cfg.Timeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			fmt.Fprintf(os.Stderr, "nacbor: reconnected to %s\n", nc.ConnectedUrl())
		}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				fmt.Fprintf(os.Stderr, "nacbor: disconnected: %v\n", err)
			}
		}),
	}

	switch {
	case cfg.Nkey != "":
		opt, err := nats.NkeyOptionFromSeed(cfg.Nkey)
		if err != nil {
			return nil, fmt.Errorf("loading NKEY seed %q: %w", cfg.Nkey, err)
		}
		opts = append(opts, opt)
	case cfg.Creds != "":
		opts = append(opts, nats.UserCredentials(cfg.Creds))
	case cfg.User != "" && cfg.Password != "":
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	case cfg.Token != "":
		opts = append(opts, nats.Token(cfg.Token))
	}

	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		opts = append(opts, nats.ClientCert(cfg.TLSCert, cfg.TLSKey))
	}
	if cfg.TLSCA != "" {
		opts = append(opts, nats.RootCAs(cfg.TLSCA))
	}
	if cfg.TLSSkipTrust {
		opts = append(opts, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	}

	nc, err := nats.Connect(cfg.Server, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to %q: %w", cfg.Server, err)
	}
	return nc, nil
}

// JS connects the underlying nats.Conn and returns a jetstream handle.
func JS(cfg Config) (*nats.Conn, jetstream.JetStream, error) {
	nc, err := Connect(cfg)
	if err != nil {
		return nil, nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Drain()
		return nil, nil, fmt.Errorf("create jetstream context: %w", err)
	}
	return nc, js, nil
}