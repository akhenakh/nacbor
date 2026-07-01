package main

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile string
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

const longDesc = `nacbor is a CLI for inspecting and manipulating CBOR-encoded data stored in
NATS, NATS JetStream streams, and NATS KV buckets.

Values are decoded using the same CBOR encoder/decoder options as the Bento
` + "`cbor`" + ` processor, so data written by Bento pipelines round-trips cleanly.

Auth can be supplied via NKEY seed (--nkey), creds file (--creds),
user/password, or token. All flags can also be set via NATSBOR_* environment
variables or a config file.
`

var rootCmd = &cobra.Command{
	Use:   "nacbor",
	Short: "Interact with NATS JetStream/KV using CBOR-encoded data",
	Long:  longDesc,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initConfig()
	},
	SilenceUsage:  true,
	SilenceErrors: true,
}

func Execute() error {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "nacbor: %v\n", err)
		return err
	}
	return nil
}

func init() {
	pf := rootCmd.PersistentFlags()

	pf.StringVarP(&cfgFile, "config", "", "", "config file (default $HOME/.nacbor.yaml)")
	pf.StringP("server", "s", "nats://localhost:4222", "NATS server URL")
	pf.String("nkey", "", "path to NKEY seed file for authentication")
	pf.String("creds", "", "path to NATS credentials (JWT+seed) file")
	pf.String("user", "", "NATS username")
	pf.String("password", "", "NATS password")
	pf.String("token", "", "NATS bearer token")
	pf.String("tls-cert", "", "client TLS certificate file")
	pf.String("tls-key", "", "client TLS private key file")
	pf.String("tls-ca", "", "TLS CA bundle file for verifying the server")
	pf.Bool("insecure", false, "skip TLS certificate verification")
	pf.Duration("timeout", 10*time.Second, "connection timeout")
	pf.String("log-level", "warn", "log level (debug, info, warn, error)")

	// Global output flags.
	pf.Bool("raw", false, "do not decode CBOR; emit raw bytes / base64")
	pf.BoolP("pretty", "p", false, "pretty-print JSON output")

	for _, key := range []string{
		"server", "nkey", "creds", "user", "password", "token",
		"tls-cert", "tls-key", "tls-ca", "insecure", "timeout",
		"log-level", "raw", "pretty",
	} {
		_ = viper.BindPFlag(key, pf.Lookup(key))
	}
}

func initConfig() error {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		if home, err := os.UserHomeDir(); err == nil {
			viper.AddConfigPath(home)
		}
		viper.AddConfigPath(".")
		viper.SetConfigName(".nacbor")
		viper.SetConfigType("yaml")
	}

	viper.SetEnvPrefix("NATSBOR")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return fmt.Errorf("reading config: %w", err)
		}
	}

	level := slog.LevelInfo
	switch strings.ToLower(viper.GetString("log-level")) {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	return nil
}