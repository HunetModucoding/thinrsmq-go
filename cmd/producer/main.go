package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
	thinrsmq "github.com/hunetmoducoding/thinrsmq-go/pkg/thinrsmq"
)

func main() {
	// Parse flags
	redisAddr := flag.String("redis", getEnv("REDIS_HOST", "localhost")+":"+getEnv("REDIS_PORT", "6379"), "Redis address (host:port)")
	password := flag.String("password", os.Getenv("REDIS_PASSWORD"), "Redis password")
	useTLS := flag.Bool("tls", getEnvBool("REDIS_USE_TLS", false), "Enable TLS")
	namespace := flag.String("ns", "", "Namespace (required)")
	topic := flag.String("topic", "", "Topic (required)")
	msgType := flag.String("type", "event", "Message type")
	version := flag.Int("version", 1, "Envelope version")

	var autoPayloads multiString
	flag.Var(&autoPayloads, "auto", "Send payloads and exit (can be repeated)")

	flag.Parse()

	// Validate required flags
	if *namespace == "" {
		fmt.Fprintln(os.Stderr, "Error: --ns (namespace) is required")
		flag.Usage()
		os.Exit(1)
	}

	if *topic == "" {
		fmt.Fprintln(os.Stderr, "Error: --topic is required")
		flag.Usage()
		os.Exit(1)
	}

	// Create Redis client options
	opts := &redis.Options{
		Addr:     *redisAddr,
		Password: *password,
	}

	// Enable TLS if requested
	if *useTLS {
		// Parse host for SNI
		host := strings.Split(*redisAddr, ":")[0]
		opts.TLSConfig = &tls.Config{
			ServerName: host,
		}
	}

	// Create Redis client
	client := redis.NewClient(opts)
	defer client.Close()

	// Test connection
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to connect to Redis at %s: %v\n", *redisAddr, err)
		os.Exit(1)
	}

	// Create config and producer
	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = *namespace

	producer := thinrsmq.NewProducer(client, cfg)

	// Auto mode: send args and exit
	if len(autoPayloads) > 0 {
		for _, payload := range autoPayloads {
			msg := thinrsmq.Message{
				Type:    *msgType,
				Payload: payload,
				Version: fmt.Sprintf("%d", *version),
			}

			id, err := producer.Publish(ctx, *topic, msg)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error publishing message: %v\n", err)
				os.Exit(1)
			}

			fmt.Printf("Published: %s\n", id)
		}
		return
	}

	// Interactive mode: read from stdin
	fmt.Printf("# Producer ready. Enter JSON payloads (one per line). Press Ctrl+C to exit.\n")
	fmt.Printf("# Publishing to: %s:%s (type=%s, version=%d)\n\n", *namespace, *topic, *msgType, *version)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		msg := thinrsmq.Message{
			Type:    *msgType,
			Payload: line,
			Version: fmt.Sprintf("%d", *version),
		}

		id, err := producer.Publish(ctx, *topic, msg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}

		fmt.Printf("Published: %s\n", id)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading stdin: %v\n", err)
		os.Exit(1)
	}
}

// multiString allows multiple occurrences of the same flag
type multiString []string

func (m *multiString) String() string {
	return strings.Join(*m, ",")
}

func (m *multiString) Set(value string) error {
	*m = append(*m, value)
	return nil
}

// getEnv returns the value of an environment variable or a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool returns the boolean value of an environment variable or a default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return value == "true" || value == "1" || value == "yes"
	}
	return defaultValue
}
