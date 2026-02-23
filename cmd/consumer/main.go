package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	thinrsmq "github.com/your-org/thin-redis-queue/thinrsmq-go/pkg/thinrsmq"
)

func main() {
	// Parse flags
	redisAddr := flag.String("redis", getEnv("REDIS_HOST", "localhost")+":"+getEnv("REDIS_PORT", "6379"), "Redis address (host:port)")
	password := flag.String("password", os.Getenv("REDIS_PASSWORD"), "Redis password")
	useTLS := flag.Bool("tls", getEnvBool("REDIS_USE_TLS", false), "Enable TLS")
	namespace := flag.String("ns", "", "Namespace (required)")
	topic := flag.String("topic", "", "Topic (required)")
	group := flag.String("group", "", "Consumer group (required)")
	failRate := flag.Float64("fail-rate", 0.0, "Fraction [0,1] of messages to randomly fail")
	processTime := flag.Duration("process-time", 0, "Simulated processing time (e.g., 2s)")
	enableMonitor := flag.Bool("monitor", true, "Enable embedded claim monitor")
	drain := flag.Bool("drain", false, "Process all pending then exit")
	timeout := flag.Int("timeout", 5000, "Timeout in ms (for drain mode)")

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

	if *group == "" {
		fmt.Fprintln(os.Stderr, "Error: --group is required")
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

	// Create config
	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = *namespace
	cfg.Consumer.BlockTimeoutMs = int64(*timeout)

	// Setup signal handling for graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Message counter for drain mode
	msgCount := 0
	lastMsgTime := time.Now()

	// Create handler
	handler := func(ctx context.Context, msg *thinrsmq.Message) error {
		timestamp := time.Now().Format(time.RFC3339)
		fmt.Printf("[%s] <- %s | %s | %s\n", timestamp, msg.ID, msg.Type, msg.Payload)

		msgCount++
		lastMsgTime = time.Now()

		// Simulate processing time
		if *processTime > 0 {
			time.Sleep(*processTime)
		}

		// Random failure simulation
		if *failRate > 0 && rand.Float64() < *failRate {
			err := fmt.Errorf("simulated error (fail-rate=%.2f)", *failRate)
			fmt.Printf("[%s] FAIL %s: %v\n", timestamp, msg.ID, err)
			return err
		}

		fmt.Printf("[%s] ACK %s\n", timestamp, msg.ID)
		return nil
	}

	// Create consumer
	consumer := thinrsmq.NewConsumer(client, cfg)

	// Subscribe
	if err := consumer.Subscribe(*topic, *group, handler); err != nil {
		fmt.Fprintf(os.Stderr, "Error subscribing: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("[%s] Joined group '%s' on stream '%s:%s'\n",
		time.Now().Format(time.RFC3339), *group, *namespace, *topic)

	// Start monitor if enabled
	var monitor *thinrsmq.ClaimMonitor
	if *enableMonitor {
		monitor = thinrsmq.NewClaimMonitor(client, cfg, handler)
		if err := monitor.Start(*topic, *group); err != nil {
			fmt.Fprintf(os.Stderr, "Error starting monitor: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[%s] Claim monitor started\n", time.Now().Format(time.RFC3339))
	}

	// Drain mode: wait for no new messages within timeout
	if *drain {
		drainTimeout := time.Duration(*timeout) * time.Millisecond
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				goto shutdown
			case <-ticker.C:
				if time.Since(lastMsgTime) > drainTimeout {
					fmt.Printf("[%s] Drain complete: no new messages for %v\n",
						time.Now().Format(time.RFC3339), drainTimeout)
					goto shutdown
				}
			}
		}
	} else {
		// Wait for interrupt
		<-ctx.Done()
	}

shutdown:
	// Graceful shutdown
	fmt.Printf("[%s] Shutting down gracefully...\n", time.Now().Format(time.RFC3339))

	if monitor != nil {
		monitor.Stop()
	}

	if err := consumer.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: shutdown error: %v\n", err)
	}

	fmt.Printf("[%s] Shutdown complete (processed %d messages)\n",
		time.Now().Format(time.RFC3339), msgCount)
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
