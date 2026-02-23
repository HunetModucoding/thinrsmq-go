package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

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
	group := flag.String("group", "", "Consumer group (for pending/consumers commands)")
	jsonOutput := flag.Bool("json", false, "Output as JSON")

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

	// Parse subcommand
	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]

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

	// Create config and admin
	cfg := thinrsmq.DefaultConfig()
	cfg.Namespace = *namespace

	admin := thinrsmq.NewAdmin(client, cfg)

	// Execute command
	switch command {
	case "stats":
		handleStats(ctx, admin, *topic, *jsonOutput)

	case "pending":
		if *group == "" {
			fmt.Fprintln(os.Stderr, "Error: --group is required for 'pending' command")
			os.Exit(1)
		}
		handlePending(ctx, admin, *topic, *group, *jsonOutput)

	case "consumers":
		if *group == "" {
			fmt.Fprintln(os.Stderr, "Error: --group is required for 'consumers' command")
			os.Exit(1)
		}
		handleConsumers(ctx, admin, *topic, *group, *jsonOutput)

	case "dlq":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Error: dlq command requires a subcommand (size, peek, replay, purge)")
			os.Exit(1)
		}
		handleDLQ(ctx, admin, *topic, args[1:], *jsonOutput)

	default:
		fmt.Fprintf(os.Stderr, "Error: unknown command '%s'\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage: thinrsmq-admin [flags] <command>")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  stats              Show stream info (length, groups, first/last ID)")
	fmt.Println("  pending            Show pending summary for group (requires --group)")
	fmt.Println("  consumers          Show consumer details for group (requires --group)")
	fmt.Println("  dlq size           Show DLQ depth")
	fmt.Println("  dlq peek [n]       Show N DLQ entries (default: 10)")
	fmt.Println("  dlq replay [n]     Replay N entries from DLQ")
	fmt.Println("  dlq purge          Purge all DLQ entries")
	fmt.Println()
	fmt.Println("Flags:")
	flag.PrintDefaults()
}

func handleStats(ctx context.Context, admin *thinrsmq.Admin, topic string, jsonOut bool) {
	info, err := admin.StreamInfo(ctx, topic)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if jsonOut {
		data, _ := json.MarshalIndent(info, "", "  ")
		fmt.Println(string(data))
	} else {
		fmt.Printf("Stream Info:\n")
		fmt.Printf("  Length:    %d\n", info.Length)
		fmt.Printf("  Groups:    %d\n", info.Groups)
		fmt.Printf("  First ID:  %s\n", info.FirstID)
		fmt.Printf("  Last ID:   %s\n", info.LastID)
	}
}

func handlePending(ctx context.Context, admin *thinrsmq.Admin, topic, group string, jsonOut bool) {
	pending, err := admin.PendingStats(ctx, topic, group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if jsonOut {
		data, _ := json.MarshalIndent(pending, "", "  ")
		fmt.Println(string(data))
	} else {
		fmt.Printf("Pending Messages:\n")
		fmt.Printf("  Count:     %d\n", pending.Count)
		fmt.Printf("  Min ID:    %s\n", pending.MinID)
		fmt.Printf("  Max ID:    %s\n", pending.MaxID)
		fmt.Printf("  Consumers: %d\n\n", len(pending.Consumers))

		if len(pending.Consumers) > 0 {
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "Consumer\tCount")
			fmt.Fprintln(w, "--------\t-----")
			for _, c := range pending.Consumers {
				fmt.Fprintf(w, "%s\t%d\n", c.Name, c.Count)
			}
			w.Flush()
		}
	}
}

func handleConsumers(ctx context.Context, admin *thinrsmq.Admin, topic, group string, jsonOut bool) {
	consumers, err := admin.ConsumerInfo(ctx, topic, group)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if jsonOut {
		data, _ := json.MarshalIndent(consumers, "", "  ")
		fmt.Println(string(data))
	} else {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "Consumer\tPending\tIdle (ms)")
		fmt.Fprintln(w, "--------\t-------\t---------")
		for _, c := range consumers {
			fmt.Fprintf(w, "%s\t%d\t%d\n", c.Name, c.Pending, c.IdleMs)
		}
		w.Flush()
	}
}

func handleDLQ(ctx context.Context, admin *thinrsmq.Admin, topic string, args []string, jsonOut bool) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "Error: dlq command requires a subcommand")
		os.Exit(1)
	}

	subcommand := args[0]

	switch subcommand {
	case "size":
		size, err := admin.DLQSize(ctx, topic)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if jsonOut {
			fmt.Printf("{\"size\": %d}\n", size)
		} else {
			fmt.Printf("DLQ Size: %d\n", size)
		}

	case "peek":
		count := 10
		if len(args) > 1 {
			var err error
			count, err = strconv.Atoi(args[1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid count '%s'\n", args[1])
				os.Exit(1)
			}
		}

		entries, err := admin.DLQPeek(ctx, topic, int64(count))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if jsonOut {
			data, _ := json.MarshalIndent(entries, "", "  ")
			fmt.Println(string(data))
		} else {
			if len(entries) == 0 {
				fmt.Println("DLQ is empty")
			} else {
				w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
				fmt.Fprintln(w, "ID\tOriginal ID\tType\tError\tAttempts\tReplay Count\tFailed At")
				fmt.Fprintln(w, "--\t-----------\t----\t-----\t--------\t------------\t---------")
				for _, e := range entries {
					replayStr := strconv.Itoa(e.ReplayCount)
					maxReplays := admin.DLQMaxReplays()
					if int64(e.ReplayCount) >= maxReplays {
						replayStr += " (FROZEN)"
					}

					// Truncate error for display
					lastErr := e.LastError
					if len(lastErr) > 30 {
						lastErr = lastErr[:27] + "..."
					}

					fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
						e.ID, e.OriginalID, e.Type, lastErr, e.TotalAttempts, replayStr, e.FailedAt)
				}
				w.Flush()
			}
		}

	case "replay":
		count := 10
		if len(args) > 1 {
			var err error
			count, err = strconv.Atoi(args[1])
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: invalid count '%s'\n", args[1])
				os.Exit(1)
			}
		}

		// Get total before replay to calculate skipped
		before, _ := admin.DLQSize(ctx, topic)

		replayed, err := admin.DLQReplay(ctx, topic, int64(count))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		after, _ := admin.DLQSize(ctx, topic)
		skipped := before - after - int64(replayed)

		if jsonOut {
			fmt.Printf("{\"replayed\": %d, \"skipped\": %d}\n", replayed, skipped)
		} else {
			if skipped > 0 {
				fmt.Printf("Replayed %d messages. Skipped %d frozen entries (max replays reached).\n", replayed, skipped)
			} else {
				fmt.Printf("Replayed %d messages.\n", replayed)
			}
		}

	case "purge":
		purged, err := admin.DLQPurge(ctx, topic)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

		if jsonOut {
			fmt.Printf("{\"purged\": %d}\n", purged)
		} else {
			fmt.Printf("Purged %d entries from DLQ.\n", purged)
		}

	default:
		fmt.Fprintf(os.Stderr, "Error: unknown dlq subcommand '%s'\n", subcommand)
		os.Exit(1)
	}
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
