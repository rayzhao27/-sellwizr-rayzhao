package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql" // registers the MySQL driver
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

type Message struct {
	Schema []Field           `json:"schema"`
	Row    map[string]string `json:"row"`
	RowNum int               `json:"row_num"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

func main() {
	// Load .env file if it exists
	_ = godotenv.Load()

	// Read config from environment variables
	kafkaBroker := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC", "table-rows")
	kafkaGroup := getEnv("KAFKA_GROUP_ID", "table-consumer")
	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "3306")
	dbUser := getEnv("DB_USER", "root")
	dbPass := getEnv("DB_PASSWORD", "secret")
	dbName := getEnv("DB_NAME", "tabledata")
	dbTable := getEnv("DB_TABLE_NAME", "extracted_table")
	batchSize := 50

	// Connect to MySQL
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
		dbUser, dbPass, dbHost, dbPort, dbName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Verify the connection works
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	log.Printf("Connected to MySQL at %s:%s/%s", dbHost, dbPort, dbName)

	// Connect to Kafka
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBroker},
		Topic:          kafkaTopic,
		GroupID:        kafkaGroup,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10 MB
		CommitInterval: time.Second,
		StartOffset:    kafka.FirstOffset, // start from beginning if no offset saved
	})
	defer reader.Close()
	log.Printf("Listening on Kafka topic: %s (group: %s)", kafkaTopic, kafkaGroup)

	// Graceful shutdown on Ctrl+C
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("Shutting down...")
		cancel()
	}()

	var (
		schema        []Field             // set on first message
		tableCreated  bool                // only create table once
		batch         []map[string]string // rows waiting to be inserted
		totalInserted int
	)

	for {
		// Read one message from Kafka (blocks until a message arrives)
		kafkaMsg, err := reader.ReadMessage(ctx)
		if err != nil {
			// Context cancelled = clean shutdown
			if ctx.Err() != nil {
				break
			}
			log.Printf("Error reading from Kafka: %v", err)
			continue
		}

		// Decode the JSON message
		var msg Message
		if err := json.Unmarshal(kafkaMsg.Value, &msg); err != nil {
			log.Printf("Failed to decode message: %v", err)
			continue
		}

		// First message: create the MySQL table based on the schema
		if !tableCreated {
			schema = msg.Schema
			log.Printf("Creating table with %d columns...", len(schema))

			if err := createTable(db, dbTable, schema); err != nil {
				log.Fatalf("Failed to create table: %v", err)
			}
			tableCreated = true
			log.Printf("Table '%s' ready", dbTable)
		}

		// Add this row to the batch
		batch = append(batch, msg.Row)

		// When batch is full, insert into MySQL
		if len(batch) >= batchSize {
			if err := batchInsert(db, dbTable, schema, batch); err != nil {
				log.Printf("Batch insert failed: %v", err)
			} else {
				totalInserted += len(batch)
				log.Printf("Inserted %d rows (total: %d)", len(batch), totalInserted)
			}
			batch = batch[:0] // clear the batch
		}
	}

	// Insert any remaining rows that didn't fill a full batch
	if len(batch) > 0 {
		if err := batchInsert(db, dbTable, schema, batch); err != nil {
			log.Printf("Final batch insert failed: %v", err)
		} else {
			totalInserted += len(batch)
		}
	}

	log.Printf("Done. Total rows inserted: %d", totalInserted)
}

func createTable(db *sql.DB, tableName string, schema []Field) error {
	// Build the column definitions from the schema
	// e.g. `company_name` VARCHAR(512), `market_cap` DOUBLE
	colDefs := make([]string, len(schema))
	for i, f := range schema {
		colDefs[i] = fmt.Sprintf("  `%s` %s", f.Name, f.Type)
	}

	// Full CREATE TABLE statement
	// - id: auto-increment primary key
	// - row_hash: unique hash of the row content (prevents duplicates)
	// - IF NOT EXISTS: safe to run multiple times
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS `+"`%s`"+` (
  `+"`id`"+`       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `+"`row_hash`"+` VARCHAR(64) NOT NULL,
  %s,
  PRIMARY KEY (`+"`id`"+`),
  UNIQUE KEY `+"`uq_row_hash`"+` (`+"`row_hash`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
		tableName,
		strings.Join(colDefs, ",\n  "),
	)

	_, err := db.Exec(ddl)
	if err != nil {
		return fmt.Errorf("CREATE TABLE failed: %w", err)
	}
	return nil
}

// ── batchInsert: inserts a slice of rows in a single transaction ───────────────

func batchInsert(db *sql.DB, tableName string, schema []Field, rows []map[string]string) error {
	if len(rows) == 0 {
		return nil
	}

	// Build column list: row_hash + all data columns
	colNames := []string{"`row_hash`"}
	for _, f := range schema {
		colNames = append(colNames, fmt.Sprintf("`%s`", f.Name))
	}

	// Build placeholder: (?, ?, ?, ...)
	placeholders := strings.Repeat("?,", len(colNames))
	placeholders = "(" + strings.TrimSuffix(placeholders, ",") + ")"

	// INSERT IGNORE skips rows where row_hash already exists (idempotent)
	query := fmt.Sprintf("INSERT IGNORE INTO `%s` (%s) VALUES %s",
		tableName,
		strings.Join(colNames, ", "),
		placeholders,
	)

	// Use a transaction so the whole batch succeeds or fails together
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	stmt, err := tx.Prepare(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		// Build the values in the same order as columns
		args := []interface{}{rowHash(row)} // first value is the hash
		for _, f := range schema {
			args = append(args, row[f.Name])
		}

		if _, err := stmt.Exec(args...); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert row: %w", err)
		}
	}

	return tx.Commit()
}

func rowHash(row map[string]string) string {
	// Sort keys so the hash is always the same regardless of map order
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	// Simple sort
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k + "=" + row[k] + "|")
	}

	sum := sha256.Sum256([]byte(sb.String()))
	return fmt.Sprintf("%x", sum[:16]) // 32-char hex string
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}
