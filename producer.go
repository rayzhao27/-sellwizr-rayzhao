package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
	"golang.org/x/net/html"
)

func main() {
	_ = godotenv.Load()

	// Read config from environment var with fallbacks
	targetURL := getEnv("TARGET_URL", "https://en.wikipedia.org/wiki/List_of_research_universities_in_the_United_States")
	kafkaBroker := getEnv("KAFKA_BROKERS", "localhost:9092")
	kafkaTopic := getEnv("KAFKA_TOPIC", "table-rows")

	// Fetch the HTML page
	log.Printf("Fetching: %s", targetURL)
	body, err := fetchWithRetry(targetURL, 3)
	if err != nil {
		log.Fatalf("Failed to fetch URL: %v", err)
	}
	log.Printf("Fetched %d bytes", len(body))

	// Parse the first HTML table
	headers, rows, err := parseFirstTable(body)
	if err != nil {
		log.Fatalf("Failed to parse table: %v", err)
	}
	log.Printf("Parsed table: %d columns, %d rows", len(headers), len(rows))

	// Infer schema (column types)
	schema := inferSchema(headers, rows)
	for _, f := range schema {
		log.Printf("  Column: %s → %s", f.Name, f.Type)
	}

	// Connect to Kafka and publish rows
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        kafkaTopic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireOne,
		WriteTimeout: 10 * time.Second,
	}
	defer writer.Close()

	ctx := context.Background()
	published := 0

	for i, row := range rows {
		// Build a map of columnName
		rowMap := make(map[string]string)
		for j, field := range schema {
			if j < len(row) {
				rowMap[field.Name] = row[j]
			}
		}

		msg := Message{
			Schema: schema,
			Row:    rowMap,
			RowNum: i,
		}

		// Serialising to json
		payload, err := json.Marshal(msg)
		if err != nil {
			log.Printf("Row %d: failed to marshal: %v", i, err)
			continue
		}

		// Push to Kafka
		err = writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("row-%d", i)),
			Value: payload,
		})
		if err != nil {
			log.Printf("Row %d: failed to publish: %v", i, err)
			continue
		}

		published++
		if published%50 == 0 {
			log.Printf("Published %d/%d rows...", published, len(rows))
		}
	}

	log.Printf("Done! Published %d rows to topic: %s", published, kafkaTopic)
}

func fetchWithRetry(url string, maxRetries int) ([]byte, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		log.Printf("Attempt %d/%d...", attempt, maxRetries)

		req, _ := http.NewRequest("GET", url, nil)
		req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		resp, err := client.Do(req)
		
		if err != nil {
			lastErr = err
			log.Printf("Attempt %d failed: %v", attempt, err)
			time.Sleep(time.Duration(attempt*attempt) * time.Second) // 1s, 4s, 9s
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("bad status: %s", resp.Status)
			time.Sleep(time.Duration(attempt*attempt) * time.Second)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("reading body: %w", err)
		}
		return body, nil // success
	}

	return nil, fmt.Errorf("all %d attempts failed, last error: %w", maxRetries, lastErr)
}

func parseFirstTable(body []byte) ([]string, [][]string, error) {
	// Parse the HTML into a node tree
	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, nil, fmt.Errorf("html parse failed: %w", err)
	}

	// Find the first <table> node
	tableNode := findNode(doc, "table")
	if tableNode == nil {
		return nil, nil, fmt.Errorf("no <table> found in page")
	}

	var headers []string
	var rows [][]string

	// Loop through every <tr> in the table
	for _, tr := range findAllNodes(tableNode, "tr") {
		cells := extractCells(tr)
		if len(cells) == 0 {
			continue
		}

		// First row with <th> cells = headers
		if headers == nil && rowHasHeaders(tr) {
			headers = cells
			continue
		}

		// If we still have no headers, use first row as headers
		if headers == nil {
			headers = cells
			continue
		}

		// Pad or trim row to match header count
		normalised := make([]string, len(headers))
		for i := range normalised {
			if i < len(cells) {
				normalised[i] = cells[i]
			}
		}
		rows = append(rows, normalised)
	}

	if len(headers) == 0 {
		return nil, nil, fmt.Errorf("could not find table headers")
	}
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("table has no data rows")
	}

	return headers, rows, nil
}

func findNode(n *html.Node, tag string) *html.Node {
	if n.Type == html.ElementNode && n.Data == tag {
		return n
	}
	for child := n.FirstChild; child != nil; child = child.NextSibling {
		if found := findNode(child, tag); found != nil {
			return found
		}
	}
	return nil
}

func findAllNodes(root *html.Node, tag string) []*html.Node {
	var result []*html.Node
	var walk func(*html.Node)

	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == tag {
			result = append(result, n)
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}

	walk(root)

	return result
}

func extractCells(row *html.Node) []string {
	var cells []string

	for child := row.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == html.ElementNode && (child.Data == "td" || child.Data == "th") {
			cells = append(cells, strings.TrimSpace(nodeText(child)))
		}
	}

	return cells
}

func nodeText(n *html.Node) string {
	if n.Type == html.TextNode {
		return n.Data
	}
	var result string

	for child := n.FirstChild; child != nil; child = child.NextSibling {
		result += nodeText(child)
	}

	return result
}

func rowHasHeaders(row *html.Node) bool {
	for child := row.FirstChild; child != nil; child = child.NextSibling {
		if child.Type == html.ElementNode && child.Data == "th" {
			return true
		}
	}

	return false
}

func inferSchema(headers []string, rows [][]string) []Field {
	fields := make([]Field, len(headers))
	for i, h := range headers {
		fields[i] = Field{
			Name: cleanColumnName(h),
			Type: inferColumnType(i, rows),
		}
	}

	return fields
}

func inferColumnType(colIndex int, rows [][]string) string {
	allInt := true
	allFloat := true
	maxLen := 0

	for _, row := range rows {
		if colIndex >= len(row) {
			continue
		}

		v := strings.TrimSpace(row[colIndex])
		if v == "" || v == "-" || v == "N/A" {
			continue // skip empty / null-like values
		}

		// Strip common formatting before trying to parse as number
		clean := strings.ReplaceAll(v, ",", "")
		clean = strings.TrimPrefix(clean, "$")
		clean = strings.TrimSuffix(clean, "%")

		if _, err := strconv.ParseInt(clean, 10, 64); err != nil {
			allInt = false
		}
		if _, err := strconv.ParseFloat(clean, 64); err != nil {
			allFloat = false
		}
		if len(v) > maxLen {
			maxLen = len(v)
		}
	}

	switch {
	case allInt:
		return "BIGINT"
	case allFloat:
		return "DOUBLE"
	case maxLen > 512:
		return "TEXT"
	default:
		return "VARCHAR(512)"
	}
}

func cleanColumnName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ToLower(name)
	// Replace anything that isn't a letter or digit with underscore
	var result strings.Builder

	for _, ch := range name {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') {
			result.WriteRune(ch)
		} else {
			result.WriteRune('_')
		}
	}

	// Clean up multiple underscores
	cleaned := strings.Trim(result.String(), "_")
	for strings.Contains(cleaned, "__") {
		cleaned = strings.ReplaceAll(cleaned, "__", "_")
	}

	if cleaned == "" {
		return "col"
	}

	return cleaned
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

type Message struct {
	Schema []Field           `json:"schema"`
	Row    map[string]string `json:"row"`
	RowNum int               `json:"row_num"`
}

type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}
