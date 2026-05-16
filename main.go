package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	pageSize      = 250
	ceActivityAPI = "api/ce/activity"
	httpTimeout   = 30 * time.Second
	// 5-minute cutoff avoids inconsistent paging caused by in-progress tasks.
	maxAgeOffset = -5 * time.Minute
)

type ceActivityResponse struct {
	Paging struct {
		PageIndex int `json:"pageIndex"`
		PageSize  int `json:"pageSize"`
		Total     int `json:"total"`
	} `json:"paging"`
	Tasks []json.RawMessage `json:"tasks"`
}

func main() {
	host := flag.String("host", "", "SonarQube host URL (or SONAR_HOST_URL env var)")
	token := flag.String("token", "", "SonarQube authentication token (or SONAR_TOKEN env var)")
	outputDir := flag.String("output", "output", "Directory to write JSON output files")
	throttle := flag.Int("throttle", 5, "Maximum number of parallel HTTP requests")
	basicAuth := flag.Bool("basic-auth", false, "Use HTTP Basic auth instead of Bearer token")
	flag.Parse()

	if *host == "" {
		*host = os.Getenv("SONAR_HOST_URL")
	}
	if *token == "" {
		*token = os.Getenv("SONAR_TOKEN")
	}

	if *host == "" {
		log.Fatal("SonarQube host URL is required: use -host flag or SONAR_HOST_URL env var")
	}
	if *token == "" {
		log.Fatal("SonarQube token is required: use -token flag or SONAR_TOKEN env var")
	}

	start := time.Now()

	if err := prepareOutputDir(*outputDir); err != nil {
		log.Fatalf("Failed to prepare output directory: %v", err)
	}

	maxAge := time.Now().Add(maxAgeOffset).Format("2006-01-02T15:04:05-0700")
	client := &http.Client{Timeout: httpTimeout}
	authHeader := buildAuthHeader(*token, *basicAuth)

	fmt.Println("Fetching page 1...")
	firstPage, err := fetchPage(client, *host, authHeader, maxAge, 1)
	if err != nil {
		log.Fatalf("Failed to fetch page 1: %v", err)
	}
	if err := savePage(*outputDir, 1, firstPage); err != nil {
		log.Fatalf("Failed to save page 1: %v", err)
	}

	total := firstPage.Paging.Total
	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	fmt.Printf("Total tasks: %d across %d page(s)\n", total, totalPages)

	if totalPages > 1 {
		if err := fetchPagesParallel(client, *host, authHeader, maxAge, *outputDir, 2, totalPages, *throttle); err != nil {
			log.Fatalf("Failed during parallel fetch: %v", err)
		}
	}

	fmt.Printf("Done. %d tasks saved to %q. Elapsed: %s\n",
		total, *outputDir, time.Since(start).Round(time.Millisecond))
}

func fetchPagesParallel(client *http.Client, host, authHeader, maxAge, outputDir string, from, to, throttle int) error {
	sem := make(chan struct{}, throttle)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for page := from; page <= to; page++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			fmt.Printf("Fetching page %d/%d...\n", p, to)
			data, err := fetchPage(client, host, authHeader, maxAge, p)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("page %d: %w", p, err)
				}
				mu.Unlock()
				return
			}
			if err := savePage(outputDir, p, data); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("save page %d: %w", p, err)
				}
				mu.Unlock()
			}
		}(page)
	}

	wg.Wait()
	return firstErr
}

func fetchPage(client *http.Client, host, authHeader, maxAge string, page int) (*ceActivityResponse, error) {
	endpoint := strings.TrimRight(host, "/") + "/" + ceActivityAPI
	params := url.Values{
		"ps":            {fmt.Sprintf("%d", pageSize)},
		"maxExecutedAt": {maxAge},
		"p":             {fmt.Sprintf("%d", page)},
	}
	reqURL := endpoint + "?" + params.Encode()

	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", authHeader)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusUnauthorized:
		return nil, fmt.Errorf("authentication failed (401): verify your token")
	case http.StatusForbidden:
		return nil, fmt.Errorf("permission denied (403): insufficient privileges")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var result ceActivityResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return &result, nil
}

func savePage(dir string, page int, data *ceActivityResponse) error {
	path := filepath.Join(dir, fmt.Sprintf("background-tasks-page-%04d.json", page))
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

func prepareOutputDir(dir string) error {
	matches, err := filepath.Glob(filepath.Join(dir, "background-tasks-page-*.json"))
	if err != nil {
		return err
	}
	for _, f := range matches {
		if err := os.Remove(f); err != nil {
			return err
		}
	}
	return os.MkdirAll(dir, 0o755)
}

func buildAuthHeader(token string, basic bool) string {
	if basic {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(token+":"))
	}
	return "Bearer " + token
}
