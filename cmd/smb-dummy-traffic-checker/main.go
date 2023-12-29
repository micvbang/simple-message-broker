package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/simple-message-broker/internal/infrastructure/logger"
)

func main() {
	flags := parseFlags()

	ctx := context.Background()
	log := logger.NewWithLevel(ctx, logger.LevelInfo)

	client := Client{log: log, client: &http.Client{}}

	records := make([]int, 0, flags.numRecords)
	for record := 0; record < flags.numRecords; record++ {
		gotRecord, err := client.Get(record)
		if err != nil {
			log.Errorf("err: %s", err)
		}

		records = append(records, gotRecord.ID)
	}

	gotRecords := inty.ToSet(records)
	for record := 0; record < flags.numRecords; record++ {
		if !gotRecords.Contains(record) {
			log.Errorf("%d not found", record)
		}
	}
}

type Client struct {
	log    logger.Logger
	client *http.Client
}

type Record struct {
	ID int
}

func (c Client) Get(id int) (Record, error) {
	theURL, err := url.Parse("http://localhost:8080/get")
	if err != nil {
		return Record{}, fmt.Errorf("parsing URL: %s", err)
	}

	query := theURL.Query()
	query.Add("record-id", strconv.FormatInt(int64(id), 10))
	theURL.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodPost, theURL.String(), nil)
	if err != nil {
		return Record{}, fmt.Errorf("creating http request: %w", err)
	}

	response, err := c.client.Do(req)
	if err != nil {
		return Record{}, fmt.Errorf("sending request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return Record{}, fmt.Errorf("got status code %d", response.StatusCode)
	}

	r := Record{}
	err = json.NewDecoder(response.Body).Decode(&r)
	if err != nil {
		return Record{}, fmt.Errorf("decoding json: %w", err)
	}

	return r, nil
}

type flags struct {
	numWorkers int
	numRecords int
}

func parseFlags() flags {
	fs := flag.NewFlagSet("scl-dump", flag.ExitOnError)

	f := flags{}

	fs.IntVar(&f.numWorkers, "w", 100, "Number of workers")
	fs.IntVar(&f.numRecords, "n", 100_000, "Number of requests to check")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fs.Usage()
		os.Exit(1)
	}

	return f
}
