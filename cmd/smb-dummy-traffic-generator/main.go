package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/micvbang/go-helpy/inty"
	"github.com/micvbang/simple-message-broker/internal/infrastructure/logger"
)

func main() {
	flags := parseFlags()

	ctx := context.Background()
	log := logger.NewWithLevel(ctx, logger.LevelInfo)

	client := Client{log: log, client: &http.Client{}}
	records := make(chan int, 2*flags.numWorkers)

	for i := 0; i < flags.numWorkers; i++ {
		go func() {
			for record := range records {
				time.Sleep(time.Duration(inty.RandomN(5)) * time.Second)

				err := client.Add(record)
				if err != nil {
					log.Errorf("err: %s", err)
				}
			}
		}()
	}

	for i := 0; i < flags.numRequests; i++ {
		records <- i
	}
}

type Client struct {
	log    logger.Logger
	client *http.Client
}

func (c Client) Add(id int) error {
	recordRdr, err := makeRecord(id)
	if err != nil {
		return fmt.Errorf("creating record: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, "http://localhost:8080/add", recordRdr)
	if err != nil {
		return fmt.Errorf("creating http request: %w", err)
	}

	response, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("got status code %d", response.StatusCode)
	}

	return nil
}

func makeRecord(id int) (io.Reader, error) {
	bs, err := json.Marshal(struct{ ID int }{id})
	if err != nil {
		return nil, err
	}

	return bytes.NewBuffer(bs), nil
}

type flags struct {
	numWorkers  int
	numRequests int
}

func parseFlags() flags {
	fs := flag.NewFlagSet("scl-dump", flag.ExitOnError)

	f := flags{}

	fs.IntVar(&f.numWorkers, "w", 100, "Number of workers")
	fs.IntVar(&f.numRequests, "n", 50_000_000, "Number of requests to send")

	err := fs.Parse(os.Args[1:])
	if err != nil {
		fs.Usage()
		os.Exit(1)
	}

	return f
}
