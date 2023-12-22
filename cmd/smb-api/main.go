package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-message-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-message-broker/internal/recordbatch"
	"github.com/micvbang/simple-message-broker/internal/storage"
)

func main() {
	ctx := context.Background()
	log := logger.NewWithLevel(ctx, logger.LevelDebug)

	mux := http.NewServeMux()

	contextCreator := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		go func() {
			time.Sleep(10 * time.Second)
			cancel()
		}()

		return ctx
	}

	session, err := session.NewSession()
	if err != nil {
		log.Fatalf("creating s3 session: %s", err)
	}

	s3Storage, err := storage.NewS3Storage(log.Name("s3 storage"), storage.S3StorageInput{
		S3:             s3.New(session),
		LocalCacheRoot: "/tmp/recordbatch",
		BucketName:     "simple-commit-log-delete-me",
		RootDir:        "/tmp/recordbatch",
		Topic:          "s3topic2",
	})
	if err != nil {
		log.Fatalf("creating s3 storage: %s", err)
	}

	batcher := recordbatch.NewBlockingBatcher(log.Name("blocking batcher"), contextCreator, func(b [][]byte) error {
		t0 := time.Now()
		err := s3Storage.AddRecordBatch(b)
		log.Debugf("persisting to s3: %v", time.Since(t0))
		return err
	})

	mux.Handle("/add", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		log.Debugf("hit %s", r.URL)

		bs, err := io.ReadAll(r.Body)
		if err != nil {
			log.Errorf("failed to read body: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
		}

		err = batcher.Add(bs)
		if err != nil {
			log.Errorf("failed to add: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, err.Error())
		}
	}))

	const recordIDKey = "record-id"
	mux.Handle("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debugf("hit %s", r.URL)

		err := r.ParseForm()
		if err != nil {
			log.Errorf("parsing form: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to parse form: %s", err)
		}

		recordID, err := uint64y.FromString(r.Form.Get(recordIDKey))
		if err != nil {
			log.Errorf("parsing record id key: %s", err.Error())
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "url parameter '%s' required, must be a number: %s", recordIDKey, err)
			w.Write([]byte(err.Error()))
		}

		record, err := s3Storage.ReadRecord(recordID)
		if err != nil {
			if errors.Is(err, storage.ErrOutOfBounds) {
				log.Debugf("not found")
				w.WriteHeader(http.StatusNotFound)
				return
			}

			log.Errorf("reading record: %s", err.Error())
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "failed to read record '%d': %s", recordID, err)
		}
		w.Write(record)
	}))

	addr := "127.0.0.1:8080"
	log.Infof("Listening on %s", addr)
	err = http.ListenAndServe(addr, mux)
	log.Fatalf("ListenAndServe returned: %s", err)
}
