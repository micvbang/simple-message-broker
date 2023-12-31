package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/micvbang/go-helpy/filey"
	"github.com/micvbang/simple-message-broker/internal/infrastructure/logger"
)

type S3Storage struct {
	log            logger.Logger
	s3             s3iface.S3API
	topicCacheRoot string
	bucketName     string
}

type S3StorageInput struct {
	S3             s3iface.S3API
	LocalCacheRoot string
	BucketName     string
	RootDir        string
	Topic          string
}

func NewS3Storage(log logger.Logger, input S3StorageInput) (*Storage, error) {
	s3Storage := &S3Storage{
		log:            log,
		s3:             input.S3,
		bucketName:     input.BucketName,
		topicCacheRoot: input.LocalCacheRoot,
	}

	return NewStorage(log, s3Storage, input.RootDir, input.Topic)
}

func (ss *S3Storage) Writer(recordBatchPath string) (io.WriteCloser, error) {
	cacheRecordBatchPath := ss.recordBatchCachePath(recordBatchPath)
	log := ss.log.
		WithField("cacheRecordBatchPath", cacheRecordBatchPath).
		WithField("recordBatchPath", recordBatchPath)

	log.Debugf("checking cache for record batch")
	if filey.Exists(cacheRecordBatchPath) {
		log.Debugf("record already exists")
		return nil, fmt.Errorf("file already exists '%s'", cacheRecordBatchPath)
	}

	log.Debugf("creating cache file")
	f, err := ss.createCacheFile(cacheRecordBatchPath)
	if err != nil {
		return nil, err
	}
	log.Debugf("creating s3WriteCloser")

	writeCloser := &s3WriteCloser{f: f, s3Upload: func(rd io.ReadSeeker) error {
		_, err := ss.s3.PutObject(&s3.PutObjectInput{
			Bucket: &ss.bucketName,
			Key:    &recordBatchPath,
			Body:   rd,
		})
		return err
	}}

	return writeCloser, nil
}

func (ss *S3Storage) Reader(recordBatchPath string) (io.ReadSeekCloser, error) {
	cacheRecordBatchPath := ss.recordBatchCachePath(recordBatchPath)
	log := ss.log.
		WithField("cacheRecordBatchPath", cacheRecordBatchPath).
		WithField("recordBatchPath", recordBatchPath)

	log.Debugf("checking cache for record batch")

	// check if file is already cached
	f, err := os.Open(cacheRecordBatchPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("checking for file in cache '%s': %w", cacheRecordBatchPath, err)
	}
	if f != nil {
		// file in cache, don't fetch from s3
		return f, nil
	}

	log.Debugf("fetching record batch from s3")
	// file not in cache
	obj, err := ss.s3.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(ss.bucketName),
		Key:    &recordBatchPath,
	})
	if err != nil {
		return nil, fmt.Errorf("retrieving s3 object: %w", err)
	}
	defer obj.Body.Close()

	log.Debugf("creating cache file")
	f, err = ss.createCacheFile(cacheRecordBatchPath)
	if err != nil {
		return nil, err
	}

	log.Debugf("copying s3 object to cache file")
	_, err = io.Copy(f, obj.Body)
	if err != nil {
		return nil, fmt.Errorf("writing s3 object to disk '%s': %w", cacheRecordBatchPath, err)
	}

	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seeking to beginning of file: %w", err)
	}

	return f, nil
}

func (ss *S3Storage) ListFiles(topicPath string, extension string) ([]string, error) {
	log := ss.log.
		WithField("topicPath", topicPath).
		WithField("extension", extension)

	fileNames := make([]string, 0, 128)

	topicPath, _ = strings.CutPrefix(topicPath, "/")

	log.Debugf("listing objects in s3")
	err := ss.s3.ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(ss.bucketName),
		Prefix: &topicPath,
	}, func(objects *s3.ListObjectsOutput, b bool) bool {
		for _, obj := range objects.Contents {
			if obj == nil || obj.Key == nil {
				continue
			}

			filePath := *obj.Key

			if strings.HasSuffix(filePath, extension) {
				fileNames = append(fileNames, *obj.Key)
			}
		}
		return true
	})

	log.Debugf("found %d files", len(fileNames))

	return fileNames, err
}

func (ss *S3Storage) recordBatchCachePath(recordBatchPath string) string {
	return filepath.Join(ss.topicCacheRoot, recordBatchPath)
}

func (ss *S3Storage) createCacheFile(cacheRecordBatchPath string) (*os.File, error) {
	log := ss.log.WithField("cacheRecordBatchPath", cacheRecordBatchPath)

	log.Debugf("creating cache dirs")
	err := os.MkdirAll(filepath.Dir(cacheRecordBatchPath), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("creating cache topic dir: %w", err)
	}

	log.Debugf("creating cache file")
	f, err := os.Create(cacheRecordBatchPath)
	if err != nil {
		return nil, fmt.Errorf("creating cache record batch '%s': %w", cacheRecordBatchPath, err)
	}

	return f, err
}

type s3WriteCloser struct {
	f        *os.File
	s3Upload func(io.ReadSeeker) error
}

func (swc *s3WriteCloser) Write(b []byte) (int, error) {
	return swc.f.Write(b)
}

func (swc *s3WriteCloser) Close() error {
	_, err := swc.f.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seeking to beginning: %w", err)
	}

	err = swc.s3Upload(swc.f)
	if err != nil {
		return fmt.Errorf("uploading to s3: %w", err)
	}

	err = swc.f.Close()
	if err != nil {
		return fmt.Errorf("closing file: %w", err)
	}

	return nil
}
