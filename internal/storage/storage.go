package storage

import (
	"fmt"
	"io"
	"path"
	"path/filepath"

	"github.com/micvbang/go-helpy/uint64y"
	"github.com/micvbang/simple-message-broker/internal/infrastructure/logger"
	"github.com/micvbang/simple-message-broker/internal/recordbatch"
)

type BackingStorage interface {
	Writer(recordBatchPath string) (io.WriteCloser, error)
	Reader(recordBatchPath string) (io.ReadSeekCloser, error)
	ListFiles(topicPath string, extension string) ([]string, error)
}

type Storage struct {
	log            logger.Logger
	topicPath      string
	nextRecordID   uint64
	recordBatchIDs []uint64

	backingStorage BackingStorage
}

func NewStorage(log logger.Logger, backingStorage BackingStorage, rootDir string, topic string) (*Storage, error) {
	topicPath := filepath.Join(rootDir, topic)

	recordBatchIDs, err := listRecordBatchIDs(backingStorage, topicPath)
	if err != nil {
		return nil, fmt.Errorf("listing record batches: %w", err)
	}

	storage := &Storage{
		log:            log,
		backingStorage: backingStorage,
		topicPath:      topicPath,
		recordBatchIDs: recordBatchIDs,
	}

	if len(recordBatchIDs) > 0 {
		newestRecordBatchID := recordBatchIDs[len(recordBatchIDs)-1]
		hdr, err := readRecordBatchHeader(backingStorage, topicPath, newestRecordBatchID)
		if err != nil {
			return nil, fmt.Errorf("reading record batch header: %w", err)
		}
		storage.nextRecordID = newestRecordBatchID + uint64(hdr.NumRecords)
	}

	return storage, nil

}

func (s *Storage) AddRecordBatch(records [][]byte) error {
	recordBatchID := s.nextRecordID

	rbPath := recordBatchPath(s.topicPath, recordBatchID)
	f, err := s.backingStorage.Writer(rbPath)
	if err != nil {
		return fmt.Errorf("opening writer '%s': %w", rbPath, err)
	}
	defer f.Close()

	err = recordbatch.Write(f, records)
	if err != nil {
		return fmt.Errorf("writing record batch: %w", err)
	}
	s.recordBatchIDs = append(s.recordBatchIDs, recordBatchID)
	s.nextRecordID = recordBatchID + uint64(len(records))

	return nil
}

func (s *Storage) ReadRecord(recordID uint64) ([]byte, error) {
	if recordID >= s.nextRecordID {
		return nil, fmt.Errorf("record ID does not exist: %w", ErrOutOfBounds)
	}

	var recordBatchID uint64
	for i := len(s.recordBatchIDs) - 1; i >= 0; i-- {
		curBatchID := s.recordBatchIDs[i]
		if curBatchID <= recordID {
			recordBatchID = curBatchID
			break
		}
	}

	rbPath := recordBatchPath(s.topicPath, recordBatchID)
	f, err := s.backingStorage.Reader(rbPath)
	if err != nil {
		return nil, fmt.Errorf("opening reader '%s': %w", rbPath, err)
	}

	rb, err := recordbatch.Parse(f)
	if err != nil {
		return nil, fmt.Errorf("parsing record batch '%s': %w", rbPath, err)
	}

	record, err := rb.Record(uint32(recordID - recordBatchID))
	if err != nil {
		return nil, fmt.Errorf("record batch '%s': %w", rbPath, err)
	}
	return record, nil
}

func readRecordBatchHeader(backingStorage BackingStorage, topicPath string, recordBatchID uint64) (recordbatch.Header, error) {
	rbPath := recordBatchPath(topicPath, recordBatchID)
	f, err := backingStorage.Reader(rbPath)
	if err != nil {
		return recordbatch.Header{}, fmt.Errorf("opening recordBatch '%s': %w", rbPath, err)
	}

	rb, err := recordbatch.Parse(f)
	if err != nil {
		return recordbatch.Header{}, fmt.Errorf("parsing record batch '%s': %w", rbPath, err)
	}

	return rb.Header, nil
}

func listRecordBatchIDs(backingStorage BackingStorage, topicPath string) ([]uint64, error) {
	filePaths, err := backingStorage.ListFiles(topicPath, recordBatchExtension)
	if err != nil {
		return nil, fmt.Errorf("listing files: %w", err)
	}

	recordIDs := make([]uint64, 0, len(filePaths))
	for _, filePath := range filePaths {
		fileName := path.Base(filePath)
		recordIDStr := fileName[:len(fileName)-len(recordBatchExtension)]

		recordID, err := uint64y.FromString(recordIDStr)
		if err != nil {
			return nil, err
		}

		recordIDs = append(recordIDs, recordID)
	}

	return recordIDs, nil
}

func recordBatchPath(topicPath string, recordBatchID uint64) string {
	return filepath.Join(topicPath, fmt.Sprintf("%012d%s", recordBatchID, recordBatchExtension))
}
