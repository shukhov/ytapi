package table

import (
	"os"
	"runtime"
	"strconv"
)

func GetBatchSize() uint64 {
	if val, exists := os.LookupEnv("DEFAULT_BATCH_SIZE"); exists {
		if parsed, err := strconv.ParseUint(val, 10, 64); err == nil {
			return parsed
		}
	}
	return 10000
}

func SetBatchSize(batchSize uint64) error {
	if err := os.Setenv("DEFAULT_BATCH_SIZE", strconv.FormatUint(batchSize, 10)); err != nil {
		return err
	}
	return nil
}

func GetWorkerPool() uint64 {
	if val, exists := os.LookupEnv("WORKER_POOL"); exists {
		if parsed, err := strconv.ParseUint(val, 10, 64); err == nil {
			return parsed
		}
	}
	numCPU := uint64(runtime.NumCPU()) - 1
	return numCPU
}

func SetWorkerPool(workerPool uint64) error {
	if err := os.Setenv("WORKER_POOL", strconv.FormatUint(workerPool, 10)); err != nil {
		return err
	}
	return nil
}

type Row = map[string]interface{}

type RowJson = []byte
