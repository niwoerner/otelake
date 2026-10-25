package server

import (
	"errors"
	"io"

	"golang.org/x/sync/semaphore"
)

// memoryPerBodyByte estimates how much memory one decompressed request byte
// takes while the request is decoded, flattened and waits for its flush: the
// body, the pdata objects and the Arrow rows.
const memoryPerBodyByte = 4

var errOverBudget = errors.New("memory budget exhausted")

// budgetReader reserves every byte it reads in the memory budget, so many
// concurrent bodies can't exceed it before they are even decoded.
type budgetReader struct {
	r        io.Reader
	budget   *semaphore.Weighted
	reserved int64
}

func (b *budgetReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 {
		if !b.budget.TryAcquire(int64(n)) {
			return n, errOverBudget
		}
		b.reserved += int64(n)
	}
	return n, err
}
