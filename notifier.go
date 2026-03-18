package notifier

import (
	"context"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/critma/notifier/internal/backoff"
	"golang.org/x/time/rate"
)

type Notifer interface {
	Send(ctx context.Context, msg Message) error
	Stats() Stats
	Close() error
}

type ExternalClient interface {
	Post(ctx context.Context, msg Message) (statusCode int, err error)
}

type notifier struct {
	client     ExternalClient
	limiter    *rate.Limiter
	taskQ      chan Message
	qClosed    atomic.Int32
	wg         sync.WaitGroup
	maxRetries int
	Sent       atomic.Int64 // успешно отправлено
	Failed     atomic.Int64 // завершилось ошибкой
	Retries    atomic.Int64 // количество повторных попыток
}

// queue states
const (
	closed = 1
	open   = 0
)

const maxRetries = 3

func NewNotifier(client ExternalClient, workers, ratePerSecond int) Notifer {
	n := &notifier{
		client:     client,
		limiter:    rate.NewLimiter(rate.Limit(ratePerSecond), ratePerSecond),
		taskQ:      make(chan Message, workers*2),
		maxRetries: maxRetries,
	}
	n.qClosed.Store(open)
	for range workers {
		n.wg.Go(n.worker)
	}
	return n
}

func (n *notifier) Send(ctx context.Context, msg Message) error {
	if n.qClosed.Load() == closed {
		return ErrClosed
	}
	select {
	case n.taskQ <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *notifier) Close() error {
	if !n.qClosed.CompareAndSwap(0, 1) {
		return ErrClosed
	}
	close(n.taskQ)
	n.wg.Wait()
	return nil
}

func (n *notifier) Stats() Stats {
	return Stats{
		Sent:    n.Sent.Load(),
		Failed:  n.Failed.Load(),
		Retries: n.Retries.Load(),
	}
}

func (n *notifier) worker() {
	defer n.wg.Done()
	for msg := range n.taskQ {
		if err := n.process(msg); err != nil {
			log.Printf("failed to process message: %v\n", err)
		}
	}
}

func (n *notifier) process(msg Message) error {
	var lastErr error
	for attempt := range n.maxRetries {
		if err := n.limiter.Wait(context.Background()); err != nil {
			return err
		}
		status, err := n.client.Post(context.Background(), msg)
		if err != nil {
			lastErr = err
			continue
		}
		if status == http.StatusTooManyRequests {
			n.Retries.Add(1)
			if attempt != n.maxRetries-1 {
				backoff := backoff.GetDelay(attempt)
				time.Sleep(backoff)
			}
			continue
		}
		if status >= 200 && status < 300 {
			n.Sent.Add(1)
			return nil
		}
		lastErr = ErrUnexpectedStatus
	}
	n.Failed.Add(1)
	return lastErr
}
