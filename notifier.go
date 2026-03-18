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

type Notifier interface {
	Send(ctx context.Context, msg Message) error
	Stats() Stats
	Close() error
}

type notifier struct {
	client     ExternalClient
	limiter    *rate.Limiter
	taskQ      chan Task
	qClosed    atomic.Int32
	wg         sync.WaitGroup
	maxRetries int
	Sent       atomic.Int64 // успешно отправлено
	Failed     atomic.Int64 // завершилось ошибкой
	Retries    atomic.Int64 // количество повторных попыток
}

type ExternalClient interface {
	Post(ctx context.Context, msg Message) (statusCode int, err error)
}

type Task struct {
	msg Message
	ctx context.Context
}

// queue states
const (
	closed = 1
	open   = 0
)

func NewNotifier(client ExternalClient, workers, ratePerSecond, maxRetries int) Notifier {
	n := &notifier{
		client:     client,
		limiter:    rate.NewLimiter(rate.Limit(ratePerSecond), ratePerSecond),
		taskQ:      make(chan Task, workers*2),
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
	case n.taskQ <- Task{msg: msg, ctx: ctx}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *notifier) Close() error {
	if !n.qClosed.CompareAndSwap(open, closed) {
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
	for task := range n.taskQ {
		if err := n.process(task.ctx, task.msg); err != nil {
			log.Printf("failed to process message {%s}: %v\n", task.msg, err)
		}
	}
}

func (n *notifier) process(ctx context.Context, msg Message) error {
	var lastErr error
	for attempt := 1; attempt <= n.maxRetries+1; attempt++ {
		select {
		case <-ctx.Done():
			n.Failed.Add(1)
			return ctx.Err()
		default:
		}

		if err := n.limiter.Wait(ctx); err != nil {
			n.Failed.Add(1)
			return err
		}

		status, err := n.client.Post(ctx, msg)
		if err != nil {
			lastErr = err
			continue
		}
		if status == http.StatusTooManyRequests {
			if attempt != n.maxRetries-1 {
				n.Retries.Add(1)
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
		break
	}
	n.Failed.Add(1)
	return lastErr
}
