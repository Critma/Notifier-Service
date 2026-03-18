package notifier_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/critma/notifier"
)

type mockClient struct {
	statusCode  int
	err         error
	numCalls    atomic.Int64
	mu          sync.Mutex
	lastMessage notifier.Message
}

func (m *mockClient) Post(ctx context.Context, msg notifier.Message) (int, error) {
	m.numCalls.Add(1)
	m.mu.Lock()
	m.lastMessage = msg
	m.mu.Unlock()
	return m.statusCode, m.err
}

func (m *mockClient) GetLastMessage() notifier.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastMessage
}

const MaxRetries = 3

func TestNotifier_SendAndClose(t *testing.T) {
	client := &mockClient{statusCode: 200}
	ntf := notifier.NewNotifier(client, 2, 10, MaxRetries)
	ctx := context.Background()

	msg := notifier.Message{ID: "1", Payload: "test"}
	if err := ntf.Send(ctx, msg); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if err := ntf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Check sends after close
	if err := ntf.Send(ctx, msg); err != notifier.ErrClosed {
		t.Fatalf("Send after Close should return ErrClosed, got %v", err)
	}
}

func TestNotifier_RateLimiting(t *testing.T) {
	client := &mockClient{statusCode: 200}
	ntf := notifier.NewNotifier(client, 2, 2, MaxRetries) // 2 req/sec
	ctx := context.Background()

	start := time.Now()
	for i := range 5 {
		msg := notifier.Message{ID: string(rune('A' + i)), Payload: "test"}
		if err := ntf.Send(ctx, msg); err != nil {
			t.Fatalf("Send failed: %v", err)
		}
	}

	if err := ntf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	elapsed := time.Since(start)
	// should be > 2 seconds
	if elapsed < 2*time.Second {
		t.Fatalf("Rate limiting not working, elapsed: %v", elapsed)
	}
}

func TestNotifier_RetryOn429(t *testing.T) {
	client := &mockClient{statusCode: 429, err: nil}
	ntf := notifier.NewNotifier(client, 1, 10, MaxRetries)
	ctx := context.Background()

	msg := notifier.Message{ID: "1", Payload: "test"}
	if err := ntf.Send(ctx, msg); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if err := ntf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should 4 calls (1+3 retry)
	if calls := client.numCalls.Load(); calls != 4 {
		t.Fatalf("Expected 4 calls due to retries, got %d", calls)
	}

	stats := ntf.Stats()
	if stats.Retries != 3 {
		t.Fatalf("Expected 3 retries, got %d", stats.Retries)
	}
	if stats.Failed == 0 {
		t.Fatalf("Expected failed message, got 0")
	}
	if stats.Failed != 1 {
		t.Fatalf("Expected 1 failed message, got %v", stats.Failed)
	}
}

func TestNotifier_ConcurrentSend(t *testing.T) {
	client := &mockClient{statusCode: 200}
	ntf := notifier.NewNotifier(client, 5, 10, MaxRetries)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Go(func() {
			msg := notifier.Message{ID: string(rune('A' + i)), Payload: "test"}
			if err := ntf.Send(ctx, msg); err != nil {
				t.Errorf("Send failed for msg %d: %v", i, err)
			}
		})
	}

	wg.Wait()
	if err := ntf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if calls := client.numCalls.Load(); calls != 10 {
		t.Fatalf("Expected 10 calls, got %d", calls)
	}
}

func TestNotifier_ContextCancellation(t *testing.T) {
	client := &mockClient{statusCode: 200}
	ntf := notifier.NewNotifier(client, 1, 10, MaxRetries)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	msg := notifier.Message{ID: "1", Payload: "test"}
	if err := ntf.Send(ctx, msg); err != context.Canceled {
		t.Fatalf("Send with canceled context should return context.Canceled, got %v", err)
	}

	if err := ntf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestNotifier_CheckData(t *testing.T) {
	ExpectedMsg := notifier.Message{ID: "1", Payload: "data"}
	client := &mockClient{statusCode: 200, lastMessage: ExpectedMsg}
	ntf := notifier.NewNotifier(client, 1, 10, MaxRetries)
	ntf.Send(context.Background(), ExpectedMsg)

	clientMessage := client.GetLastMessage()
	if clientMessage.ID != ExpectedMsg.ID || clientMessage.Payload != ExpectedMsg.Payload {
		t.Fatalf("Expected message %v, got %v", ExpectedMsg, clientMessage)
	}

	if err := ntf.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}
