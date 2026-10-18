package query

import (
	"context"
	"testing"

	"github.com/zzet/gortex/internal/graph"
)

func TestEngineWithContextKeepsRequestsIndependent(t *testing.T) {
	e := NewEngine(graph.New())
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	defer cancelFirst()
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	first, second := e.WithContext(firstCtx), e.WithContext(secondCtx)
	if first == e || second == e || first == second {
		t.Fatal("cancellable requests must have independent engines")
	}
	if first.requestContext() != firstCtx || second.requestContext() != secondCtx {
		t.Fatal("caller context not preserved")
	}
	cancelFirst()
	if first.requestContext().Err() != context.Canceled || second.requestContext().Err() != nil || e.requestContext().Err() != nil {
		t.Fatal("request cancellation leaked to another engine")
	}
	// Explicit rebinding must clear a cancelled request context, even when the
	// replacement cannot be cancelled. Optimising WithContext itself must not
	// accidentally keep the previous request's cancellation.
	reset := first.WithContext(context.Background())
	if reset.requestContext().Err() != nil || first.requestContext().Err() != context.Canceled {
		t.Fatal("rebinding retained or mutated previous cancellation")
	}
	type key struct{}
	values := context.WithValue(context.Background(), key{}, "request")
	if got := e.WithContext(values).requestContext().Value(key{}); got != "request" {
		t.Fatal("non-cancellable context values lost")
	}
}
