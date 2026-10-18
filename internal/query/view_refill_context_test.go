package query

import (
	"context"
	"testing"

	"github.com/zzet/gortex/internal/search"
)

type refillChannelBackend struct{ fusedCalls, channelCalls int }

func (*refillChannelBackend) Add(string, ...string) {}
func (*refillChannelBackend) Remove(string)         {}
func (*refillChannelBackend) Close()                {}
func (*refillChannelBackend) Count() int            { return 1 }
func (*refillChannelBackend) Search(string, int) []search.SearchResult {
	return []search.SearchResult{{ID: "legacy-fused"}}
}
func (b *refillChannelBackend) SearchContext(context.Context, string, int) []search.SearchResult {
	b.fusedCalls++
	return []search.SearchResult{{ID: "fused-vector"}}
}
func (b *refillChannelBackend) SearchChannels(string, int) ([]search.SearchResult, []string) {
	b.channelCalls++
	return []search.SearchResult{{ID: "text"}}, []string{"vector"}
}

type refillContextChannelBackend struct {
	*refillChannelBackend
	ctx   context.Context
	calls int
}

func (b *refillContextChannelBackend) SearchChannelsTimedContext(ctx context.Context, _ string, _ int) ([]search.SearchResult, []string, search.ChannelTimings) {
	b.ctx = ctx
	b.calls++
	return []search.SearchResult{{ID: "context-text"}}, []string{"context-vector"}, search.ChannelTimings{TextMS: 7}
}

func TestViewBaseTextRefillContextPreservesTextChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, contextual := range []bool{false, true} {
		name := "legacy channels"
		if contextual {
			name = "context channels"
		}
		t.Run(name, func(t *testing.T) {
			base := &refillChannelBackend{}
			var backend search.Backend = base
			contextualBackend := &refillContextChannelBackend{refillChannelBackend: base}
			want := "text"
			if contextual {
				backend = contextualBackend
				want = "context-text"
			}
			got := viewBaseTextRefillContext(ctx, backend, "query", nil)(5)
			if len(got) != 1 || got[0].ID != want {
				t.Fatalf("refill = %v, want text channel %s", got, want)
			}
			if base.fusedCalls != 0 {
				t.Fatalf("refill invoked fused search %d times", base.fusedCalls)
			}
			if contextual {
				if contextualBackend.calls != 1 || contextualBackend.ctx != ctx || base.channelCalls != 0 {
					t.Fatal("context channel priority or caller context lost")
				}
			} else if base.channelCalls != 1 {
				t.Fatal("legacy channel was bypassed")
			}
		})
	}
}
