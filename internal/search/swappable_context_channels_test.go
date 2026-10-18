package search

import (
	"context"
	"testing"
)

type splitChannelBackend struct{ fusedCalls, channelCalls int }

func (*splitChannelBackend) Add(string, ...string) {}
func (*splitChannelBackend) Remove(string)         {}
func (*splitChannelBackend) Close()                {}
func (*splitChannelBackend) Count() int            { return 1 }
func (*splitChannelBackend) Search(string, int) []SearchResult {
	return []SearchResult{{ID: "legacy-fused"}}
}
func (b *splitChannelBackend) SearchContext(context.Context, string, int) []SearchResult {
	b.fusedCalls++
	return []SearchResult{{ID: "fused"}}
}
func (b *splitChannelBackend) SearchChannels(string, int) ([]SearchResult, []string) {
	b.channelCalls++
	return []SearchResult{{ID: "text"}}, []string{"vector"}
}

type contextSplitChannelBackend struct {
	*splitChannelBackend
	ctx   context.Context
	calls int
}

func (b *contextSplitChannelBackend) SearchChannelsTimedContext(ctx context.Context, _ string, _ int) ([]SearchResult, []string, ChannelTimings) {
	b.ctx = ctx
	b.calls++
	return []SearchResult{{ID: "context-text"}}, []string{"context-vector"}, ChannelTimings{TextMS: 7}
}

func TestSwappableContextPreservesSeparateChannels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, contextual := range []bool{false, true} {
		name := "legacy channels"
		if contextual {
			name = "context channels"
		}
		t.Run(name, func(t *testing.T) {
			base := &splitChannelBackend{}
			var backend Backend = base
			contextualBackend := &contextSplitChannelBackend{splitChannelBackend: base}
			wantText, wantVector := "text", "vector"
			if contextual {
				backend = contextualBackend
				wantText, wantVector = "context-text", "context-vector"
			}
			text, vector, timings := NewSwappable(backend).SearchChannelsTimedContext(ctx, "query", 5)
			if len(text) != 1 || text[0].ID != wantText || len(vector) != 1 || vector[0] != wantVector {
				t.Fatalf("channels = %v %v, want %s %s", text, vector, wantText, wantVector)
			}
			if base.fusedCalls != 0 {
				t.Fatalf("fused search invoked %d times", base.fusedCalls)
			}
			if contextual {
				if contextualBackend.calls != 1 || contextualBackend.ctx != ctx || base.channelCalls != 0 || timings.TextMS != 7 {
					t.Fatal("context channel priority/context/timing lost")
				}
			} else if base.channelCalls != 1 {
				t.Fatal("legacy channel was bypassed")
			}
		})
	}
}
