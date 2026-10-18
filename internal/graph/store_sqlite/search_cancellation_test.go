package store_sqlite

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/zzet/gortex/internal/graph"
	"github.com/zzet/gortex/internal/query"
	"github.com/zzet/gortex/internal/search"
)

func TestSearchCancellationReleasesPoolWait(t *testing.T) {
	for _, term := range []string{"WidgetExtensions", "widget extensions"} {
		t.Run(term, func(t *testing.T) {
			s, err := Open(filepath.Join(t.TempDir(), "search.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			n := &graph.Node{ID: "app/widget.go::WidgetExtensions", Name: "WidgetExtensions", Kind: graph.KindType, RepoPrefix: "app", FilePath: "app/widget.go"}
			s.AddNode(n)
			if err := s.UpsertSymbolFTS(n.ID, "widget extensions"); err != nil {
				t.Fatal(err)
			}
			e := query.NewEngine(s)
			e.SetSearch(search.NewSwappable(search.NewSymbolSearcherBackend(s)))
			opts := query.QueryOptions{RepoAllow: map[string]bool{"app": true}, SkipInnerRerank: true, SkipVectorChannel: true}
			// Positive control proves the scoped production backend is active, and warms
			// its memoised corpus gate before deliberately exhausting the private pool.
			if got := e.GatherSymbolCandidates(term, 5, opts, nil); len(got) != 1 || got[0].Node.ID != n.ID {
				t.Fatalf("positive scoped control: %#v", got)
			}
			s.db.SetMaxOpenConns(1)
			conn, err := s.db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			before := s.db.Stats().WaitCount
			go func() { defer close(done); e.WithContext(ctx).GatherSymbolCandidates(term, 5, opts, nil) }()
			wait := time.NewTimer(time.Second)
			defer wait.Stop()
			for s.db.Stats().WaitCount == before {
				select {
				case <-wait.C:
					conn.Close()
					<-done
					t.Fatal("query never reached held connection")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(300 * time.Millisecond):
				conn.Close()
				<-done
				t.Fatal("cancelled search kept waiting for the private SQLite connection")
			}
			conn.Close()
			if got := e.GatherSymbolCandidates(term, 5, opts, nil); len(got) != 1 {
				t.Fatalf("active context polluted by cancellation: %#v", got)
			}
		})
	}
}

// The indexed corpus can be answered immediately while an exact view layer
// waits on SQLite. This exercises the second FTS hop, not just the base lane.
type emptyIndexedCorpus struct{}

func (emptyIndexedCorpus) Add(string, ...string)                    {}
func (emptyIndexedCorpus) Remove(string)                            {}
func (emptyIndexedCorpus) Close()                                   {}
func (emptyIndexedCorpus) Count() int                               { return 1 }
func (emptyIndexedCorpus) Search(string, int) []search.SearchResult { return nil }

func TestViewLayerSearchCancellationReleasesPoolWait(t *testing.T) {
	base, derived := openGenerationReadPair(t)
	// Same logical ID, distinct generation payloads: a generation-blind query
	// cannot pass the positive controls.
	for _, store := range []*Store{base, derived} {
		hits, err := store.SearchSymbolBundlesContext(context.Background(), "Shared", 5)
		if err != nil || len(hits) != 1 {
			t.Fatalf("generation scoped FTS: %v %v", hits, err)
		}
		if hits[0].Node.Name != "Shared"+map[*Store]string{base: genZeroMark, derived: genOneMark}[store] {
			t.Fatalf("wrong generation payload: %+v", hits[0].Node)
		}
	}
	e := query.NewEngine(derived)
	e.SetSearch(search.NewSwappable(emptyIndexedCorpus{}))
	e = e.WithViewLayers(derived, []query.ViewLayerSource{{Search: search.NewSymbolSearcherBackend(derived)}})
	opts := query.QueryOptions{SkipInnerRerank: true, SkipVectorChannel: true, SkipExactNameSplice: true}
	if got := e.GatherSymbolCandidates("Shared", 5, opts, nil); len(got) != 1 {
		t.Fatalf("positive layer control: %#v", got)
	}
	base.db.SetMaxOpenConns(1)
	conn, err := base.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	before := base.db.Stats().WaitCount
	go func() { defer close(done); e.WithContext(ctx).GatherSymbolCandidates("Shared", 5, opts, nil) }()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for base.db.Stats().WaitCount == before {
		select {
		case <-timeout.C:
			conn.Close()
			<-done
			t.Fatal("layer never reached held connection")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		conn.Close()
		<-done
		t.Fatal("cancelled exact-view layer kept waiting")
	}
}

func TestSearchBundlesContextPreservesEdgeMetadata(t *testing.T) {
	s := floodedScopeStore(t, 0)
	id := "app/widget.go::WidgetExtensions"
	other := "app/other.go::Caller"
	s.AddNode(&graph.Node{ID: other, Name: "Caller", Kind: graph.KindFunction, RepoPrefix: "app", FilePath: "app/other.go"})
	for _, ends := range [][2]string{{id, other}, {other, id}} {
		s.AddEdge(&graph.Edge{From: ends[0], To: ends[1], Kind: graph.EdgeCalls, FilePath: "app/widget.go", Line: 1, Meta: map[string]any{"similarity": 0.75, "resolve_terminal": true, "resolve_terminal_reason": "test", "semantic_source": "fixture"}})
	}
	expectedOut := s.GetOutEdgesByNodeIDs([]string{id})[id]
	expectedIn := s.GetInEdgesByNodeIDs([]string{id})[id]
	if len(expectedOut) != 1 || len(expectedIn) != 1 || expectedOut[0].Meta["similarity"] != 0.75 {
		t.Fatal("full edge positive control failed")
	}
	bundles, err := s.SearchSymbolBundlesRepoScopedContext(context.Background(), "widget extensions", []string{"app"}, 5)
	if err != nil || len(bundles) != 1 {
		t.Fatalf("bundle: %v %v", bundles, err)
	}
	if !reflect.DeepEqual(bundles[0].OutEdges, expectedOut) || !reflect.DeepEqual(bundles[0].InEdges, expectedIn) {
		t.Fatalf("bundle metadata lost: out=%+v in=%+v", bundles[0].OutEdges[0].Meta, bundles[0].InEdges[0].Meta)
	}
}
