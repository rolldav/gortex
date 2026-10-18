package mcp

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcplib "github.com/mark3labs/mcp-go/mcp"
	"github.com/zzet/gortex/internal/graph"
	"github.com/zzet/gortex/internal/graph/store_sqlite"
	"github.com/zzet/gortex/internal/query"
	"github.com/zzet/gortex/internal/search"
)

// A controlled wait at the store boundary lets this test isolate the real
// middleware -> engineFor -> Swappable context chain without private DB access.
// The storage package separately tests actual exhausted SQLite pool waits.
type deadlineSearchStore struct {
	*store_sqlite.Store
	entered chan struct{}
	release chan struct{}
}

func (s *deadlineSearchStore) SearchSymbolBundlesRepoScoped(q string, allow []string, limit int) ([]graph.SymbolBundle, error) {
	close(s.entered)
	<-s.release
	return s.Store.SearchSymbolBundlesRepoScoped(q, allow, limit)
}

func (s *deadlineSearchStore) SearchSymbolBundlesRepoScopedContext(ctx context.Context, q string, allow []string, limit int) ([]graph.SymbolBundle, error) {
	close(s.entered)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.release:
	}
	return s.Store.SearchSymbolBundlesRepoScopedContext(ctx, q, allow, limit)
}

func TestSearchDeadlineReleasesMCPHandler(t *testing.T) {
	srv, _ := setupTestServer(t)
	store, err := store_sqlite.Open(filepath.Join(t.TempDir(), "search.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	n := &graph.Node{ID: "app/widget.go::WidgetExtensions", Name: "WidgetExtensions", Kind: graph.KindType, RepoPrefix: "app", FilePath: "app/widget.go"}
	store.AddNode(n)
	if err := store.UpsertSymbolFTS(n.ID, "widget extensions"); err != nil {
		t.Fatal(err)
	}
	gated := &deadlineSearchStore{Store: store, entered: make(chan struct{}), release: make(chan struct{})}
	defer close(gated.release)
	srv.engine = query.NewEngine(store)
	srv.engine.SetSearch(search.NewSwappable(search.NewSymbolSearcherBackend(gated)))
	opts := query.QueryOptions{RepoAllow: map[string]bool{"app": true}, SkipInnerRerank: true, SkipVectorChannel: true}
	// Check the real scoped SQL separately before enabling the controlled wait.
	if hits, err := store.SearchSymbolBundlesRepoScoped("widget extensions", []string{"app"}, 5); err != nil || len(hits) != 1 {
		t.Fatalf("positive scoped SQL control: %v %v", hits, err)
	}
	srv.ToolCallTimeout = 100 * time.Millisecond
	finished := make(chan struct{})
	srv.addTool(mcplib.NewTool("test_cancellable_sql_search"), func(ctx context.Context, _ mcplib.CallToolRequest) (*mcplib.CallToolResult, error) {
		defer close(finished)
		srv.engineFor(ctx).GatherSymbolCandidates("widget extensions", 5, opts, nil)
		return mcplib.NewToolResultText("returned"), nil
	})
	result := make(chan *mcplib.CallToolResult, 1)
	go func() { result <- callToolByName(t, srv, context.Background(), "test_cancellable_sql_search", nil) }()
	select {
	case <-gated.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("MCP handler did not reach scoped store")
	}
	select {
	case res := <-result:
		if !res.IsError || !strings.Contains(toolText(res), "deadline") {
			t.Error("expired search returned a successful terminal result")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deadline middleware did not release transport")
	}
	select {
	case <-finished:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("deadline returned while search worker kept running")
	}
}
