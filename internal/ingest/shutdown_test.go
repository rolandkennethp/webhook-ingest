package ingest_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// TestShutdownWaitsForInFlightRecordingProcessing proves that Service.Shutdown
// blocks until the background recording-processing goroutine spawned by
// Ingest has actually finished, rather than returning immediately and
// letting that work be silently dropped (the bug behind production symptom
// #4: "in-flight work disappears on deploy").
func TestShutdownWaitsForInFlightRecordingProcessing(t *testing.T) {
	srv, st, svc := testutil.NewServerWithService(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID) // has a recording_url
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var processed bool
	row := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("expected recording_processed to be true after Shutdown returned")
	}
}