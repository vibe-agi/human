package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

func TestPingQueriesLiveDatabaseAndFailsAfterClose(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Ping(context.Background()); err != nil {
		t.Fatalf("ping open database: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Ping(context.Background()); err == nil {
		t.Fatal("ping succeeded after database close")
	}
}

func TestPingFailsWhenRuntimeSchemaIsDamaged(t *testing.T) {
	database, err := Open(context.Background(), filepath.Join(t.TempDir(), "gateway.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.db.ExecContext(context.Background(), `DROP TABLE human_schema`); err != nil {
		t.Fatal(err)
	}
	if err := database.Ping(context.Background()); err == nil {
		t.Fatal("ping succeeded after the gateway schema marker was removed")
	}
}
