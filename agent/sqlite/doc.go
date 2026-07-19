// Package sqlite provides the official local SQLite adapter for agent.Store.
// It owns one dedicated database and its cross-process ownership lock until the
// returned framework.Resource is released.
package sqlite
