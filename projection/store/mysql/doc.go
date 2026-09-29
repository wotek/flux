// Package mysql provides a MySQL-backed implementation of [projection.Store]
// that executes read-model mutations and checkpoint position updates in a single
// database transaction.
//
// Concurrency & Atomicity:
// During each Update call, the store begins a MySQL transaction and attaches it
// to the context passed into the mutate callback. Callers must use [TxFromContext]
// to execute their read-model SQL operations on the active transaction. If mutate
// succeeds, the checkpoint position is updated using monotonic max semantics on
// the exact same transaction before committing. If mutate or any subsequent step
// fails, the transaction is rolled back and the checkpoint cursor remains unchanged.
//
// Schema:
// Checkpoints are persisted to the standard checkpoints table defined in
// checkpoint/store/mysql/schema.sql.
//
// For complete setup and architectural guides, see https://flux.keylight.io/backends/mysql.
package mysql
