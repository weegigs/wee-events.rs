//! Runs the conformance test suite against `SqliteEventStore`.

use wee_events_sqlite::SqliteEventStore;

wee_events::store_test_suite!(sqlite_store, SqliteEventStore::open_in_memory().unwrap());
