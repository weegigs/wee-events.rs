//! Runs the conformance test suite against `MemoryStore`.

wee_events::store_test_suite!(memory_store, wee_events::MemoryStore::new());
