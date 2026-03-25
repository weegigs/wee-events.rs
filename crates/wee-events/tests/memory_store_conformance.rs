//! Runs the conformance test suite against `MemoryStore`.

wee_events::testing::store_test_suite!(memory_store, wee_events::memory::MemoryStore::new());
