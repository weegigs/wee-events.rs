//! Runs the conformance test suite against `MemoryStore`.

wee_events::testing::store_test_suite!(memory_store, wee_events::memory::MemoryStore::new());

wee_events::testing::shared_store_test_suite!(memory_store_shared_backing, {
    // Cloning a `MemoryStore` shares the underlying state — same logical
    // persistence layer behind two handles. Replaces the older
    // `from_shared(Arc<Backing>)` pattern.
    let store = wee_events::memory::MemoryStore::new();
    (store.clone(), store)
});
