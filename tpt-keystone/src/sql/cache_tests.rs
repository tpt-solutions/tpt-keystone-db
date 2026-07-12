use super::cache::StatementCache;

#[test]
fn cache_hit_after_first_parse() {
    let cache = StatementCache::new(10);
    cache.parse("SELECT 1").unwrap();
    let (entries, hits, misses) = cache.stats();
    assert_eq!(entries, 1);
    assert_eq!(hits, 0);
    assert_eq!(misses, 1);

    cache.parse("SELECT 1").unwrap();
    let (entries, hits, misses) = cache.stats();
    assert_eq!(entries, 1);
    assert_eq!(hits, 1);
    assert_eq!(misses, 1);
}

#[test]
fn cache_lru_eviction_at_capacity() {
    let cache = StatementCache::new(2);
    cache.parse("SELECT 1").unwrap();
    cache.parse("SELECT 2").unwrap();
    cache.parse("SELECT 3").unwrap();

    let (entries, _, _) = cache.stats();
    assert_eq!(entries, 2);
    // The oldest entry ("SELECT 1") should have been evicted: re-parsing it
    // is a miss, not a hit.
    let (_, hits_before, misses_before) = cache.stats();
    cache.parse("SELECT 1").unwrap();
    let (_, hits_after, misses_after) = cache.stats();
    assert_eq!(hits_after, hits_before);
    assert_eq!(misses_after, misses_before + 1);
}

#[test]
fn cache_reuses_entry_on_reaccess() {
    let cache = StatementCache::new(2);
    cache.parse("SELECT 1").unwrap();
    cache.parse("SELECT 2").unwrap();
    // Touch "SELECT 1" again so it's now the most-recently-used entry.
    cache.parse("SELECT 1").unwrap();
    // Inserting a third distinct entry should evict "SELECT 2", not "SELECT 1".
    cache.parse("SELECT 3").unwrap();

    let (_, hits_before, misses_before) = cache.stats();
    cache.parse("SELECT 1").unwrap();
    let (_, hits_after, misses_after) = cache.stats();
    assert_eq!(
        hits_after,
        hits_before + 1,
        "SELECT 1 should still be cached"
    );
    assert_eq!(misses_after, misses_before);
}

#[test]
fn parse_error_not_cached() {
    let cache = StatementCache::new(10);
    assert!(cache.parse("SELECT ~").is_err());
    let (entries, _, misses) = cache.stats();
    assert_eq!(entries, 0);
    assert_eq!(misses, 1);

    // Repeating the same bad SQL is still a miss, not a hit, since nothing
    // was cached for it.
    assert!(cache.parse("SELECT ~").is_err());
    let (entries, hits, misses) = cache.stats();
    assert_eq!(entries, 0);
    assert_eq!(hits, 0);
    assert_eq!(misses, 2);
}
