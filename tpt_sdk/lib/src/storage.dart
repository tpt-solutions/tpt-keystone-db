/// Offline cache storage. Mirrors the SharedPreferences shape (string values
/// keyed by string) so callers can plug in `shared_preferences` directly. The
/// default [InMemoryStorage] is not persisted across app restarts.
abstract class Storage {
  Future<String?> getString(String key);
  Future<void> setString(String key, String value);
  Future<void> remove(String key);
}

/// Non-persistent in-memory [Storage] used when no adapter is supplied.
class InMemoryStorage implements Storage {
  final Map<String, String> _store = {};

  @override
  Future<String?> getString(String key) async => _store[key];

  @override
  Future<void> setString(String key, String value) async => _store[key] = value;

  @override
  Future<void> remove(String key) async => _store.remove(key);
}
