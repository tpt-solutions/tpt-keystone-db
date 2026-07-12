import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'models.dart';
import 'storage.dart';

/// Client for TPT Keystone's Canvas HTTP/JSON query bridge
/// (`wire::http_query.rs`) with offline-first caching. The Canvas bridge is
/// non-streaming (one JSON response per request); Flux streaming lives in
/// [KeystoneClient.subscribeFlux].
class KeystoneClient {
  KeystoneClient({
    required this.url,
    this.fluxUrl,
    Storage? storage,
    this.cacheTtl = const Duration(minutes: 5),
    this.headers = const {},
  }) : storage = storage ?? InMemoryStorage();

  /// Canvas HTTP/JSON bridge base URL, e.g. `https://db.example.com:5435`.
  final String url;

  /// Flux WebSocket URL, e.g. `wss://db.example.com:5434`.
  final String? fluxUrl;

  final Storage storage;
  final Duration cacheTtl;
  final Map<String, String> headers;

  String _cacheKey(String query, List<dynamic> params) =>
      'tpt:${jsonEncode({'q': query, 'p': params})}';

  /// Runs [query] against Canvas. On a network failure, returns the last
  /// cached result if it is still fresh; otherwise rethrows. Successes refresh
  /// the cache.
  Future<QueryResult> query(String query, [List<dynamic> params = const []]) =>
      _query(query, params, offline: false);

  /// Forces a network round-trip, bypassing and refreshing the cache.
  Future<QueryResult> refresh(String query,
      [List<dynamic> params = const []]) async {
    final result = await _fetch(query, params);
    await _writeCache(_cacheKey(query, params), result);
    return result;
  }

  /// Drops any cached result for a (query, params) pair.
  Future<void> invalidate(String query, [List<dynamic> params = const []]) =>
      storage.remove(_cacheKey(query, params));

  Future<QueryResult> _query(String query, List<dynamic> params,
      {required bool offline}) async {
    final key = _cacheKey(query, params);
    if (!offline) {
      try {
        final result = await _fetch(query, params);
        await _writeCache(key, result);
        return result;
      } on Exception {
        final cached = await _readCache(key);
        if (cached != null) return cached;
        rethrow;
      }
    }
    final cached = await _readCache(key);
    if (cached != null) return cached;
    throw Exception(
        'KeystoneClient.query: offline and no fresh cache for "$query"');
  }

  Future<QueryResult> _fetch(String query, List<dynamic> params) async {
    final client = HttpClient();
    try {
      final request = await client.postUrl(Uri.parse(url));
      request.headers.contentType = ContentType.json;
      headers.forEach((k, v) => request.headers.set(k, v));
      request.write(jsonEncode({'query': query, 'params': params}));
      final response = await request.close();
      if (response.statusCode != 200) {
        throw Exception(
            'KeystoneClient.query: Canvas responded ${response.statusCode}');
      }
      final body = await response.transform(utf8.decoder).join();
      return QueryResult.fromJson(jsonDecode(body) as Map<String, dynamic>);
    } finally {
      client.close(force: true);
    }
  }

  Future<void> _writeCache(String key, QueryResult result) async {
    final entry = jsonEncode({
      'result': {'columns': result.columns, 'rows': result.rows},
      'ts': DateTime.now().millisecondsSinceEpoch,
    });
    await storage.setString(key, entry);
  }

  Future<QueryResult?> _readCache(String key) async {
    final raw = await storage.getString(key);
    if (raw == null) return null;
    final entry = jsonDecode(raw) as Map<String, dynamic>;
    final ts = entry['ts'] as int;
    if (DateTime.now().millisecondsSinceEpoch - ts > cacheTtl.inMilliseconds) {
      return null;
    }
    final result = entry['result'] as Map<String, dynamic>;
    return QueryResult.fromJson(result);
  }

  /// Subscribes to a Flux [topic]. Returns an unsubscribe callback that closes
  /// the socket. Throws if [fluxUrl] is null or no WebSocket is available.
  Future<StreamSubscription<FluxRecord>> subscribeFlux(
    String topic,
    void Function(FluxRecord) onRecord, {
    void Function(Object)? onError,
  }) async {
    if (fluxUrl == null) {
      throw Exception('KeystoneClient.subscribeFlux: no fluxUrl configured');
    }
    final ws = await WebSocket.connect(fluxUrl!);
    ws.add(jsonEncode({'subscribe': topic}));
    final records = ws
        .where((m) => m is String)
        .cast<String>()
        .map((m) => FluxRecord.fromJson(jsonDecode(m) as Map<String, dynamic>));
    final sub = records.listen(
      onRecord,
      onError: onError,
      onDone: () => ws.close(),
    );
    return sub;
  }
}
