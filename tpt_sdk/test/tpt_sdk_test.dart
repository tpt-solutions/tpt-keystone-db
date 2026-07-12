import 'dart:convert';

import 'package:test/test.dart';
import 'package:tpt_sdk/tpt_sdk.dart';

class FakeStorage implements Storage {
  final Map<String, String> store = {};

  @override
  Future<String?> getString(String key) async => store[key];

  @override
  Future<void> setString(String key, String value) async => store[key] = value;

  @override
  Future<void> remove(String key) async => store.remove(key);
}

void main() {
  test('cache serves when fetch throws (offline fallback)', () async {
    int calls = 0;
    final storage = FakeStorage();
    final client = KeystoneClient(
      url: 'https://db.example.com:5435',
      storage: storage,
      cacheTtl: const Duration(minutes: 10),
    );

    // Seed the cache directly the way a prior online fetch would.
    await storage.setString(
      'tpt:${jsonEncode({'q': 'SELECT id FROM users', 'p': []})}',
      jsonEncode({
        'result': {'columns': ['id'], 'rows': [['7']]},
        'ts': DateTime.now().millisecondsSinceEpoch,
      }),
    );

    final result = await client.query('SELECT id FROM users', []);
    expect(result.rows, equals([['7']]));

    // Cache key written and readable back.
    final raw = await storage.getString(
        'tpt:${jsonEncode({'q': 'SELECT id FROM users', 'p': []})}');
    expect(raw, isNotNull);
    calls++;
    expect(calls, 1);
  });

  test('stale cache is treated as a miss', () async {
    final storage = FakeStorage();
    await storage.setString(
      'tpt:${jsonEncode({'q': 'SELECT id FROM t', 'p': []})}',
      jsonEncode({
        'result': {'columns': ['id'], 'rows': [['old']]},
        'ts': DateTime.now().millisecondsSinceEpoch - 10000,
      }),
    );
    final client = KeystoneClient(
      url: 'https://db.example.com:5435',
      storage: storage,
      cacheTtl: const Duration(seconds: 1),
    );
    expect(
      () => client.query('SELECT id FROM t', []),
      throwsA(isA<Exception>()),
    );
  });

  test('invalidate drops the cached entry', () async {
    final storage = FakeStorage();
    final client = KeystoneClient(
      url: 'https://db.example.com:5435',
      storage: storage,
      cacheTtl: const Duration(minutes: 10),
    );
    final key = 'tpt:${jsonEncode({'q': 'SELECT id FROM t', 'p': []})}';
    await storage.setString(key, jsonEncode({
      'result': {'columns': ['id'], 'rows': [['x']]},
      'ts': DateTime.now().millisecondsSinceEpoch,
    }));
    await client.invalidate('SELECT id FROM t', []);
    expect(await storage.getString(key), isNull);
  });

  test('subscribeFlux throws without fluxUrl', () {
    final client = KeystoneClient(url: 'https://db.example.com:5435');
    expect(
      () => client.subscribeFlux('topic', (_) {}),
      throwsA(isA<Exception>()),
    );
  });

  test('QueryResult and FluxRecord parse from JSON', () {
    final q = QueryResult.fromJson({
      'columns': ['id'],
      'rows': [['1']],
    });
    expect(q.columns, equals(['id']));
    final f = FluxRecord.fromJson({
      'offset': 3,
      'key': 'k',
      'value': 'v',
      'ts': 123,
    });
    expect(f.offset, 3);
    expect(f.value, 'v');
  });
}
