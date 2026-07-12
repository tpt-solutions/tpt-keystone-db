/// Result of a Canvas query.
class QueryResult {
  const QueryResult({required this.columns, required this.rows});

  factory QueryResult.fromJson(Map<String, dynamic> json) => QueryResult(
        columns: List<String>.from(json['columns'] as List),
        rows: (json['rows'] as List)
            .map((r) => List<dynamic>.from(r as List))
            .toList(),
      );

  final List<String> columns;
  final List<dynamic> rows;
}

/// A single record pushed over a Flux stream.
class FluxRecord {
  const FluxRecord({
    required this.offset,
    this.key,
    this.value,
    required this.ts,
  });

  factory FluxRecord.fromJson(Map<String, dynamic> json) => FluxRecord(
        offset: json['offset'] as int,
        key: json['key'] as String?,
        value: json['value'] as String?,
        ts: json['ts'] as int,
      );

  final int offset;
  final String? key;
  final String? value;
  final int ts;
}
