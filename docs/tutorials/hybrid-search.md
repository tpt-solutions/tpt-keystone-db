# Hybrid search: vector + BM25 full-text, fused

This walks through Prism's k-NN vector search, Canopy's BM25 full-text
ranking, and `hybrid_search`, which fuses the two into one ranked result.
It reproduces the scenario in
`tpt-keystone/src/executor/prism_tests.rs::hybrid_search_fuses_vector_and_bm25_rankings`,
so you can compare your own output against a known-good run.

## Set up a table with both a vector and a text column

```sql
CREATE TABLE docs (id INT4, label TEXT, body TEXT, embedding VECTOR);

INSERT INTO docs VALUES (1, 'a', 'rust systems programming rust rust', '[1.0,0.0,0.0]');
INSERT INTO docs VALUES (2, 'b', 'python scripting language',          '[0.95,0.05,0.0]');
INSERT INTO docs VALUES (3, 'c', 'rust programming language',          '[-1.0,-1.0,-1.0]');
INSERT INTO docs VALUES (4, 'd', 'cooking recipes and food',           '[-0.9,-0.9,-1.0]');

CREATE INDEX ON docs USING VECTOR (embedding) WITH (metric = 'l2');
CREATE INDEX ON docs USING GIN (body);
```

Row `a` is both the nearest vector neighbor to `[1.0,0.0,0.0]` *and* the
strongest text match for "rust" (three mentions). Row `b` is vector-near but
never mentions "rust". Row `c` mentions "rust" but is vector-far. Row `d` is
unrelated on both signals.

## Vector search alone

```sql
SELECT label, distance FROM vector_search('docs', 'embedding', '[1.0,0.0,0.0]', 3);
```

Nearest-first by L2 distance: `a`, then `b`, then whichever of `c`/`d` is
closer to the query point (both are far, in the opposite octant).

## BM25 full-text search alone

There's no SQL surface for ranked-only BM25 yet (`json_text_search` stays
AND-only, unranked) — it's reached via the storage API directly, or through
`hybrid_search` below. `Database::fts_search_bm25("docs", "body", "rust", 10)`
ranks `a` above `c` (three mentions in a shorter document beats one mention
in a longer one) and excludes `b`/`d` entirely (zero mentions of "rust").

## Hybrid: `hybrid_search`

```sql
SELECT label, vec_distance, bm25_score, fused_score
FROM hybrid_search('docs', 'embedding', '[1.0,0.0,0.0]', 'body', 'rust', 3);
```

Expected result, ranked by `fused_score` descending:

| label | vec_distance | bm25_score | fused_score |
|---|---|---|---|
| `a` | small (nearest neighbor) | highest (3 mentions, shortest doc) | highest — wins on **both** signals |
| `b` | small | `NULL` (zero mentions) | present via vector rank alone |
| `c` | large | present (1 mention) | present via BM25 rank alone |

Row `d` doesn't appear — it isn't a top-`pool` candidate on either the
vector or BM25 side, so it never enters the fusion at all.

## How the fusion works

`hybrid_search` runs the vector k-NN search and the BM25 search
independently (each over a wider candidate pool than the requested `k`, so
rows strong in only one signal still have a chance to surface), then
combines the two rankings with **Reciprocal Rank Fusion**:

```
fused_score(row) = Σ 1 / (60 + rank)
```

summed over whichever of the two ranked lists the row appears in (1-indexed
rank within that list; a row present in both lists gets both terms added).
`60` is RRF's standard constant from the original paper — there's no
tunable weight between the vector and text signals, deliberately, so you
don't need to guess a blend ratio for your data. `vec_distance`/
`bm25_score` are `NULL` for whichever signal didn't surface that row, so you
can tell "won on both" apart from "won on one" in the output.

## Requirements and errors

`hybrid_search` needs **both** a `VECTOR` index on the vector column and a
`GIN`/`FTS` index on the text column — it errors clearly (`no vector index
on ...` / `no full-text index on ...`) rather than silently degrading to
one signal if either index is missing.
