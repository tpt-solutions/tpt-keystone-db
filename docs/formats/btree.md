# Local B-Tree Secondary Index Format

Source: `tpt-keystone/src/storage/btree.rs`. Local per-node only — not
replicated through the shared object store (same scope cut documented for
the Meridian/Chronos/Plexus secondary indexes).

One B-Tree index is one file. Order (max children per internal node) is a
fixed constant, `ORDER = 64`.

## File layout

```
bytes 0..8    root pointer: u64 big-endian file offset of the root node
              (0 is used as the "no root yet" sentinel — an empty tree has
              this header written as all-zero and no nodes follow it)
bytes 8..     nodes, each addressed by its own byte offset (used as the
              node "ID" everywhere — parent nodes store child offsets, not
              a separate index)
```

Nodes are never relocated once written, **except** that updating an existing
leaf/internal node in place without a split rewrites it at its *original*
offset (`write_node_at`) — same size in, same size out is not guaranteed
(the node just gets fully rewritten there), which works because a node's
serialized size is only ever compared against `ORDER`, not against the
previous on-disk size of that exact node, and old bytes beyond the new
node's length are simply left as file garbage past the new EOF-relevant
boundary if the node shrinks (this doesn't happen in this code's insert-only
paths, but a decoder should not assume "the immediately-following bytes
after any node are certainly a different node" without also tracking which
offsets are still reachable from the current root).

A newly-created node is instead appended at the file's current end
(`write_node`, which uses `stream_position()` as the new node's offset).

## Node encoding

```
u8   tag              (0 = leaf, 1 = internal)
u32  count             (number of keys in this node)
```

### Leaf node (`tag == 0`)

```
count * { u32 key_len, <key_len> bytes key }     -- all keys, in order
count * { u32 val_len, <val_len> bytes value }    -- all values (primary keys), same order
```

Keys and values are stored as two separate parallel arrays (all keys first,
then all values), not interleaved per entry.

### Internal node (`tag == 1`)

```
count * { u32 key_len, <key_len> bytes key }     -- separator keys, in order
(count + 1) * u64 child_offset                    -- child node file offsets
```

Standard B-Tree separator-key semantics: `children[i]` covers keys `<
keys[i]`, `children[i+1]` covers keys in `[keys[i], keys[i+1])`, and
`children[count]` covers keys `>= keys[count-1]`.

## Split behavior

A leaf or internal node splits when its key count reaches `ORDER` (64):

- **Leaf split**: split at `mid = len/2`. The middle key is promoted to the
  parent *and* duplicated as the first key of the new right leaf (leaf
  splits duplicate the separator, matching a B+Tree-style leaf split, even
  though internal nodes don't otherwise carry a linked-leaf structure).
- **Internal split**: split at `mid = len/2`. The middle key is promoted to
  the parent and is **not** duplicated in either child (true B-Tree internal
  split — only leaves duplicate the separator).

A root split creates a brand-new internal node with one key and two
children, written at a new offset, and the header's root pointer is updated
to point at it.

## Known caveat for reimplementers

The root-pointer header (bytes 0..8) is a reserved slot that must be written
*before* the first node, otherwise the first node would be written at offset
0 and immediately clobbered by the header write that follows it. The current
implementation writes an 8-byte zero placeholder at offset 0 before writing
the very first leaf specifically to guard against this. Any reimplementation
that writes nodes starting from a freshly-created (zero-length) file must do
the same — reserve/skip the first 8 bytes before writing any node.
