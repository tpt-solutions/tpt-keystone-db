//! Borrowed row view shared between [`crate::keystone`] and [`crate::ffi`].
//!
//! `tpt-keystone` encodes a row on the wire (and in storage, per
//! `CLAUDE.md`) as a length-prefixed sequence of `Option<Vec<u8>>` cells.
//! [`RowView`] mirrors that shape but borrows straight from the connection's
//! read buffer instead of allocating one `Vec<u8>` per cell — a caller that
//! only needs to inspect a few columns (e.g. Canvas feeding a chart) never
//! pays for a copy it doesn't use. [`RowView::to_owned_row`] escapes to an
//! owned [`crate::keystone::Row`] once the caller needs to keep the data past
//! the borrow's lifetime (e.g. across an `await` point or an FFI boundary).

/// A single row's cells, borrowed from the buffer they were decoded from.
///
/// `None` represents SQL `NULL`; `Some(bytes)` is the cell's text-format
/// wire representation (this SDK always negotiates text format, matching
/// `tpt-keystone`'s simple query protocol).
#[derive(Debug, Clone, Copy)]
pub struct RowView<'a> {
    cells: &'a [Option<Box<[u8]>>],
}

impl<'a> RowView<'a> {
    pub fn new(cells: &'a [Option<Box<[u8]>>]) -> Self {
        Self { cells }
    }

    pub fn len(&self) -> usize {
        self.cells.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cells.is_empty()
    }

    /// The raw bytes of column `i`, or `None` for SQL NULL.
    pub fn get(&self, i: usize) -> Option<&'a [u8]> {
        self.cells.get(i).and_then(|c| c.as_deref())
    }

    /// Column `i` decoded as UTF-8 text (the only format this SDK uses).
    pub fn get_str(&self, i: usize) -> Option<&'a str> {
        self.get(i).and_then(|b| std::str::from_utf8(b).ok())
    }

    pub fn iter(&self) -> impl Iterator<Item = Option<&'a [u8]>> {
        self.cells.iter().map(|c| c.as_deref())
    }

    pub fn to_owned_row(&self) -> Vec<Option<Vec<u8>>> {
        self.cells.iter().map(|c| c.as_ref().map(|b| b.to_vec())).collect()
    }
}
