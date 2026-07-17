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

#[cfg(test)]
mod tests {
    use super::*;

    fn buffer() -> Vec<Option<Box<[u8]>>> {
        vec![
            Some(b"1".to_vec().into_boxed_slice()),
            None,
            Some(b"hello".to_vec().into_boxed_slice()),
        ]
    }

    #[test]
    fn len_and_is_empty_track_cell_count() {
        let buf = buffer();
        let v = RowView::new(&buf);
        assert_eq!(v.len(), 3);
        assert!(!v.is_empty());

        let empty: Vec<Option<Box<[u8]>>> = vec![];
        assert!(RowView::new(&empty).is_empty());
    }

    #[test]
    fn get_returns_raw_bytes_and_null() {
        let one: &[u8] = b"1";
        let hello: &[u8] = b"hello";
        let buf = buffer();
        let v = RowView::new(&buf);
        assert_eq!(v.get(0), Some(one));
        assert_eq!(v.get(1), None);
        assert_eq!(v.get(2), Some(hello));
        // Out-of-bounds reads are None, mirroring SQL semantics.
        assert_eq!(v.get(3), None);
    }

    #[test]
    fn get_str_decodes_utf8_and_skips_invalid() {
        let buf = buffer();
        let v = RowView::new(&buf);
        assert_eq!(v.get_str(0), Some("1"));
        assert_eq!(v.get_str(1), None);
        assert_eq!(v.get_str(2), Some("hello"));

        let bad: Vec<Option<Box<[u8]>>> = vec![Some(vec![0xff, 0xfe].into_boxed_slice())];
        assert_eq!(RowView::new(&bad).get_str(0), None);
    }

    #[test]
    fn iter_yields_each_cell_as_option() {
        let one: &[u8] = b"1";
        let hello: &[u8] = b"hello";
        let buf = buffer();
        let v = RowView::new(&buf);
        let collected: Vec<Option<&[u8]>> = v.iter().collect();
        assert_eq!(collected, vec![Some(one), None, Some(hello)]);
    }

    #[test]
    fn to_owned_escapes_the_borrow_without_copying_values() {
        let buf = buffer();
        let owned = RowView::new(&buf).to_owned_row();
        assert_eq!(owned, vec![
            Some(b"1".to_vec()),
            None,
            Some(b"hello".to_vec()),
        ]);
    }
}
