//! End-to-end tests for Meridian (Phase 6): `GEOMETRY` columns, WKT
//! literals, `ST_*` scalar functions, and `CREATE INDEX ... USING SPATIAL`
//! driving a spatial+temporal index scan through the same
//! `resolve_primary_table` path Phase 2's B-Tree index lookup uses.

use std::sync::Arc;
use std::time::Duration;

use super::execute_query;
use crate::storage::config::NodeRole;
use crate::storage::database::Database;
use crate::storage::lease::LeaseManager;
use crate::storage::objectstore::{LocalFsObjectStore, ObjectStore};

fn test_db() -> (Arc<Database>, tempfile::TempDir, tempfile::TempDir) {
    let bucket = tempfile::tempdir().unwrap();
    let local = tempfile::tempdir().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFsObjectStore::open(bucket.path()).unwrap());
    let lease = Arc::new(LeaseManager::new(
        store.clone(),
        "db",
        "node-1".into(),
        Duration::from_secs(30),
    ));
    lease.try_acquire().unwrap();
    let db = Arc::new(
        Database::open(
            local.path(),
            store,
            lease.handle(),
            NodeRole::Writer,
            Default::default(),
        )
        .unwrap(),
    );
    (db, bucket, local)
}

fn cell_text(cell: &Option<Vec<u8>>) -> String {
    String::from_utf8(cell.clone().unwrap()).unwrap()
}

#[test]
fn geometry_column_round_trips_as_wkt() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE drones (id INT4, pos GEOMETRY)", db.clone()).unwrap();
    execute_query(
        "INSERT INTO drones VALUES (1, 'POINT(-122.4194 37.7749)')",
        db.clone(),
    )
    .unwrap();

    let result = execute_query("SELECT ST_AsText(pos) FROM drones", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "POINT(-122.4194 37.7749)");
}

#[test]
fn st_makepoint_and_st_distance() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_Distance(ST_MakePoint(-0.1276, 51.5074), ST_MakePoint(2.3522, 48.8566))",
        db.clone(),
    )
    .unwrap();
    let dist: f64 = cell_text(&result.rows[0][0]).parse().unwrap();
    assert!(
        (300_000.0..390_000.0).contains(&dist),
        "London-Paris distance got {dist}"
    );
}

#[test]
fn st_dwithin_filters_a_plain_scan() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE drones (id INT4, pos GEOMETRY)", db.clone()).unwrap();
    execute_query(
        "INSERT INTO drones VALUES (1, 'POINT(-122.4194 37.7749)')",
        db.clone(),
    )
    .unwrap(); // San Francisco
    execute_query(
        "INSERT INTO drones VALUES (2, 'POINT(2.3522 48.8566)')",
        db.clone(),
    )
    .unwrap(); // Paris

    let result = execute_query(
        "SELECT id FROM drones WHERE ST_DWithin(pos, ST_MakePoint(-122.4194, 37.7749), 500)",
        db.clone(),
    )
    .unwrap();
    let ids: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(ids, vec!["1"]);
}

/// The Phase 6 milestone query: "find all drones within 500m of a
/// coordinate, between T1 and T2" — driven by a spatial index rather than a
/// full scan. We can't directly assert "single index scan" from SQL, so
/// this instead confirms (a) the spatial index answers the query correctly
/// end-to-end and (b) `Database::spatial_query`/`indexed_column_spatial`
/// are actually exercised by inserting a decoy far outside the radius/time
/// window that a naive non-indexed scan would still have to visit.
#[test]
fn spatial_index_scan_combines_radius_and_time_range() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE drones (id INT4, pos GEOMETRY)", db.clone()).unwrap();
    // Near SF, inside the time window.
    execute_query(
        "INSERT INTO drones VALUES (1, 'POINT(-122.4194 37.7749 0 1000)')",
        db.clone(),
    )
    .unwrap();
    // Near SF, but outside the time window.
    execute_query(
        "INSERT INTO drones VALUES (2, 'POINT(-122.4195 37.7750 0 9000)')",
        db.clone(),
    )
    .unwrap();
    // Far away (Paris), inside the time window.
    execute_query(
        "INSERT INTO drones VALUES (3, 'POINT(2.3522 48.8566 0 1000)')",
        db.clone(),
    )
    .unwrap();

    execute_query("CREATE INDEX ON drones USING SPATIAL (pos)", db.clone()).unwrap();
    assert!(db.indexed_column_spatial("drones", "pos"));

    let result = execute_query(
        "SELECT id FROM drones WHERE ST_DWithin(pos, ST_MakePoint(-122.4194, 37.7749), 500) AND ST_T(pos) BETWEEN 0 AND 2000",
        db.clone(),
    ).unwrap();
    let ids: Vec<String> = result.rows.iter().map(|r| cell_text(&r[0])).collect();
    assert_eq!(ids, vec!["1"]);
}

#[test]
fn st_within_point_in_polygon() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_Within(ST_MakePoint(5, 5), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        db.clone(),
    ).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "t");

    let result = execute_query(
        "SELECT ST_Within(ST_MakePoint(50, 50), ST_GeomFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'))",
        db.clone(),
    ).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "f");
}

#[test]
fn st_geomfromtext_with_srid_round_trips_through_st_srid_and_asewkt() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_SRID(ST_GeomFromText('POINT(1 2)', 4326))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "4326");

    let result = execute_query(
        "SELECT ST_AsEWKT(ST_GeomFromText('POINT(1 2)', 4326))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "SRID=4326;POINT(1 2)");
}

#[test]
fn st_setsrid_overrides_srid() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_SRID(ST_SetSRID(ST_MakePoint(1, 2), 3857))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "3857");
}

#[test]
fn st_srid_defaults_to_zero_when_unset() {
    let (db, _b, _l) = test_db();
    let result = execute_query("SELECT ST_SRID(ST_MakePoint(1, 2))", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "0");
}

#[test]
fn st_transform_4326_to_3857_moves_the_point() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_X(ST_Transform(ST_SetSRID(ST_MakePoint(-0.1276, 51.5074), 4326), 3857))",
        db.clone(),
    )
    .unwrap();
    let x: f64 = cell_text(&result.rows[0][0]).parse().unwrap();
    assert!((-14210.0..-14190.0).contains(&x), "got {x}");
}

#[test]
fn st_transform_without_srid_errors() {
    let (db, _b, _l) = test_db();
    let err = execute_query(
        "SELECT ST_Transform(ST_MakePoint(1, 2), 3857)",
        db.clone(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("known SRID"), "{err}");
}

#[test]
fn st_asbinary_and_st_geomfromwkb_round_trip() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_AsText(ST_GeomFromWKB(ST_AsBinary(ST_MakePoint(1.5, -2.5))))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "POINT(1.5 -2.5)");
}

#[test]
fn st_asewkb_preserves_srid_through_st_geomfromewkb() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_SRID(ST_GeomFromEWKB(ST_AsEWKB(ST_SetSRID(ST_MakePoint(1, 2), 4326))))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "4326");
}

#[test]
fn geography_column_type_is_distinct_from_geometry() {
    let (db, _b, _l) = test_db();
    execute_query(
        "CREATE TABLE zones (id INT4, area GEOGRAPHY)",
        db.clone(),
    )
    .unwrap();
    execute_query(
        "INSERT INTO zones VALUES (1, 'POINT(-122.4194 37.7749)')",
        db.clone(),
    )
    .unwrap();
    let result = execute_query(
        "SELECT data_type FROM information_schema.columns WHERE table_name = 'zones' AND column_name = 'area'",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "geography");

    let result = execute_query("SELECT ST_AsText(area) FROM zones", db.clone()).unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "POINT(-122.4194 37.7749)");
}

// --- Raster (Phase 6 "raster + vector unified storage model") ---

#[test]
fn raster_column_reports_its_own_catalog_type() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE tiles (id INT4, tile RASTER)", db.clone()).unwrap();
    let result = execute_query(
        "SELECT data_type FROM information_schema.columns WHERE table_name = 'tiles' AND column_name = 'tile'",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "raster");
}

#[test]
fn raster_column_round_trips_through_makeemptyraster_and_value() {
    let (db, _b, _l) = test_db();
    execute_query("CREATE TABLE tiles (id INT4, tile RASTER)", db.clone()).unwrap();
    execute_query(
        "INSERT INTO tiles VALUES (1, ST_MakeEmptyRaster(3, 3, 0.0, 0.0, 1.0, 1.0, 4326))",
        db.clone(),
    )
    .unwrap();

    let result = execute_query(
        "SELECT ST_Width(tile), ST_Height(tile), ST_Value(tile, 1, 1) FROM tiles WHERE id = 1",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "3");
    assert_eq!(cell_text(&result.rows[0][1]), "3");
    assert_eq!(cell_text(&result.rows[0][2]), "0");
}

#[test]
fn st_setvalue_updates_a_single_cell() {
    let (db, _b, _l) = test_db();
    let made = execute_query(
        "SELECT ST_SetValue(ST_MakeEmptyRaster(2, 2, 0.0, 0.0, 1.0, 1.0, 0), 0, 0, 99.5)",
        db.clone(),
    )
    .unwrap();
    let hex = cell_text(&made.rows[0][0]);

    execute_query("CREATE TABLE tiles (id INT4, tile RASTER)", db.clone()).unwrap();
    execute_query(
        &format!("INSERT INTO tiles VALUES (1, '{hex}')"),
        db.clone(),
    )
    .unwrap();

    let result = execute_query(
        "SELECT ST_Value(tile, 0, 0), ST_Value(tile, 1, 1) FROM tiles",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "99.5");
    assert_eq!(cell_text(&result.rows[0][1]), "0");
}

#[test]
fn st_asraster_rasterizes_a_polygon_into_filled_cells() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_Width(r), ST_Height(r), ST_Value(r, 0, 0) FROM (SELECT ST_AsRaster(ST_GeomFromText('POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))'), 1.0, 1.0, 7.0) AS r) t",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "4");
    assert_eq!(cell_text(&result.rows[0][1]), "4");
    assert_eq!(cell_text(&result.rows[0][2]), "7");
}

// --- OGC Simple Features scalar functions added alongside the conformance
// suite (`executor/ogc_conformance_tests.rs`) ---

#[test]
fn st_geometrytype_and_dimension_report_per_geometry_kind() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_GeometryType(ST_GeomFromText('POINT(1 2)')), ST_Dimension(ST_GeomFromText('POINT(1 2)')), \
         ST_GeometryType(ST_GeomFromText('LINESTRING(0 0, 1 1)')), ST_Dimension(ST_GeomFromText('LINESTRING(0 0, 1 1)')), \
         ST_GeometryType(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))')), ST_Dimension(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "ST_Point");
    assert_eq!(cell_text(&result.rows[0][1]), "0");
    assert_eq!(cell_text(&result.rows[0][2]), "ST_LineString");
    assert_eq!(cell_text(&result.rows[0][3]), "1");
    assert_eq!(cell_text(&result.rows[0][4]), "ST_Polygon");
    assert_eq!(cell_text(&result.rows[0][5]), "2");
}

#[test]
fn st_envelope_returns_the_bounding_box_polygon() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0, 4 2)')))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(
        cell_text(&result.rows[0][0]),
        "POLYGON((0 0, 4 0, 4 2, 0 2, 0 0))"
    );
}

#[test]
fn st_equals_is_exact_coordinate_equality() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_Equals(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)')), \
         ST_Equals(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(3 4)'))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "t");
    assert_eq!(cell_text(&result.rows[0][1]), "f");
}

#[test]
fn st_isempty_distinguishes_empty_from_populated_geometries() {
    let (db, _b, _l) = test_db();
    let result = execute_query(
        "SELECT ST_IsEmpty(ST_GeomFromText('LINESTRING EMPTY')), ST_IsEmpty(ST_GeomFromText('LINESTRING(0 0, 1 1)'))",
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "t");
    assert_eq!(cell_text(&result.rows[0][1]), "f");
}
