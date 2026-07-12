//! OGC Simple Feature Access — SQL Option conformance suite (the Phase 6
//! "OGC Simple Features + SQL/MM Spatial compatibility ... still not
//! attempted: the OGC conformance test suite" TODO item).
//!
//! The real OGC 05-134 (Simple Feature Access — Part 2: SQL Option) Annex A
//! conformance suite is built around a `geometry_columns` catalog view and a
//! specific seven-table dataset (`lakes`/`roads`/`ponds`/`named_places`/
//! `streams`/`buildings`/`bridges` from the OGC's own worked example). This
//! engine has neither a `geometry_columns` catalog view nor that dataset, so
//! this suite is **modeled on** the OGC tests' function coverage and
//! expected-result style — one test per `ST_*` function the spec exercises,
//! against small hand-picked geometries with known correct answers — rather
//! than a verbatim port of the official queries. That distinction matters:
//! passing this suite means "this engine's implemented OGC functions behave
//! correctly," not "this engine passed the official OGC SFA-SQL certification
//! test kit," which would additionally require `geometry_columns`, exact
//! catalog-driven test harness compatibility, and every function below
//! marked "OUT OF SCOPE."
//!
//! Coverage (functions with a real test here): `ST_GeometryType`, `ST_SRID`,
//! `ST_Dimension`, `ST_AsText`, `ST_AsBinary`/`ST_GeomFromWKB`, `ST_IsEmpty`,
//! `ST_Envelope`, `ST_X`/`ST_Y`, `ST_Length`, `ST_Area`, `ST_Equals`,
//! `ST_Within`, `ST_Contains`, `ST_Intersects`, `ST_Distance`.
//!
//! **Explicitly OUT OF SCOPE** (OGC SFA-SQL requires these; this engine does
//! not implement them — not silently skipped, listed here so the gap is
//! honest and searchable): `ST_IsSimple`, `ST_Boundary`, `ST_StartPoint`/
//! `ST_EndPoint`/`ST_IsClosed`/`ST_IsRing`, `ST_NumPoints`/`ST_PointN`,
//! `ST_Centroid`/`ST_PointOnSurface`, `ST_ExteriorRing`/
//! `ST_NumInteriorRing`/`ST_InteriorRingN`, any `MULTI*`/`GEOMETRYCOLLECTION`
//! type and its accessors (`ST_NumGeometries`/`ST_GeometryN`), `ST_Disjoint`/
//! `ST_Touches`/`ST_Overlaps`/`ST_Crosses`/`ST_Relate` (DE-9IM predicates
//! beyond the four implemented above), and every boolean-set-operation
//! function (`ST_Buffer`/`ST_ConvexHull`/`ST_Intersection`/`ST_Union`/
//! `ST_Difference`/`ST_SymDifference`) — `geo::geometry`'s own module doc
//! already lists "no buffering, no polygon boolean ops" as a scope cut.
//! `ST_Intersects`/`ST_Equals` are also narrower than the OGC spec's exact
//! semantics (bbox-only, and exact-coordinate-order respectively) — see
//! their own doc comments in `executor::eval`.

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

fn scalar(db: &Arc<Database>, sql: &str) -> String {
    let result = execute_query(sql, db.clone()).unwrap();
    cell_text(&result.rows[0][0])
}

// --- Conformance Test: Basic geometry accessors (OGC 05-134 §A.3, tests on
// `ST_GeometryType`/`ST_SRID`/`ST_Dimension`/`ST_AsText`) ---

#[test]
fn conformance_st_geometrytype_reports_the_ogc_type_name() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(&db, "SELECT ST_GeometryType(ST_GeomFromText('POINT(0 0)'))"),
        "ST_Point"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_GeometryType(ST_GeomFromText('LINESTRING(0 0, 1 1)'))"
        ),
        "ST_LineString"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_GeometryType(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'))"
        ),
        "ST_Polygon"
    );
}

#[test]
fn conformance_st_srid_round_trips_through_geomfromtext() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_SRID(ST_GeomFromText('POINT(-71.064544 42.28787)', 4326))"
        ),
        "4326"
    );
    // No SRID given: OGC/PostGIS convention is SRID 0 ("unspecified").
    assert_eq!(
        scalar(&db, "SELECT ST_SRID(ST_GeomFromText('POINT(1 1)'))"),
        "0"
    );
}

#[test]
fn conformance_st_dimension_matches_topological_dimension() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(&db, "SELECT ST_Dimension(ST_GeomFromText('POINT(0 0)'))"),
        "0"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_Dimension(ST_GeomFromText('LINESTRING(0 0, 1 1)'))"
        ),
        "1"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_Dimension(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 0))'))"
        ),
        "2"
    );
}

#[test]
fn conformance_st_astext_round_trips_wkt() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_AsText(ST_GeomFromText('POINT(-71.064544 42.28787)'))"
        ),
        "POINT(-71.064544 42.28787)"
    );
}

// --- Conformance Test: WKB I/O (OGC 05-134 §A.3, `ST_AsBinary`/`ST_GeomFromWKB`) ---

#[test]
fn conformance_st_asbinary_and_geomfromwkb_round_trip() {
    let (db, _b, _l) = test_db();
    let hex = scalar(&db, "SELECT ST_AsBinary(ST_GeomFromText('POINT(1 2)'))");
    let result = execute_query(
        &format!("SELECT ST_AsText(ST_GeomFromWKB('{hex}'))"),
        db.clone(),
    )
    .unwrap();
    assert_eq!(cell_text(&result.rows[0][0]), "POINT(1 2)");
}

// --- Conformance Test: emptiness and envelope (OGC 05-134 §A.3, `ST_IsEmpty`/`ST_Envelope`) ---

#[test]
fn conformance_st_isempty_true_only_for_empty_linestrings_and_polygons() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_IsEmpty(ST_GeomFromText('LINESTRING EMPTY'))"
        ),
        "t"
    );
    assert_eq!(
        scalar(&db, "SELECT ST_IsEmpty(ST_GeomFromText('POLYGON EMPTY'))"),
        "t"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_IsEmpty(ST_GeomFromText('LINESTRING(0 0, 1 1)'))"
        ),
        "f"
    );
    // A POINT can never be empty in this engine's `Geometry::Point(Coord)`
    // model — see `geo::geometry::Geometry::from_wkt`'s doc comment on the
    // `POINT EMPTY` gap.
    assert_eq!(
        scalar(&db, "SELECT ST_IsEmpty(ST_GeomFromText('POINT(0 0)'))"),
        "f"
    );
}

#[test]
fn conformance_st_envelope_of_a_linestring_is_its_bbox_rectangle() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_AsText(ST_Envelope(ST_GeomFromText('LINESTRING(0 0, 4 2)')))"
        ),
        "POLYGON((0 0, 4 0, 4 2, 0 2, 0 0))"
    );
}

// --- Conformance Test: coordinate accessors (OGC 05-134 §A.3, `ST_X`/`ST_Y`) ---

#[test]
fn conformance_st_x_and_st_y_read_back_a_points_ordinates() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_X(ST_GeomFromText('POINT(-71.064544 42.28787)'))"
        ),
        "-71.064544"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_Y(ST_GeomFromText('POINT(-71.064544 42.28787)'))"
        ),
        "42.28787"
    );
}

// --- Conformance Test: measurement functions (OGC 05-134 §A.3, `ST_Length`/`ST_Area`) ---

#[test]
fn conformance_st_length_sums_linestring_segment_distances() {
    let (db, _b, _l) = test_db();
    // A 1-degree-of-longitude segment on the equator: haversine distance
    // should be close to 111.32 km (WGS84-ish great-circle length).
    let len = scalar(
        &db,
        "SELECT ST_Length(ST_GeomFromText('LINESTRING(0 0, 1 0)'))",
    )
    .parse::<f64>()
    .unwrap();
    assert!(
        (len - 111_195.0).abs() < 2_000.0,
        "expected ~111.2km, got {len}m"
    );
}

#[test]
fn conformance_st_area_of_a_unit_square_is_one() {
    let (db, _b, _l) = test_db();
    // Planar shoelace area — see `st_area`'s own doc comment: not geodesic.
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_Area(ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))'))"
        ),
        "1"
    );
}

// --- Conformance Test: spatial predicates (OGC 05-134 §A.3, `ST_Equals`/
// `ST_Within`/`ST_Contains`/`ST_Intersects`) ---

#[test]
fn conformance_st_equals_true_for_identical_points_false_otherwise() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_Equals(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(1 2)'))"
        ),
        "t"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_Equals(ST_GeomFromText('POINT(1 2)'), ST_GeomFromText('POINT(9 9)'))"
        ),
        "f"
    );
}

#[test]
fn conformance_st_within_and_st_contains_are_inverse_point_in_polygon_tests() {
    let (db, _b, _l) = test_db();
    let square = "ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')";
    let inside = "ST_GeomFromText('POINT(5 5)')";
    let outside = "ST_GeomFromText('POINT(50 50)')";

    assert_eq!(
        scalar(&db, &format!("SELECT ST_Within({inside}, {square})")),
        "t"
    );
    assert_eq!(
        scalar(&db, &format!("SELECT ST_Contains({square}, {inside})")),
        "t"
    );
    assert_eq!(
        scalar(&db, &format!("SELECT ST_Within({outside}, {square})")),
        "f"
    );
}

#[test]
fn conformance_st_intersects_true_for_overlapping_bboxes() {
    let (db, _b, _l) = test_db();
    let a = "ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')";
    let overlapping = "ST_GeomFromText('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))')";
    let disjoint = "ST_GeomFromText('POLYGON((100 100, 110 100, 110 110, 100 110, 100 100))')";

    assert_eq!(
        scalar(&db, &format!("SELECT ST_Intersects({a}, {overlapping})")),
        "t"
    );
    assert_eq!(
        scalar(&db, &format!("SELECT ST_Intersects({a}, {disjoint})")),
        "f"
    );
}

// --- Conformance Test: distance (OGC 05-134 §A.3, `ST_Distance`) ---

#[test]
fn conformance_st_distance_between_coincident_points_is_zero() {
    let (db, _b, _l) = test_db();
    assert_eq!(
        scalar(
            &db,
            "SELECT ST_Distance(ST_GeomFromText('POINT(1 1)'), ST_GeomFromText('POINT(1 1)'))"
        ),
        "0"
    );
}
