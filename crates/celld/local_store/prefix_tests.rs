// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use futures_util::TryStreamExt as _;
use rusqlite::StatementStatus;

struct Fixture {
    _directory: tempfile::TempDir,
    store: LocalStore,
}

impl Fixture {
    fn new(keys: &[&str]) -> Self {
        crate::test_runtime::init();
        let directory = tempfile::tempdir().unwrap();
        let store = LocalStore::open(directory.path().join("objects.sqlite3")).unwrap();
        let fixture = Self {
            _directory: directory,
            store,
        };
        for key in keys {
            fixture.insert(key, "1");
        }
        fixture
    }

    fn insert(&self, key: &str, etag: &str) {
        self.store
            .connect()
            .unwrap()
            .execute(
                "INSERT INTO objects(key, body, etag, modified_ms, attributes)
                 VALUES (?1, X'', ?2, 0, '[]')",
                params![key, etag],
            )
            .unwrap();
    }

    async fn list(&self, prefix: Option<&Path>) -> object_store::Result<Vec<String>> {
        Ok(self
            .store
            .list(prefix)
            .try_collect::<Vec<_>>()
            .await?
            .iter()
            .map(|meta| meta.location.to_string())
            .collect())
    }

    async fn page(&self, prefix: &str, options: PaginatedListOptions) -> PaginatedListResult {
        self.store
            .list_paginated(Some(prefix), options)
            .await
            .unwrap()
    }
}

fn objects(result: &ListResult) -> Vec<String> {
    result
        .objects
        .iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

fn prefixes(result: &ListResult) -> Vec<String> {
    result.common_prefixes.iter().map(Path::to_string).collect()
}

#[tokio::test]
async fn directory_listings_keep_component_boundaries_and_literal_names() {
    let fixture = Fixture::new(&[
        "nodes",
        "nodes/a",
        "nodes/a/b",
        "nodes/b",
        "nodes-x/a",
        "nodes0/a",
        "Nodes/a",
        "a_c/x",
        "abc/x",
        "a%b/x",
        "axb/x",
        "it's/x",
    ]);
    for (prefix, expected) in [
        ("nodes", vec!["nodes/a", "nodes/a/b", "nodes/b"]),
        ("nodes/a", vec!["nodes/a/b"]),
        ("Nodes", vec!["Nodes/a"]),
        ("a_c", vec!["a_c/x"]),
        ("a%b", vec!["a%b/x"]),
        ("it's", vec!["it's/x"]),
        ("missing", vec![]),
    ] {
        assert_eq!(
            fixture
                .list(Some(&Path::parse(prefix).unwrap()))
                .await
                .unwrap(),
            expected,
            "prefix {prefix:?}"
        );
    }
    let all = fixture.list(None).await.unwrap();
    assert_eq!(all.len(), 12);
    assert_eq!(fixture.list(Some(&Path::default())).await.unwrap(), all);

    let delimited = fixture
        .store
        .list_with_delimiter(Some(&Path::from("nodes")))
        .await
        .unwrap();
    assert_eq!(objects(&delimited), ["nodes/a", "nodes/b"]);
    assert_eq!(prefixes(&delimited), ["nodes/a"]);

    let offset: Vec<_> = fixture
        .store
        .list_with_offset(Some(&Path::from("nodes")), &Path::from("nodes/a"))
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        offset
            .iter()
            .map(|meta| meta.location.to_string())
            .collect::<Vec<_>>(),
        ["nodes/a/b", "nodes/b"]
    );
}

#[tokio::test]
async fn names_written_through_the_store_round_trip_without_reencoding() {
    let fixture = Fixture::new(&[]);
    for name in ["přehled/dokument.html", "100%/x", "a b/x"] {
        let location = Path::from(name);
        fixture
            .store
            .put(&location, PutPayload::from_static(b"body"))
            .await
            .unwrap();
        let prefix = Path::from(name.split('/').next().unwrap());
        assert_eq!(
            fixture.list(Some(&prefix)).await.unwrap(),
            [location.to_string()]
        );
    }
}

#[tokio::test]
async fn raw_prefixes_are_case_sensitive_and_include_the_prefix_key() {
    let fixture = Fixture::new(&[
        "nodes",
        "nodes/a",
        "nodes-x/a",
        "nodes0/a",
        "Nodes/a",
        "a%",
        "a%b/x",
        "a_",
        "a_c/x",
        "abc/x",
        "x'; DROP TABLE objects; --/y",
    ]);
    for (prefix, expected) in [
        ("nodes", vec!["nodes", "nodes-x/a", "nodes/a", "nodes0/a"]),
        ("N", vec!["Nodes/a"]),
        ("a%", vec!["a%", "a%b/x"]),
        ("a_", vec!["a_", "a_c/x"]),
        (
            "x'; DROP TABLE objects; --",
            vec!["x'; DROP TABLE objects; --/y"],
        ),
        ("missing", vec![]),
    ] {
        let page = fixture.page(prefix, PaginatedListOptions::default()).await;
        assert_eq!(objects(&page.result), expected, "prefix {prefix:?}");
    }
    assert_eq!(fixture.list(None).await.unwrap().len(), 11);
}

#[tokio::test]
async fn unicode_ranges_match_string_prefixes_at_scalar_boundaries() {
    let keys = [
        "é/x",
        "é/y",
        "ée/z",
        "ê/x",
        "\u{bf}/x",
        "\u{c0}/x",
        "\u{d7ff}/x",
        "\u{e000}/x",
        "\u{ffff}/x",
        "\u{10000}/x",
        "\u{10ffff}/x",
        "\u{10ffff}\u{10ffff}/x",
        "z\u{10ffff}/x",
        "{/x",
    ];
    let fixture = Fixture::new(&keys);
    let mut ordered = keys.to_vec();
    ordered.sort();
    for prefix in [
        "",
        "é",
        "ée",
        "\u{bf}",
        "\u{d7ff}",
        "\u{ffff}",
        "\u{10000}",
        "\u{10ffff}",
        "\u{10ffff}\u{10ffff}",
        "z\u{10ffff}",
    ] {
        let page = fixture.page(prefix, PaginatedListOptions::default()).await;
        let expected: Vec<_> = ordered
            .iter()
            .filter(|key| key.starts_with(prefix))
            .copied()
            .collect();
        assert_eq!(objects(&page.result), expected, "prefix {prefix:?}");
    }
    assert_eq!(
        fixture
            .list(Some(&Path::parse("é").unwrap()))
            .await
            .unwrap(),
        ["é/x", "é/y"]
    );
}

#[tokio::test]
async fn tokens_and_offsets_resume_after_the_key_within_the_prefix() {
    let fixture = Fixture::new(&["a", "nodes/a", "nodes/b", "nodes/c", "nodes0/a"]);
    for (after, expected) in [
        ("a", vec!["nodes/a", "nodes/b", "nodes/c"]),
        ("nodes/a", vec!["nodes/b", "nodes/c"]),
        ("nodes/az", vec!["nodes/b", "nodes/c"]),
        ("nodes/c", vec![]),
        ("nodes0", vec![]),
    ] {
        for token in [false, true] {
            let options = if token {
                PaginatedListOptions {
                    page_token: Some(after.into()),
                    ..Default::default()
                }
            } else {
                PaginatedListOptions {
                    offset: Some(after.into()),
                    ..Default::default()
                }
            };
            let page = fixture.page("nodes/", options).await;
            assert_eq!(
                objects(&page.result),
                expected,
                "after {after:?}, token {token}"
            );
        }
    }
    let page = fixture
        .page(
            "nodes/",
            PaginatedListOptions {
                page_token: Some("nodes/b".into()),
                offset: Some("nodes/a".into()),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(objects(&page.result), ["nodes/c"]);
}

#[tokio::test]
async fn delimiter_pagination_resumes_after_the_whole_group() {
    let fixture = Fixture::new(&["nodes/a/1", "nodes/a/2", "nodes/b/1", "nodes/c", "nodes/d"]);
    let mut token = None;
    for (expected_prefixes, expected_objects, expected_token) in [
        (vec!["nodes/a"], vec![], Some("nodes/a/2")),
        (vec!["nodes/b"], vec![], Some("nodes/b/1")),
        (vec![], vec!["nodes/c"], Some("nodes/c")),
        (vec![], vec!["nodes/d"], None),
    ] {
        let page = fixture
            .page(
                "nodes/",
                PaginatedListOptions {
                    page_token: token,
                    delimiter: Some("/".into()),
                    max_keys: Some(1),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(prefixes(&page.result), expected_prefixes);
        assert_eq!(objects(&page.result), expected_objects);
        assert_eq!(page.page_token.as_deref(), expected_token);
        token = page.page_token;
    }
}

#[tokio::test]
async fn unrelated_rows_are_not_read_or_decoded_by_any_prefixed_listing() {
    let fixture = Fixture::new(&["nodes/a", "nodes/b/1"]);
    // These rows make an unbounded metadata read fail before Rust can filter it.
    fixture.insert("aaa/bad-etag", "not an integer");
    fixture.insert("zzz/bad-etag", "not an integer");
    fixture.insert("zzz//bad-path", "1");
    assert_eq!(
        fixture.list(Some(&Path::from("nodes"))).await.unwrap(),
        ["nodes/a", "nodes/b/1"]
    );
    let delimited = fixture
        .store
        .list_with_delimiter(Some(&Path::from("nodes")))
        .await
        .unwrap();
    assert_eq!(objects(&delimited), ["nodes/a"]);
    assert_eq!(prefixes(&delimited), ["nodes/b"]);
    let page = fixture
        .page("nodes/", PaginatedListOptions::default())
        .await;
    assert_eq!(objects(&page.result), ["nodes/a", "nodes/b/1"]);
    assert!(fixture.list(None).await.is_err());
}

#[tokio::test]
async fn in_range_errors_surface_but_rows_before_the_cursor_are_not_decoded() {
    let fixture = Fixture::new(&["nodes/b", "nodes/c"]);
    fixture.insert("nodes/a", "not an integer");
    assert!(fixture.list(Some(&Path::from("nodes"))).await.is_err());
    assert!(fixture
        .store
        .list_with_delimiter(Some(&Path::from("nodes")))
        .await
        .is_err());
    assert!(fixture
        .store
        .list_paginated(Some("nodes/"), PaginatedListOptions::default())
        .await
        .is_err());
    let page = fixture
        .page(
            "nodes/",
            PaginatedListOptions {
                page_token: Some("nodes/a".into()),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(objects(&page.result), ["nodes/b", "nodes/c"]);
}

// Count the actual metadata statement's VM work, rather than asserting noisy
// wall-clock timings. Public-API tests above also prove each listing uses bounds.
fn query_work(fixture: &Fixture, range: &KeyRange) -> (usize, i32) {
    let connection = fixture.store.connect().unwrap();
    let (sql, bounds) = range.statement();
    let mut statement = connection.prepare(&sql).unwrap();
    let rows: Vec<String> = statement
        .query_map(rusqlite::params_from_iter(bounds), |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    (rows.len(), statement.get_status(StatementStatus::VmStep))
}

#[test]
fn listing_work_tracks_the_prefix_and_cursor_instead_of_the_whole_table() {
    let fixture = Fixture::new(&["nodes/a", "nodes/b", "nodes/c", "nodes/d", "nodes/e"]);
    let prefix = KeyRange::directory(&Path::from("nodes"));
    let (count, small) = query_work(&fixture, &prefix);
    assert_eq!(count, 5);
    let mut connection = fixture.store.connect().unwrap();
    let transaction = connection.transaction().unwrap();
    for index in 0..4000 {
        transaction
            .execute(
                "INSERT INTO objects VALUES (?1, X'', 1, 0, '[]')",
                [format!("other/{index:05}")],
            )
            .unwrap();
    }
    transaction.commit().unwrap();
    let (count, bounded) = query_work(&fixture, &prefix);
    assert_eq!(count, 5);
    assert!(
        bounded < small * 2,
        "{small} steps before unrelated rows, {bounded} after"
    );
    let (count, full) = query_work(&fixture, &KeyRange::default());
    assert_eq!(count, 4005);
    assert!(full > 4000, "the full-scan control must count VM work");
    assert!(
        bounded * 20 < full,
        "prefix {bounded} steps, full scan {full}"
    );
    let (count, deep) = query_work(&fixture, &KeyRange::prefixed("other/", Some("other/03994")));
    assert_eq!(count, 5);
    assert!(deep * 20 < full, "deep page {deep} steps, full scan {full}");
}
