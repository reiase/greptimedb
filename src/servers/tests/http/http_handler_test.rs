// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use axum::body::{Body, Bytes};
use axum::extract::{Json, Query, RawBody, State};
use axum::Form;
use common_telemetry::metric;
use http_body::combinators::UnsyncBoxBody;
use hyper::Response;
use metrics::counter;
use servers::http::{
    handler as http_handler, ApiState, GreptimeOptionsConfigState,
    JsonOutput,
};
use servers::metrics_handler::MetricsHandler;
use table::test_util::MemTable;

use crate::{
    create_testing_sql_query_handler,
    ServerSqlQueryHandlerRef,
};

#[tokio::test]
async fn test_sql_not_provided() {
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());
    let Json(json) = http_handler::sql(
        State(ApiState {
            sql_handler,
        }),
        Query(http_handler::SqlQuery::default()),
        axum::Extension(auth::userinfo_by_name(None)),
        Form(http_handler::SqlQuery::default()),
    )
    .await;
    assert!(!json.success());
    assert_eq!(
        Some(&"sql parameter is required.".to_string()),
        json.error()
    );
    assert!(json.output().is_none());
}

#[tokio::test]
async fn test_sql_output_rows() {
    common_telemetry::init_default_ut_logging();

    let query = create_query();
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let Json(json) = http_handler::sql(
        State(ApiState {
            sql_handler,
        }),
        query,
        axum::Extension(auth::userinfo_by_name(None)),
        Form(http_handler::SqlQuery::default()),
    )
    .await;
    assert!(json.success(), "{json:?}");
    assert!(json.error().is_none());
    match &json.output().expect("assertion failed")[0] {
        JsonOutput::Records(records) => {
            assert_eq!(1, records.num_rows());
            let json = serde_json::to_string_pretty(&records).unwrap();
            assert_eq!(
                json,
                r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "SUM(numbers.uint32s)",
        "data_type": "UInt64"
      }
    ]
  },
  "rows": [
    [
      4950
    ]
  ]
}"#
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_sql_form() {
    common_telemetry::init_default_ut_logging();

    let form = create_form();
    let sql_handler = create_testing_sql_query_handler(MemTable::default_numbers_table());

    let Json(json) = http_handler::sql(
        State(ApiState {
            sql_handler,
        }),
        Query(http_handler::SqlQuery::default()),
        axum::Extension(auth::userinfo_by_name(None)),
        form,
    )
    .await;
    assert!(json.success(), "{json:?}");
    assert!(json.error().is_none());
    match &json.output().expect("assertion failed")[0] {
        JsonOutput::Records(records) => {
            assert_eq!(1, records.num_rows());
            let json = serde_json::to_string_pretty(&records).unwrap();
            assert_eq!(
                json,
                r#"{
  "schema": {
    "column_schemas": [
      {
        "name": "SUM(numbers.uint32s)",
        "data_type": "UInt64"
      }
    ]
  },
  "rows": [
    [
      4950
    ]
  ]
}"#
            );
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn test_metrics() {
    metric::init_default_metrics_recorder();

    counter!("test_metrics", 1);
    let stats = MetricsHandler;
    let text = http_handler::metrics(axum::extract::State(stats), Query(HashMap::default())).await;
    assert!(text.contains("test_metrics counter"));
}

fn create_query() -> Query<http_handler::SqlQuery> {
    Query(http_handler::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        db: None,
    })
}

fn create_form() -> Form<http_handler::SqlQuery> {
    Form(http_handler::SqlQuery {
        sql: Some("select sum(uint32s) from numbers limit 20".to_string()),
        db: None,
    })
}

/// Currently the payload of response should be simply an empty json "{}";
#[tokio::test]
async fn test_health() {
    let expected_json = http_handler::HealthResponse {};
    let expected_json_str = "{}".to_string();

    let query = http_handler::HealthQuery {};
    let Json(json) = http_handler::health(Query(query)).await;
    assert_eq!(json, expected_json);
    assert_eq!(
        serde_json::ser::to_string(&json).unwrap(),
        expected_json_str
    );
}

#[tokio::test]
async fn test_status() {
    let hostname = hostname::get()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let expected_json = http_handler::StatusResponse {
        source_time: env!("SOURCE_TIMESTAMP"),
        commit: env!("GIT_COMMIT"),
        branch: env!("GIT_BRANCH"),
        rustc_version: env!("RUSTC_VERSION"),
        hostname,
        version: env!("CARGO_PKG_VERSION"),
    };

    let Json(json) = http_handler::status().await;
    assert_eq!(json, expected_json);
}

#[tokio::test]
async fn test_config() {
    let toml_str = r#"
            mode = "distributed"

            [http_options]
            addr = "127.0.0.1:4000"
            timeout = "30s"
            body_limit = "2GB"

            [logging]
            level = "debug"
            dir = "/tmp/greptimedb/test/logs"
        "#;
    let rs = http_handler::config(State(GreptimeOptionsConfigState {
        greptime_config_options: toml_str.to_string(),
    }))
    .await;
    assert_eq!(200_u16, rs.status().as_u16());
    assert_eq!(get_body(rs).await, toml_str);
}

async fn get_body(response: Response<UnsyncBoxBody<Bytes, axum::Error>>) -> Bytes {
    hyper::body::to_bytes(response.into_body()).await.unwrap()
}
