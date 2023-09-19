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
use std::time::Instant;

use axum::extract::{Json, Query, RawBody, State};
use common_catalog::consts::DEFAULT_CATALOG_NAME;
use common_error::ext::ErrorExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use session::context::QueryContext;

use crate::http::{ApiState, JsonResponse};

macro_rules! json_err {
    ($e: expr) => {{
        return Json(JsonResponse::with_error(
            format!("Invalid argument: {}", $e),
            common_error::status_code::StatusCode::InvalidArguments,
        ));
    }};

    ($msg: expr, $code: expr) => {{
        return Json(JsonResponse::with_error($msg.to_string(), $code));
    }};
}

macro_rules! unwrap_or_json_err {
    ($result: expr) => {
        match $result {
            Ok(result) => result,
            Err(e) => json_err!(e),
        }
    };
}
