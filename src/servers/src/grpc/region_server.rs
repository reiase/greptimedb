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

use std::sync::Arc;

use api::v1::region::region_server::Region as RegionServer;
use api::v1::region::{region_request, RegionRequest, RegionResponse};
use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_runtime::Runtime;
use common_telemetry::{debug, error, TRACE_ID};
use snafu::{OptionExt, ResultExt};
use tonic::{Request, Response};

use crate::error::{InvalidQuerySnafu, JoinTaskSnafu, Result};
use crate::grpc::TonicResult;

#[async_trait]
pub trait RegionServerHandler: Send + Sync {
    async fn handle(&self, request: region_request::Body) -> Result<RegionResponse>;
}

pub type RegionServerHandlerRef = Arc<dyn RegionServerHandler>;

#[derive(Clone)]
pub struct RegionServerRequestHandler {
    handler: Arc<dyn RegionServerHandler>,
    runtime: Arc<Runtime>,
}

impl RegionServerRequestHandler {
    pub fn new(handler: Arc<dyn RegionServerHandler>, runtime: Arc<Runtime>) -> Self {
        Self { handler, runtime }
    }

    async fn handle(&self, request: RegionRequest) -> Result<RegionResponse> {
        let trace_id = request
            .header
            .context(InvalidQuerySnafu {
                reason: "Expecting non-empty region request header.",
            })?
            .trace_id;
        let query = request.body.context(InvalidQuerySnafu {
            reason: "Expecting non-empty region request body.",
        })?;

        let handler = self.handler.clone();

        // Executes requests in another runtime to
        // 1. prevent the execution from being cancelled unexpected by Tonic runtime;
        //   - Refer to our blog for the rational behind it:
        //     https://www.greptime.com/blogs/2023-01-12-hidden-control-flow.html
        //   - Obtaining a `JoinHandle` to get the panic message (if there's any).
        //     From its docs, `JoinHandle` is cancel safe. The task keeps running even it's handle been dropped.
        // 2. avoid the handler blocks the gRPC runtime incidentally.
        let handle = self.runtime.spawn(TRACE_ID.scope(trace_id, async move {
            handler.handle(query).await.map_err(|e| {
                if e.status_code().should_log_error() {
                    error!(e; "Failed to handle request");
                } else {
                    // Currently, we still print a debug log.
                    debug!("Failed to handle request, err: {}", e);
                }
                e
            })
        }));

        handle.await.context(JoinTaskSnafu)?
    }
}

#[async_trait]
impl RegionServer for RegionServerRequestHandler {
    async fn handle(
        &self,
        request: Request<RegionRequest>,
    ) -> TonicResult<Response<RegionResponse>> {
        let request = request.into_inner();
        let response = self.handle(request).await?;
        Ok(Response::new(response))
    }
}
