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

//! All query handler traits for various request protocols, like SQL or GRPC.
//! Instance that wishes to support certain request protocol, just implement the corresponding
//! trait, the Server will handle codec for you.
//!
//! Note:
//! Query handlers are not confined to only handle read requests, they are expecting to handle
//! write requests too. So the "query" here not might seem ambiguity. However, "query" has been
//! used as some kind of "convention", it's the "Q" in "SQL". So we might better stick to the
//! word "query".

pub mod grpc;
pub mod sql;

use std::sync::Arc;

use api::prom_store::remote::{ReadRequest, WriteRequest};
use async_trait::async_trait;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use session::context::QueryContextRef;

use crate::error::Result;
use crate::influxdb::InfluxdbRequest;
use crate::opentsdb::codec::DataPoint;
use crate::prom_store::Metrics;

pub type OpentsdbProtocolHandlerRef = Arc<dyn OpentsdbProtocolHandler + Send + Sync>;
pub type InfluxdbLineProtocolHandlerRef = Arc<dyn InfluxdbLineProtocolHandler + Send + Sync>;
pub type PromStoreProtocolHandlerRef = Arc<dyn PromStoreProtocolHandler + Send + Sync>;
pub type OpenTelemetryProtocolHandlerRef = Arc<dyn OpenTelemetryProtocolHandler + Send + Sync>;

#[async_trait]
pub trait InfluxdbLineProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, request: &InfluxdbRequest, ctx: QueryContextRef) -> Result<()>;
}

#[async_trait]
pub trait OpentsdbProtocolHandler {
    /// A successful request will not return a response.
    /// Only on error will the socket return a line of data.
    async fn exec(&self, data_point: &DataPoint, ctx: QueryContextRef) -> Result<()>;
}

pub struct PromStoreResponse {
    pub content_type: String,
    pub content_encoding: String,
    pub body: Vec<u8>,
}

#[async_trait]
pub trait PromStoreProtocolHandler {
    /// Handling prometheus remote write requests
    async fn write(&self, request: WriteRequest, ctx: QueryContextRef) -> Result<()>;
    /// Handling prometheus remote read requests
    async fn read(&self, request: ReadRequest, ctx: QueryContextRef) -> Result<PromStoreResponse>;
    /// Handling push gateway requests
    async fn ingest_metrics(&self, metrics: Metrics) -> Result<()>;
}

#[async_trait]
pub trait OpenTelemetryProtocolHandler {
    /// Handling opentelemetry metrics request
    async fn metrics(
        &self,
        request: ExportMetricsServiceRequest,
        ctx: QueryContextRef,
    ) -> Result<ExportMetricsServiceResponse>;
}
