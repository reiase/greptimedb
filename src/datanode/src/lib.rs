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

#![feature(assert_matches)]
#![feature(trait_upcasting)]

pub mod datanode;
pub mod error;
pub mod heartbeat;
pub mod instance;
pub mod metrics;
#[cfg(any(test, feature = "testing"))]
mod mock;
pub mod region_server;
pub mod server;
pub mod sql;
mod store;
#[cfg(test)]
mod tests;
