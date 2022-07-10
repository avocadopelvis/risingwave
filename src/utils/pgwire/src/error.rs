// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Error as IoError;

use thiserror::Error;
pub type Result<T> = std::result::Result<T, PsqlError>;
use crate::pg_server::BoxedError;
/// Error type used in pgwire crates.
#[derive(Error, Debug)]
pub enum PsqlError {
    #[error("Encode error {0}.")]
    CancelError(String),

    #[error("{0}")]
    IoError(#[from] IoError),

    #[error("Failed to handle ssl request: {0}")]
    SslError(IoError),

    #[error("Invaild sql: {0}")]
    InvaildSQL(IoError),

    #[error("Failed to get response from session: {0}")]
    ReponseError(BoxedError),

    // The difference between IoError and ReadMsgIoError is that ReadMsgIoError needed to report
    // to users but IoError does not.
    #[error("Fail to read message: {0}")]
    ReadMsgError(IoError),

    #[error("Fail to set up pg session: {0}")]
    StartupError(IoError),

    #[error("Failed to authenticate session: {0}.")]
    AuthenticationError(IoError),
}

impl PsqlError {
    /// Construct a Cancel error. Used when Ctrl-c a processing query. Similar to PG.
    pub fn cancel() -> Self {
        PsqlError::CancelError("ERROR:  canceling statement due to user request".to_string())
    }
}
