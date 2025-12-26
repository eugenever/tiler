use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, Display, EnumString, IntoStaticStr};
use uuid::Uuid;

#[async_trait::async_trait]
pub trait Queue: Send + Sync + Debug {
    async fn push(
        &self,
        job: JobDetail,
        scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), crate::db::error::Error>;
    // pull fetches at most `number_of_jobs` from the queue.
    async fn pull(&self, number_of_jobs: Option<i32>) -> Result<Vec<Job>, crate::db::error::Error>;
    async fn delete_job(&self, job_id: Uuid) -> Result<(), crate::db::error::Error>;
    async fn fail_job(&self, job_id: Uuid) -> Result<(), crate::db::error::Error>;
    async fn clear(&self) -> Result<(), crate::db::error::Error>;
}

#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Display, EnumString, IntoStaticStr, AsRefStr,
)]
pub enum JobType {
    #[serde(rename = "pyramid")]
    #[strum(serialize = "pyramid")]
    Pyramid { datasource_id: String },

    #[serde(rename = "calculation")]
    #[strum(serialize = "calculation")]
    Calculation,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub job_id: Uuid,
    pub detail: JobDetail,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobDetail {
    pub jt: JobType,
    pub name: String,
    pub scheduled_for: Option<chrono::DateTime<chrono::Utc>>,
    pub data: sqlx::types::Json<serde_json::Value>,
}
