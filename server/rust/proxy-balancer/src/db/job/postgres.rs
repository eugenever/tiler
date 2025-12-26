use chrono;
use sqlx::{postgres::PgRow, types::Json, Row};
use ulid::Ulid;
use uuid::Uuid;

use crate::db::{
    job::queue::{Job, JobDetail, Queue},
    DB,
};
use crate::defaults::JOB_CONCURRENCY;

#[derive(Debug, Clone)]
pub struct PostgresQueue {
    db: DB,
    max_attempts: u32,
}

const MAX_FAILED_ATTEMPTS: i32 = 3; // low, as most jobs also use retries internally

#[derive(Debug, Clone)]
struct PostgresJob {
    job_id: Uuid,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,

    scheduled_for: chrono::DateTime<chrono::Utc>,
    failed_attempts: u32,
    status: PostgresJobStatus,
    job_detail: Json<JobDetail>,
}

impl<'r> sqlx::FromRow<'r, PgRow> for PostgresJob {
    fn from_row(row: &'r PgRow) -> Result<PostgresJob, sqlx::Error> {
        let uuid: String = row.try_get("job_id")?;
        let job_id = Uuid::parse_str(&uuid).map_err(|err| sqlx::Error::Decode(Box::new(err)))?;
        let created_at: chrono::DateTime<chrono::Utc> = row.try_get("created_at")?;
        let updated_at: chrono::DateTime<chrono::Utc> = row.try_get("updated_at")?;
        let scheduled_for: chrono::DateTime<chrono::Utc> = row.try_get("scheduled_for")?;
        let failed_attempts: i32 = row.try_get("failed_attempts")?;
        let status: PostgresJobStatus = row.try_get("status")?;
        let job_detail: Json<JobDetail> = row.try_get("job_detail")?;

        Ok(PostgresJob {
            job_id,
            created_at,
            updated_at,
            scheduled_for,
            failed_attempts: failed_attempts as u32,
            status,
            job_detail,
        })
    }
}

// We use a INT postgres representation for performance reasons
#[derive(Debug, Clone, sqlx::Type, PartialEq)]
#[repr(i32)]
enum PostgresJobStatus {
    Queued,
    Running,
    Failed,
}

impl From<PostgresJob> for Job {
    fn from(pj: PostgresJob) -> Self {
        Job {
            job_id: pj.job_id,
            detail: pj.job_detail.0,
        }
    }
}

impl PostgresQueue {
    pub fn new(db: DB) -> PostgresQueue {
        let queue = PostgresQueue {
            db,
            max_attempts: 5,
        };
        queue
    }
}

#[async_trait::async_trait]
impl Queue for PostgresQueue {
    async fn push(
        &self,
        job_detail: JobDetail,
        date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), crate::db::error::Error> {
        let scheduled_for = date.unwrap_or(chrono::Utc::now());
        let failed_attempts: i32 = 0;
        let job_detail = Json(job_detail);
        let status = PostgresJobStatus::Queued;
        let now = chrono::Utc::now();
        let job_id: Uuid = Ulid::new().into();
        let query = "INSERT INTO queue
          (job_id, created_at, updated_at, scheduled_for, failed_attempts, status, job_detail)
          VALUES ($1, $2, $3, $4, $5, $6, $7)";

        let mut transaction = self.db.begin().await?;
        match sqlx::query(query)
            .bind(job_id.to_string())
            .bind(now)
            .bind(now)
            .bind(scheduled_for)
            .bind(failed_attempts)
            .bind(status)
            .bind(job_detail)
            .execute(&mut *transaction)
            .await
        {
            Err(err) => {
                transaction.rollback().await?;
                return Err(err.into());
            }
            Ok(jobs) => {
                transaction.commit().await?;
                jobs
            }
        };

        Ok(())
    }

    async fn delete_job(&self, job_id: Uuid) -> Result<(), crate::db::error::Error> {
        let query = "DELETE FROM queue WHERE job_id = $1";

        let mut transaction = self.db.begin().await?;
        match sqlx::query(query)
            .bind(job_id.to_string())
            .execute(&mut *transaction)
            .await
        {
            Err(err) => {
                transaction.rollback().await?;
                return Err(err.into());
            }
            Ok(jobs) => {
                transaction.commit().await?;
                jobs
            }
        };

        Ok(())
    }

    async fn fail_job(&self, job_id: Uuid) -> Result<(), crate::db::error::Error> {
        let now = chrono::Utc::now();
        let query = "UPDATE queue
          SET status = $1, updated_at = $2, failed_attempts = failed_attempts + 1
          WHERE job_id = $3";

        let mut transaction = self.db.begin().await?;
        match sqlx::query(query)
            .bind(PostgresJobStatus::Queued)
            .bind(now)
            .bind(job_id.to_string())
            .execute(&mut *transaction)
            .await
        {
            Err(err) => {
                transaction.rollback().await?;
                return Err(err.into());
            }
            Ok(jobs) => {
                transaction.commit().await?;
                jobs
            }
        };

        Ok(())
    }

    async fn pull(&self, number_of_jobs: Option<i32>) -> Result<Vec<Job>, crate::db::error::Error> {
        let mut nj = JOB_CONCURRENCY as i32;
        if let Some(v) = number_of_jobs {
            nj = v
        }
        let now = chrono::Utc::now();
        let query = "UPDATE queue
          SET status = $1, updated_at = $2
          WHERE job_id IN (
              SELECT job_id
              FROM queue
              WHERE status = $3 AND scheduled_for <= $4 AND failed_attempts < $5
              ORDER BY scheduled_for
              FOR UPDATE SKIP LOCKED
              LIMIT $6
          )
          RETURNING job_id, created_at, updated_at, scheduled_for, failed_attempts, status, job_detail";

        let mut transaction = self.db.begin().await?;
        let jobs: Vec<PostgresJob> = match sqlx::query_as::<_, PostgresJob>(query)
            .bind(PostgresJobStatus::Running)
            .bind(now)
            .bind(PostgresJobStatus::Queued)
            .bind(now)
            .bind(MAX_FAILED_ATTEMPTS)
            .bind(nj)
            .fetch_all(&mut *transaction)
            .await
        {
            Err(err) => {
                transaction.rollback().await?;
                return Err(err.into());
            }
            Ok(jobs) => {
                transaction.commit().await?;
                jobs
            }
        };

        Ok(jobs.into_iter().map(Into::into).collect())
    }

    async fn clear(&self) -> Result<(), crate::db::error::Error> {
        let query = "DELETE FROM queue";

        let mut transaction = self.db.begin().await?;
        match sqlx::query(query).execute(&mut *transaction).await {
            Err(err) => {
                transaction.rollback().await?;
                return Err(err.into());
            }
            Ok(jobs) => {
                transaction.commit().await?;
                jobs
            }
        };

        Ok(())
    }
}
