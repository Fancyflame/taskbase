use anyhow::Result;

use crate::db_backend::service::DbService;

pub struct PushTask {
    /// if id is none, spawn a new task
    pub id: Option<i64>,
    pub namespace: String,
    pub task_name: String,
    pub context: Vec<u8>,
    pub status: TaskStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Blocking,
    Ready,
    Processing,
    Terminated,
}

impl TaskStatus {
    pub const fn to_str(&self) -> &'static str {
        match self {
            Self::Blocking => "blocking",
            Self::Ready => "ready",
            Self::Processing => "processing",
            Self::Terminated => "terminated",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "blocking" => Self::Blocking,
            "ready" => Self::Ready,
            "processing" => Self::Processing,
            "terminated" => Self::Terminated,
            _ => return None,
        })
    }
}

impl DbService {
    /// 批量更新任务信息，只更新有权限的 namespace 下的任务
    pub async fn push_blocking_tasks(&self, tasks: &[PushTask]) -> Result<()> {
        // 先过滤出有权限的 namespace 的任务
        let tasks: Vec<&PushTask> = tasks
            .iter()
            .filter(|task| self.namespaces.contains(&task.namespace))
            .collect();

        if tasks.is_empty() {
            return Ok(());
        }

        // 准备批量数组参数
        let ids = unzip_task_list(&tasks, |t| t.id);
        let namespaces = unzip_task_list(&tasks, |t| t.namespace.as_str());
        let task_names = unzip_task_list(&tasks, |t| t.task_name.as_str());
        let contexts = unzip_task_list(&tasks, |t| t.context.as_slice());
        let statuses = unzip_task_list(&tasks, |t| TaskStatus::to_str(&t.status));

        // 一次性批量调用 push_tasks 函数
        sqlx::query("SELECT push_tasks($1::bigint[], $2::varchar[], $3::varchar[], $4::bytea[], $5::task_status[])")
            .bind(&ids)
            .bind(&namespaces)
            .bind(&task_names)
            .bind(&contexts)
            .bind(&statuses)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

fn unzip_task_list<'a, F, R>(tasks: &[&'a PushTask], f: F) -> Vec<R>
where
    F: Fn(&'a PushTask) -> R,
{
    tasks.iter().copied().map(f).collect()
}
