SET SCHEMA 'taskbase';

CREATE TYPE task_status AS ENUM (
    'pending',
    'ready',
    'running',
    'finished',
    'failed'
);

CREATE TABLE task_map (
    id BIGSERIAL PRIMARY KEY,
    namespace VARCHAR(255), -- 命名空间，用于执行器找到自己可执行的任务
    task_name VARCHAR(255) NOT NULL, -- 任务名。应该按照什么流程执行任务？
    context JSONB NOT NULL,
    status task_status NOT NULL,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 为 status='ready' 创建部分索引，只索引 ready 状态的任务
-- 这样索引更小更高效，专门优化查找 ready 任务的查询
CREATE INDEX IF NOT EXISTS idx_task_map_status_ready ON task_map (status)
WHERE
    status = 'ready';

-- 获取 ready 任务并更新状态为 running
-- 如果行被锁定，会跳过而不是等待锁释放
CREATE OR REPLACE FUNCTION fetch_tasks(limit_count INT DEFAULT 200)
    RETURNS SETOF task_map AS
$$
BEGIN
    RETURN QUERY
        WITH ready_task_ids AS (
            SELECT tm.id
            FROM task_map tm
            WHERE tm.status = 'ready'
            FOR UPDATE SKIP LOCKED
            LIMIT limit_count
        ),
        updated AS (
            UPDATE task_map
            SET status = 'running'
            WHERE id IN (SELECT id FROM ready_task_ids)
            RETURNING task_map.*
        )
        SELECT *
        FROM updated;
END;
$$ LANGUAGE plpgsql;

-- 添加自动更新 update_time
CREATE OR REPLACE FUNCTION update_update_time_column()
    RETURNS TRIGGER AS
$$
BEGIN
    NEW.update_time = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_task_map_update_time
    BEFORE UPDATE
    ON task_map
    FOR EACH ROW
    EXECUTE FUNCTION update_update_time_column();