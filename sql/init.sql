CREATE TYPE task_status AS ENUM (
    'pending',
    'ready',
    'running',
    'finished',
    'failed'
);

CREATE TABLE task_map (
    id BIGSERIAL PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL, -- 任务名。应该按照什么流程执行任务？
    next_step VARCHAR(255), -- 下一个需要执行的步骤。如果为NULL则该任务已完成。
    context JSONB NOT NULL,
    status task_status NOT NULL DEFAULT 'ready',
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 为 status='ready' 创建部分索引，只索引 ready 状态的任务
-- 这样索引更小更高效，专门优化查找 ready 任务的查询
CREATE INDEX IF NOT EXISTS idx_task_map_status_ready ON task_map (status)
WHERE
    status = 'ready';

CREATE OR REPLACE FUNCTION spawn_task(
    task_name VARCHAR(255),
    first_step VARCHAR(255),
    init_context JSONB
) RETURNS BIGINT AS $$
DECLARE
    new_id BIGINT;
BEGIN
    IF first_step IS NULL THEN
        RAISE EXCEPTION 'first_step cannot be null when spawning a new task';
    END IF;

    INSERT INTO task_map (task_name, next_step, context)
    VALUES (task_name, first_step, init_context)
    RETURNING id INTO new_id;

    RETURN new_id;
END;
$$ LANGUAGE plpgsql;

-- CREATE OR REPLACE FUNCTION resume_task(task_id BIGINT)
-- RETURNS void
-- AS $$
-- BEGIN
--     UPDATE task_map
--     SET status = 'running'
--     WHERE id = task_id;
-- END;
-- $$ LANGUAGE plpgsql;

-- 获取 ready 任务并更新状态为 running
-- 如果行被锁定，会跳过而不是等待锁释放
CREATE OR REPLACE FUNCTION fetch_tasks(limit_count INT DEFAULT 200)
RETURNS SETOF task_map AS $$
BEGIN
    RETURN QUERY
    WITH locked AS (
        SELECT tm.id
        FROM task_map tm
        WHERE tm.status = 'ready'
        -- ORDER BY tm.id -- 是否有排序需求待定
        FOR UPDATE SKIP LOCKED
        LIMIT limit_count
    ),
    updated AS (
        UPDATE task_map
        SET status = 'running'
        WHERE id IN (SELECT id FROM locked)
        RETURNING task_map.*
    )
    SELECT * FROM updated;
END;
$$ LANGUAGE plpgsql;

-- 定义任务更新项类型
CREATE TYPE task_update_item AS (
    task_id BIGINT,
    next_step VARCHAR(255),
    context JSONB,
    status task_status
);

-- 批量更新任务
-- 要求：任务当前状态必须为 'running'，更新后状态不能为 'running'
CREATE OR REPLACE FUNCTION update_task(
    updates task_update_item[]
) RETURNS INT AS $$
DECLARE
    updated_count INT;
    invalid_task_id BIGINT;
    invalid_reason TEXT;
BEGIN
    -- 只 unnest 一次，在同一个 CTE 中完成验证和更新
    WITH update_data AS (
        SELECT
            (u).task_id,
            (u).next_step,
            (u).context,
            (u).status AS new_status
        FROM unnest(updates) AS u
    ),
    validation AS (
        SELECT
            ud.task_id,
            tm.status AS current_status,
            ud.new_status,
            CASE
                WHEN tm.status != 'running' THEN '任务当前状态不是 running (当前状态: ' || tm.status || ')'
                WHEN ud.new_status = 'running' THEN '不允许将状态更新为 running'
                ELSE NULL
            END AS error_reason
        FROM update_data ud
        LEFT JOIN task_map tm ON tm.id = ud.task_id
        WHERE tm.status IS NULL
           OR tm.status != 'running'
           OR ud.new_status = 'running'
    )
    SELECT task_id, error_reason INTO invalid_task_id, invalid_reason
    FROM validation
    LIMIT 1;
    
    -- 如果发现无效的更新项，抛出异常
    IF invalid_task_id IS NOT NULL THEN
        RAISE EXCEPTION '更新任务失败 (task_id: %, 原因: %)', invalid_task_id, invalid_reason;
    END IF;
    
    -- 执行批量更新（复用 update_data CTE 的定义逻辑，但 PostgreSQL 需要重新定义）
    WITH update_data AS (
        SELECT
            (u).task_id,
            (u).next_step,
            (u).context,
            (u).status
        FROM unnest(updates) AS u
    )
    UPDATE task_map tm
    SET
        next_step = ud.next_step,
        context = ud.context,
        status = ud.status
    FROM update_data ud
    WHERE tm.id = ud.task_id
      AND tm.status = 'running';  -- 双重检查，确保只更新 running 状态的任务
    
    GET DIAGNOSTICS updated_count = ROW_COUNT;
    RETURN updated_count;
END;
$$ LANGUAGE plpgsql;

-- 添加自动更新update_time
CREATE OR REPLACE FUNCTION update_update_time_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_task_map_update_time
    BEFORE UPDATE ON task_map
    FOR EACH ROW
    EXECUTE FUNCTION update_update_time_column();