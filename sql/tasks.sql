CREATE TABLE namespaces (
    id VARCHAR(255) PRIMARY KEY
);

CREATE TYPE task_status AS ENUM (
    'blocking',
    'ready',
    'processing',
    'finished',
    'failed'
);

CREATE TABLE task_map (
    id BIGSERIAL PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL REFERENCES namespaces(id), -- 命名空间，用于执行器找到自己可执行的任务
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

-- 可由别的执行器发起执行的任务列表
CREATE TABLE export_task_interfaces (
    namespace VARCHAR(255) NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    task_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (namespace, task_name)
);

-- 创建外部任务的函数
-- 参数：p_namespace - 目标执行器的命名空间
--      p_task_name - 任务名称
--      p_context - 任务上下文数据（JSONB）
-- 返回：新创建的任务ID，如果任务不在导出列表中则返回 NULL
CREATE OR REPLACE FUNCTION spawn_extern_task(
    p_namespace VARCHAR(255),
    p_task_name VARCHAR(255),
    p_context JSONB
) RETURNS BIGINT AS
$$
DECLARE
    v_task_id BIGINT;
BEGIN
    -- 检查任务是否在导出列表中
    IF NOT EXISTS (
        SELECT 1 
        FROM export_task_interfaces 
        WHERE namespace = p_namespace 
          AND task_name = p_task_name
    ) THEN
        -- 任务不在导出列表中，返回 NULL
        RETURN NULL;
    END IF;

    -- 创建新任务，状态为 'ready'
    INSERT INTO task_map (namespace, task_name, context, status)
    VALUES (p_namespace, p_task_name, p_context, 'ready')
    RETURNING id INTO v_task_id;

    RETURN v_task_id;
END;
$$ LANGUAGE plpgsql;

-- 获取 ready 任务并更新状态为 processing
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
            SET status = 'processing'
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