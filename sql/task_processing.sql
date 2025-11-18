-- 创建外部任务的函数
-- 参数：p_namespace - 目标执行器
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

-- 推送任务：插入或更新任务
-- 参数：p_id - 任务ID，如果为 NULL 则插入新任务
--      p_namespace - 命名空间
--      p_task_name - 任务名称
--      p_context - 任务上下文数据（BYTEA）
--      p_status - 任务状态
-- 逻辑：
--   - 如果 p_id 为 NULL，插入新任务
--   - 如果 p_id 不为 NULL，检查任务是否存在且 namespace 匹配
--   - 如果检查失败（任务不存在或 namespace 不匹配），忽略该操作
CREATE OR REPLACE FUNCTION push_task(
    p_id BIGINT,
    p_namespace VARCHAR(255),
    p_task_name VARCHAR(255),
    p_context BYTEA,
    p_status task_status
) RETURNS VOID AS
$$
BEGIN
    -- 如果 id 为 NULL，插入新任务
    IF p_id IS NULL THEN
        INSERT INTO task_map (namespace, task_name, context, status)
        VALUES (p_namespace, p_task_name, p_context, p_status);
        RETURN;
    END IF;

    UPDATE task_map
    SET task_name = p_task_name,
        context = p_context,
        status = p_status
    WHERE id = p_id
      AND namespace = p_namespace;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION push_tasks(
    p_ids BIGINT[],
    p_namespaces VARCHAR(255)[],
    p_task_names VARCHAR(255)[],
    p_contexts BYTEA[],
    p_statuses task_status[]
) RETURNS VOID AS
$$
DECLARE
    i INT;
    arr_len INT;
BEGIN
    -- 循环调用 push_task 处理每个任务
    FOR i IN 1..arr_len LOOP
        PERFORM push_task(
            p_ids[i],
            p_namespaces[i],
            p_task_names[i],
            p_contexts[i],
            p_statuses[i]
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 获取 ready 任务并更新状态为 processing
-- 如果行被锁定，会等待锁释放
CREATE OR REPLACE FUNCTION fetch_tasks(
    p_namespaces VARCHAR(255)[], -- 命名空间列表，命中任意一个即可
    p_limit_count INT DEFAULT 200
)
RETURNS TABLE (
    id BIGINT,
    namespace VARCHAR(255),
    task_name VARCHAR(255),
    context BYTEA
) AS $$
BEGIN
    RETURN QUERY
        WITH ready_task_ids AS (
            SELECT tm.id
            FROM task_map tm
            WHERE tm.namespace = ANY(p_namespaces)
            AND tm.status = 'ready'
            FOR UPDATE -- SKIP LOCKED
            LIMIT p_limit_count
        ),
        updated AS (
            UPDATE task_map
            SET status = 'processing'
            WHERE task_map.id IN (
                SELECT ready_task_ids.id FROM ready_task_ids
            )
            RETURNING task_map.*
        )
        SELECT
            updated.id,
            updated.namespace,
            updated.task_name,
            updated.context
        FROM updated;
END;
$$ LANGUAGE plpgsql;