CREATE TABLE namespaces (
    id VARCHAR(255) PRIMARY KEY
);

CREATE TYPE task_status AS ENUM (
    'blocking',
    'ready',
    'processing',
    'terminated'
);

CREATE TABLE task_map (
    id BIGSERIAL PRIMARY KEY,
    namespace VARCHAR(255) NOT NULL REFERENCES namespaces(id), --用于执行器找到自己可执行的任务
    task_name VARCHAR(255) NOT NULL,
    context BYTEA NOT NULL,
    status task_status NOT NULL,
    create_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    update_time TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 为 status='ready' 和 namespace 创建部分索引
-- 优化查询：WHERE namespace = ? AND status = 'ready'
CREATE INDEX idx_task_map_status_ready ON task_map (namespace, status)
WHERE
    status = 'ready';

CREATE TABLE blocking_tasks (
    listener BIGINT NOT NULL REFERENCES task_map(id) ON DELETE CASCADE,
    listening BIGINT NOT NULL REFERENCES task_map(id) ON DELETE CASCADE,
    UNIQUE (listener, listening)
);

CREATE INDEX idx_blocking_tasks_listener ON blocking_tasks (listener);
CREATE INDEX idx_blocking_tasks_listening ON blocking_tasks (listening);

-- 可由别的执行器发起执行的任务列表
CREATE TABLE export_task_interfaces (
    namespace VARCHAR(255) NOT NULL REFERENCES namespaces(id) ON DELETE CASCADE,
    task_name VARCHAR(255) NOT NULL,
    PRIMARY KEY (namespace, task_name)
);

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

-- 当 ready 任务数量增加时发出通知
CREATE OR REPLACE FUNCTION notify_task_ready()
    RETURNS TRIGGER AS
$$
DECLARE
    channel TEXT;
BEGIN
    channel := format('task_ready/%s', NEW.namespace);
    PERFORM pg_notify(channel);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_task_ready
    AFTER INSERT OR UPDATE OF status
    ON task_map
    FOR EACH ROW
    WHEN (NEW.status = 'ready')
    EXECUTE FUNCTION notify_task_ready();

-- 当任务状态转别为terminated时激活所有监听它的任务
CREATE OR REPLACE FUNCTION on_task_terminated()
    RETURNS TRIGGER AS
$$
DECLARE
    channel TEXT;
BEGIN
    UPDATE task_map
    SET status = 'ready'
    WHERE id IN (
        SELECT listener
        FROM blocking_tasks
        WHERE listening = NEW.id
    )
      AND status = 'blocking';
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_task_terminated
    AFTER UPDATE OF status
    ON task_map
    FOR EACH ROW
    WHEN (NEW.status = 'terminated')
    EXECUTE FUNCTION on_task_terminated();

-- 当任务从 blocking 转为其他状态时删除它的所有监听器
CREATE OR REPLACE FUNCTION cleanup_blocking_tasks_on_unblock()
    RETURNS TRIGGER AS
$$
BEGIN
    DELETE FROM blocking_tasks WHERE listener = NEW.id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cleanup_blocking_tasks_on_unblock
    AFTER UPDATE OF status
    ON task_map
    FOR EACH ROW
    WHEN (OLD.status = 'blocking' AND NEW.status <> 'blocking')
    EXECUTE FUNCTION cleanup_blocking_tasks_on_unblock();