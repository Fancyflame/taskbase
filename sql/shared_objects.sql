-- 共享对象表
CREATE TABLE shared_objects (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL
);

-- 对象引用表：记录每个引用，绑定到 task
-- task 被删除时，引用记录会自动删除（ON DELETE CASCADE）
CREATE TABLE shared_object_refs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    object_id BIGINT NOT NULL REFERENCES shared_objects (id) ON DELETE CASCADE,
    attached_task_id BIGINT NOT NULL REFERENCES task_map (id) ON DELETE CASCADE
);

CREATE INDEX idx_shared_object_refs_object_id ON shared_object_refs (object_id);

-- 创建共享对象并返回引用ID
-- 参数：data - 对象数据，attached_task_id - 关联的任务ID
CREATE OR REPLACE FUNCTION create_object(data JSONB, attached_task_id BIGINT)
    RETURNS UUID AS -- 返回的是引用ID
$$
DECLARE
    new_object_id BIGINT;
    new_ref_id UUID;
BEGIN
    -- 创建共享对象
    INSERT INTO shared_objects (data) 
    VALUES (data) 
    RETURNING id INTO new_object_id;
    
    -- 创建引用并返回引用ID
    INSERT INTO shared_object_refs (object_id, attached_task_id) 
    VALUES (new_object_id, attached_task_id) 
    RETURNING id INTO new_ref_id;
    
    RETURN new_ref_id;
END;
$$ LANGUAGE plpgsql;

-- 通过引用ID读取共享对象数据
CREATE OR REPLACE FUNCTION read_object(ref_id UUID)
    RETURNS JSONB AS
$$
DECLARE
    result_data JSONB;
BEGIN
    SELECT so.data INTO result_data
    FROM shared_object_refs sor
    JOIN shared_objects so ON sor.object_id = so.id
    WHERE sor.id = ref_id;

    RETURN result_data;
END;
$$ LANGUAGE plpgsql;

-- 克隆引用：为同一个对象创建新的引用
-- 参数：ref_id - 源引用ID，attach_task_id - 新引用关联的任务ID
CREATE OR REPLACE FUNCTION clone_object(ref_id UUID, attach_task_id BIGINT)
    RETURNS UUID AS
$$
DECLARE
    source_object_id BIGINT;
    new_ref_id UUID;
BEGIN
    -- 获取源引用对应的对象ID
    SELECT object_id INTO source_object_id
    FROM shared_object_refs
    WHERE id = ref_id;

    -- 如果源引用不存在，返回 NULL
    IF source_object_id IS NULL THEN
        RETURN NULL;
    END IF;

    -- 为同一个对象创建新引用并返回引用ID
    INSERT INTO shared_object_refs (object_id, attached_task_id)
    VALUES (source_object_id, attach_task_id)
    RETURNING id INTO new_ref_id;

    RETURN new_ref_id;
END;
$$ LANGUAGE plpgsql;

-- 当引用被删除后，检查对象是否还有其他引用
-- 如果没有引用，自动删除对象
CREATE OR REPLACE FUNCTION cleanup_unreferenced_objects()
    RETURNS TRIGGER AS
$$
DECLARE
    remaining_refs INT;
BEGIN
    -- 检查被删除引用对应的对象是否还有其他引用
    SELECT COUNT(*)
    INTO remaining_refs
    FROM shared_object_refs
    WHERE object_id = OLD.object_id;

    -- 如果没有其他引用了，删除对象
    IF remaining_refs = 0 THEN
        DELETE FROM shared_objects WHERE id = OLD.object_id;
    END IF;

    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

-- 在引用表上创建触发器：删除引用后检查并清理对象
CREATE TRIGGER trg_cleanup_unreferenced_objects
    AFTER DELETE
    ON shared_object_refs
    FOR EACH ROW
    EXECUTE FUNCTION cleanup_unreferenced_objects();