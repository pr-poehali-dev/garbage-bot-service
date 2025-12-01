-- Добавляем таблицу для архива чатов
CREATE TABLE IF NOT EXISTS order_chat_archive (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL,
    sender_id BIGINT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем индекс для быстрого поиска по заказу
CREATE INDEX IF NOT EXISTS idx_chat_archive_order_id ON order_chat_archive(order_id);

-- Добавляем флаг архивации в order_chat
ALTER TABLE order_chat ADD COLUMN IF NOT EXISTS is_archived BOOLEAN DEFAULT FALSE;

-- Создаем индекс для фильтрации архивных чатов
CREATE INDEX IF NOT EXISTS idx_order_chat_archived ON order_chat(is_archived) WHERE is_archived = FALSE;