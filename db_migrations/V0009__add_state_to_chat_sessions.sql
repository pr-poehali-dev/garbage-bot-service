-- Добавляем поля для управления состоянием заказа
ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS state VARCHAR(50);
ALTER TABLE chat_sessions ADD COLUMN IF NOT EXISTS order_data JSONB;