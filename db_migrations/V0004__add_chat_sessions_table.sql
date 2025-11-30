-- Add chat_sessions table to track which order chat user is currently in
CREATE TABLE IF NOT EXISTS chat_sessions (
    telegram_id BIGINT PRIMARY KEY,
    order_id INTEGER NOT NULL,
    last_message_id INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (telegram_id) REFERENCES users(telegram_id),
    FOREIGN KEY (order_id) REFERENCES orders(id)
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_order ON chat_sessions(order_id);
CREATE INDEX IF NOT EXISTS idx_chat_sessions_updated ON chat_sessions(updated_at);