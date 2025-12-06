ALTER TABLE t_p39739760_garbage_bot_service.orders
ADD COLUMN payment_id VARCHAR(255),
ADD COLUMN payment_status VARCHAR(50) DEFAULT 'pending',
ADD COLUMN payment_url TEXT,
ADD COLUMN paid_at TIMESTAMP;