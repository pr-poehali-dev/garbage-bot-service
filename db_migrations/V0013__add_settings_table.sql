CREATE TABLE IF NOT EXISTS t_p39739760_garbage_bot_service.settings (
  key VARCHAR(50) PRIMARY KEY,
  value TEXT NOT NULL,
  description TEXT,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO t_p39739760_garbage_bot_service.settings (key, value, description) VALUES
('bag_price', '50', 'Цена за один пакет мусора (35л)'),
('subscription_daily_price', '2499', 'Цена подписки "Каждый день" (30 дней)'),
('subscription_alternate_price', '1399', 'Цена подписки "Через день" (30 дней)')
ON CONFLICT (key) DO NOTHING;