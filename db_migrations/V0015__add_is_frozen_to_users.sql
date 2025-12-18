-- Add is_frozen column to users table
ALTER TABLE t_p39739760_garbage_bot_service.users 
ADD COLUMN IF NOT EXISTS is_frozen BOOLEAN DEFAULT false;