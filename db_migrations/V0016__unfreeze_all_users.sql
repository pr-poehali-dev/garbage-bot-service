-- Unfreeze all frozen users
UPDATE t_p39739760_garbage_bot_service.users 
SET is_frozen = false 
WHERE is_frozen = true;