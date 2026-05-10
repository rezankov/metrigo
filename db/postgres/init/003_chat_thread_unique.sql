CREATE UNIQUE INDEX IF NOT EXISTS idx_chat_threads_seller_id_unique
ON chat_threads(seller_id);