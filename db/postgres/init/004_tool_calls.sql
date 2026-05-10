CREATE TABLE IF NOT EXISTS ai_tool_calls (
    id BIGSERIAL PRIMARY KEY,
    seller_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    args_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB,
    ok BOOLEAN NOT NULL DEFAULT FALSE,
    error TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ai_tool_calls_seller_id_created_at
ON ai_tool_calls(seller_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_tool_calls_tool_name
ON ai_tool_calls(tool_name);