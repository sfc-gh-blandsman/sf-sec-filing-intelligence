# Lessons Learned — SEC Filing Intelligence Pipeline

Hard-won architectural insights from building this pipeline. Reference these before making changes.

---

## 1. EXECUTE AS CALLER is mandatory for SPs in Task context

Any stored procedure that runs inside a Snowflake Task DAG (or needs session state like `CURRENT_DATABASE()`) must use `EXECUTE AS CALLER`. Without it, the SP runs as the owner role and cannot see the caller's database/schema context.

## 2. AI output columns must be TEXT or VARCHAR(500+)

Three separate incidents of column overflow from Cortex AI functions (AI_EXTRACT, AI_COMPLETE). AI-generated text is unpredictable in length. Always use `TEXT` (16 MB max) or at minimum `VARCHAR(16777216)` for columns that store AI output. Never use `VARCHAR(100)` or similar short types.

## 3. Snowflake PKs are NOT enforced

Primary keys in Snowflake are metadata-only constraints — they are not enforced at INSERT time. Parallel tasks (e.g., 12 monthly feed tasks, 3 chunking tasks) will insert duplicate rows if the same ACCESSION_NO appears at quarter boundaries. Always deduplicate explicitly with `WHERE NOT EXISTS` or a post-insert `T_FEED_VALIDATE` cleanup task.

## 4. Cortex Search: create once, refresh incrementally

Never `DROP` and recreate a Cortex Search service in production. Dropping discards all computed embeddings (expensive to regenerate). Instead, use `ALTER CORTEX SEARCH SERVICE ... REFRESH` for incremental updates. Only recreate when immutable properties change (embedding model, refresh mode, text/vector columns).

## 5. Task DAG halt requires suspending ALL nodes, not just the root

Suspending only the root task does NOT stop a running DAG. `EXECUTE TASK` works on suspended roots, and auto-retry still fires. The finalizer can re-trigger the root. To truly halt: suspend ALL tasks (root + children + finalizer). Suspended children are skipped during execution.

## 6. TASK_AUTO_RETRY_ATTEMPTS retries the entire DAG from root

This is a graph-level retry, not per-task. When any child task fails, the entire DAG restarts from the root task, up to N times. Design idempotent tasks — they must handle being re-run from scratch.

## 7. Large HTTP downloads in Python SPs need special handling

For SEC EDGAR archives (>800 MB):
- Use `stream=True` with `iter_content(chunk_size=10MB)` — never `.content` (loads entire response into memory)
- Retry with backoff (3 attempts, 30s/60s sleep between)
- Use Snowpark-optimized warehouse (more memory per node)
- Batch-flush parsed results to Snowflake every N rows to bound memory
- Set `timeout=600` (10 minutes) for large downloads

## 8. YAML scalars with special characters need block scalar format

In Cortex Agent specs, description strings with parentheses, commas, colons, or quotes cause YAML parse errors when using inline scalars. Use block scalar `|` format:

```yaml
description: |
  This tool searches SEC filings (10-K, 10-Q, 8-K) for relevant information.
```

Not: `description: "This tool searches SEC filings (10-K, 10-Q, 8-K)"` (fails on parentheses).

## 9. _PIPELINE_CONFIG + _CFG() pattern for Task DAG configuration

Session variables (`$config_*`) are NOT accessible inside Task bodies. The correct pattern:
1. Store config in `_PIPELINE_CONFIG` table (key-value pairs)
2. Create a helper function `_CFG(key)` that reads from this table
3. Tasks call `_CFG('warehouse')` instead of `$config_warehouse`
4. Persist config by running `00_config.sql` after Phase 1 (INSERT OVERWRITE)

## 10. Eval logical_consistency metric fails in Task context

The `logical_consistency` eval metric fails for agents with large traces when run inside a Task DAG (Snowflake bug SNOW-3490805). Workaround: run evaluations from an interactive Snowsight session or Cortex Code, not from a scheduled task.
