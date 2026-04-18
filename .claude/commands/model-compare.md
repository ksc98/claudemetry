Compare model performance using the claudemetry DO SQLite database via `burnage shell -c "SQL"`.

The user may provide specific models or a time range as $ARGUMENTS. If no arguments, default to comparing the two most recent distinct models in the last 7 days.

## Database schema

Relevant **transactions** columns for comparison: ts (epoch ms), session_id, status, elapsed_ms, model, input_tokens, output_tokens, cache_read, cache_creation, stop_reason, thinking_budget, thinking_blocks, max_tokens.

For the full schema run `burnage shell -c "PRAGMA table_info(transactions)"`.

**session_summaries** table: session_id, model, turns, first_ts, last_ts, input_tokens, output_tokens, cache_read, cache_creation

## Queries to run

Run these queries via `burnage shell -c "SQL"`. Determine the two models to compare first (from args or by querying recent distinct models). Filter to `status = 200` for all metric queries. Use the models in all WHERE clauses below.

1. **Overview**: turns, total input/output tokens, total cache read/creation, avg output tokens, avg elapsed_ms, cache hit rate per model
2. **Stop reason distribution**: model, stop_reason, count
3. **Thinking usage**: avg thinking_blocks, turns with thinking, pct thinking, avg thinking_budget per model
4. **Latency breakdown**: avg/min/max elapsed_ms, avg latency for tool_use vs end_turn stops per model
5. **Throughput**: avg tokens/sec overall, tokens/sec for tool_use vs end_turn per model
6. **Context size**: avg input_tokens, avg cache_read, avg cache_creation, avg total context per model

## Output format

Present results as a single markdown comparison table with the two models as columns plus a delta column. Group related metrics with row separators. Call out notable differences (>15% delta) with brief commentary after the table.

If sample sizes are very different, note the caveat upfront.
