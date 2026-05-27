-- =============================================================================
-- 01: Ticker Enrichment
-- =============================================================================
-- Fills NULL ticker symbols in FILING_INDEX via SEC company facts API.
-- Processes batch_size CIKs per call, cursor-based for resumability.
-- Rate-limited to ~9 req/sec (0.11s sleep) per SEC fair use policy.
--
-- Run BEFORE chunking/signal extraction so tickers propagate downstream.
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- SP: ENRICH_TICKERS
-- =============================================================================
-- Processes batch_size CIKs starting after P_AFTER_CIK (cursor).
-- Returns "DONE" when no more CIKs above cursor have NULL ticker.
--
-- Usage:
--   CALL ENRICH_TICKERS(500, '0000000000');
--   -- Repeat until it returns "DONE"
-- =============================================================================

CREATE OR REPLACE PROCEDURE ENRICH_TICKERS(
    P_BATCH_SIZE INT DEFAULT 500,
    P_AFTER_CIK VARCHAR DEFAULT '0000000000',
    USER_AGENT VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'enrich_tickers'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS $$
import requests
import time

def enrich_tickers(session, p_batch_size: int, p_after_cik: str, user_agent: str) -> str:
    # Derive database/schema from current context (works in both interactive and Task)
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    headers = {
        'User-Agent': user_agent,
        'Accept-Encoding': 'gzip, deflate'
    }

    # Get distinct CIKs not yet checked, ordered, starting after cursor
    pending = session.sql(f"""
        SELECT DISTINCT CIK
        FROM {fqn}.FILING_INDEX
        WHERE TICKER IS NULL AND TICKER_CHECKED_AT IS NULL AND CIK > '{p_after_cik}'
        ORDER BY CIK
        LIMIT {int(p_batch_size)}
    """).to_pandas()

    if pending.empty:
        return "DONE"

    last_cik = pending['CIK'].iloc[-1]

    # Phase 1: Collect all CIK->ticker mappings via SEC API
    mappings = []
    errors = 0

    for _, row in pending.iterrows():
        cik = row['CIK'].lstrip('0') or '0'
        url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"

        try:
            resp = requests.get(url, headers=headers, timeout=30)
            time.sleep(0.11)

            if resp.status_code != 200:
                errors += 1
                continue

            data = resp.json()
            tickers = data.get('tickers', [])
            exchanges = data.get('exchanges', [])

            # Pick the best ticker: prefer one with a recognized exchange
            ticker = None
            if tickers and exchanges and len(tickers) == len(exchanges):
                # Prefer tickers on major exchanges (NYSE, Nasdaq, CBOE, etc.)
                major_exchanges = {'NYSE', 'Nasdaq', 'CBOE', 'NYSE Arca', 'NYSE MKT', 'Cboe BZX'}
                for t, e in zip(tickers, exchanges):
                    if e in major_exchanges:
                        ticker = t
                        break
                # Fallback: first ticker with any exchange
                if not ticker:
                    for t, e in zip(tickers, exchanges):
                        if e:
                            ticker = t
                            break
                # Last resort: first ticker
                if not ticker and tickers:
                    ticker = tickers[0]
            elif tickers:
                ticker = tickers[0]

            if ticker:
                mappings.append((row['CIK'], ticker))

        except Exception:
            errors += 1

    if not mappings:
        # No tickers found — still mark all CIKs in batch as checked
        cik_list = ", ".join(f"'{row['CIK']}'" for _, row in pending.iterrows())
        session.sql(f"""
            UPDATE {fqn}.FILING_INDEX
            SET TICKER_CHECKED_AT = CURRENT_TIMESTAMP()
            WHERE CIK IN ({cik_list}) AND TICKER_CHECKED_AT IS NULL
        """).collect()
        return f"No tickers found in batch ({len(pending)} CIKs checked), {errors} errors, last_cik={last_cik}"

    # Phase 2: Build VALUES clause and MERGE in one shot
    values_list = ", ".join(
        f"('{cik}', '{ticker.replace(chr(39), chr(39)+chr(39))}')"
        for cik, ticker in mappings
    )

    result = session.sql(f"""
        MERGE INTO {fqn}.FILING_INDEX tgt
        USING (SELECT COLUMN1 AS CIK, COLUMN2 AS TICKER FROM VALUES {values_list}) src
        ON tgt.CIK = src.CIK AND tgt.TICKER IS NULL
        WHEN MATCHED THEN UPDATE SET tgt.TICKER = src.TICKER, tgt.TICKER_CHECKED_AT = CURRENT_TIMESTAMP()
    """).collect()

    rows_updated = result[0]['number of rows updated'] if result else 0

    # Mark ALL CIKs in batch as checked (including those that had no ticker)
    cik_list = ", ".join(f"'{row['CIK']}'" for _, row in pending.iterrows())
    session.sql(f"""
        UPDATE {fqn}.FILING_INDEX
        SET TICKER_CHECKED_AT = CURRENT_TIMESTAMP()
        WHERE CIK IN ({cik_list}) AND TICKER_CHECKED_AT IS NULL
    """).collect()

    return f"Enriched {len(mappings)} CIKs ({rows_updated} filings updated), {errors} errors, last_cik={last_cik}"
$$;


-- =============================================================================
-- SP: ENRICH_TICKERS_BULK
-- =============================================================================
-- Downloads SEC's company_tickers.json (single bulk file mapping CIK→ticker)
-- and MERGEs into FILING_INDEX. This catches companies the per-CIK API misses.
-- Much faster than per-CIK calls: one HTTP request covers all ~12K active filers.
--
-- Usage:
--   CALL ENRICH_TICKERS_BULK($config_user_agent);
-- =============================================================================

CREATE OR REPLACE PROCEDURE ENRICH_TICKERS_BULK(
    USER_AGENT VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'enrich_tickers_bulk'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS $$
import requests
import json

def enrich_tickers_bulk(session, user_agent: str) -> str:
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    headers = {
        'User-Agent': user_agent,
        'Accept-Encoding': 'gzip, deflate'
    }

    # Download bulk company_tickers.json from SEC
    url = "https://www.sec.gov/files/company_tickers.json"
    resp = requests.get(url, headers=headers, timeout=60)

    if resp.status_code != 200:
        return f"ERROR: SEC returned status {resp.status_code}"

    data = resp.json()

    # Format: {"0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."}, ...}
    # Deduplicate by CIK (SEC file can have multiple entries for multi-class shares)
    seen_ciks = {}
    for entry in data.values():
        cik = str(entry.get('cik_str', '')).zfill(10)
        ticker = entry.get('ticker', '')
        if cik and ticker and cik not in seen_ciks:
            seen_ciks[cik] = ticker
    mappings = list(seen_ciks.items())

    if not mappings:
        return "ERROR: No mappings found in company_tickers.json"

    # Batch MERGE in chunks of 5000 to avoid SQL size limits
    total_updated = 0
    batch_size = 5000

    for i in range(0, len(mappings), batch_size):
        batch = mappings[i:i + batch_size]
        values_list = ", ".join(
            f"('{cik}', '{ticker.replace(chr(39), chr(39)+chr(39))}')"
            for cik, ticker in batch
        )

        result = session.sql(f"""
            MERGE INTO {fqn}.FILING_INDEX tgt
            USING (SELECT COLUMN1 AS CIK, COLUMN2 AS TICKER FROM VALUES {values_list}) src
            ON tgt.CIK = src.CIK AND tgt.TICKER IS NULL
            WHEN MATCHED THEN UPDATE SET tgt.TICKER = src.TICKER, tgt.TICKER_CHECKED_AT = CURRENT_TIMESTAMP()
        """).collect()

        rows = result[0]['number of rows updated'] if result else 0
        total_updated += rows

    # Mark all CIKs from bulk file as checked (even if they didn't match any filings)
    session.sql(f"""
        UPDATE {fqn}.FILING_INDEX
        SET TICKER_CHECKED_AT = CURRENT_TIMESTAMP()
        WHERE TICKER IS NOT NULL AND TICKER_CHECKED_AT IS NULL
    """).collect()

    return f"Bulk enrichment complete: {len(mappings)} CIK->ticker mappings loaded, {total_updated} filings updated"
$$;


-- =============================================================================
-- Helper view: Ingestion / index completeness status
-- =============================================================================
CREATE OR REPLACE VIEW V_INGESTION_STATUS AS
SELECT
    FORM_TYPE,
    COUNT(*)                                          AS TOTAL_IN_INDEX,
    COUNT(DOWNLOADED_AT)                              AS DOWNLOADED,
    COUNT(*) - COUNT(DOWNLOADED_AT)                   AS PENDING_DOWNLOAD,
    COUNT(TICKER)                                     AS WITH_TICKER,
    TO_CHAR(MIN(FILED_AT), 'YYYY-MM-DD')             AS EARLIEST_FILING,
    TO_CHAR(MAX(FILED_AT), 'YYYY-MM-DD')             AS LATEST_FILING
FROM FILING_INDEX
GROUP BY FORM_TYPE
ORDER BY TOTAL_IN_INDEX DESC;


-- =============================================================================
-- EXECUTION
-- =============================================================================
-- Recommended order:
--   1. Bulk enrichment (fast — one HTTP call, covers ~10K+ CIKs):
--      CALL ENRICH_TICKERS_BULK($config_user_agent);
--
--   2. Per-CIK enrichment for any remaining (catches CIKs not in bulk file):
--      CALL ENRICH_TICKERS(500, '0000000000', $config_user_agent);
--      -- Repeat until it returns "DONE"
--
-- Full enrichment loop (run in a Snowsight worksheet):
-- BEGIN
--     -- Phase 1: Bulk (one call)
--     CALL ENRICH_TICKERS_BULK($config_user_agent);
--     -- Phase 2: Per-CIK for remaining
--     LET result VARCHAR := '';
--     LET last_cik VARCHAR := '0000000000';
--     LET iteration INT := 0;
--     LOOP
--         iteration := iteration + 1;
--         CALL ENRICH_TICKERS(500, :last_cik, $config_user_agent);
--         result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
--         IF (CONTAINS(:result, 'DONE')) THEN BREAK; END IF;
--         IF (:iteration >= 50) THEN BREAK; END IF;
--         last_cik := REGEXP_SUBSTR(:result, 'last_cik=([0-9]+)', 1, 1, 'e');
--         IF (:last_cik IS NULL) THEN BREAK; END IF;
--     END LOOP;
--     RETURN 'Ticker enrichment complete after ' || :iteration || ' batches';
-- END;
