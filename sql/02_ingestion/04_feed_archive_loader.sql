-- =============================================================================
-- 04: Feed Archive Loader (High-Throughput Alternative)
-- =============================================================================
-- Alternative ingestion method using SEC EDGAR daily Feed archives.
-- Downloads daily tar.gz files (each containing ALL filings for that day),
-- extracts metadata + content in a single pass.
--
-- Throughput: ~10,000 filings per HTTP request vs 1 filing/request (standard method)
-- Use case: Full-corpus ingestion (1M+ filings) — reduces download from days to hours
--
-- Feed URL pattern:
--   https://www.sec.gov/Archives/edgar/Feed/{YYYY}/QTR{Q}/{YYYYMMDD}.nc.tar.gz
--
-- Advantages over standard method:
--   - 100-1000x fewer HTTP requests
--   - SEC headers already contain PERIOD_OF_REPORT and SIC code
--   - Eliminates need for separate metadata backfill step
--   - Complete submission text (not just primary document)
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse_ingest);

-- =============================================================================
-- SP: LOAD_FEED_ARCHIVE
-- =============================================================================
-- Downloads a single day's feed archive from SEC EDGAR, extracts target form
-- types, parses SEC headers, and inserts into both FILING_INDEX and FILING_CONTENT.
--
-- Parameters:
--   FEED_DATE:   Date to download (YYYY-MM-DD format)
--   USER_AGENT:  SEC EDGAR user agent string
--
-- Usage:
--   CALL LOAD_FEED_ARCHIVE('2025-01-02', 'YourCompany SEC-Filing-Project admin@company.com');
--
-- Returns summary of loaded filings or error message.
-- =============================================================================

CREATE OR REPLACE PROCEDURE LOAD_FEED_ARCHIVE(
    FEED_DATE VARCHAR,
    USER_AGENT VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
HANDLER = 'load_feed_archive'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS $$
import requests
import tarfile
import io
import re
import time
import pandas as pd
from datetime import datetime, timezone

TARGET_FORMS = {
    '10-K', '10-K/A', '10-KT', '10-KSB',
    '10-Q', '10-Q/A', '10-QSB',
    '8-K',  '8-K/A'
}

MAX_TEXT_CHARS = 16_000_000  # 16MB Snowflake TEXT column limit

def parse_sec_header(text):
    """Extract metadata from the SEC-HEADER block of a submission.
    Handles both plain-text (TXT downloads) and SGML (feed archive) formats.
    """
    header = {}
    snippet = text[:5000]

    # CIK (plain-text or SGML)
    m = re.search(r'CENTRAL INDEX KEY:\s*(\d+)', snippet)
    if not m:
        m = re.search(r'<CIK>(\d+)', snippet)
    if m:
        header['CIK'] = m.group(1).zfill(10)

    # Company name
    m = re.search(r'COMPANY CONFORMED NAME:\s*(.+)', snippet)
    if not m:
        m = re.search(r'<CONFORMED-NAME>([^<\n]+)', snippet)
    if m:
        header['COMPANY_NAME'] = m.group(1).strip()[:500]

    # Form type
    m = re.search(r'FORM TYPE:\s*(.+)', snippet)
    if not m:
        m = re.search(r'<TYPE>([^\s<]+)', snippet)
    if m:
        header['FORM_TYPE'] = m.group(1).strip()

    # Date filed
    m = re.search(r'FILED AS OF DATE:\s*(\d{8})', snippet)
    if not m:
        m = re.search(r'<FILING-DATE>(\d{8})', snippet)
    if m:
        try:
            dt = datetime.strptime(m.group(1), '%Y%m%d').replace(tzinfo=timezone.utc)
            header['FILED_AT'] = dt.strftime('%Y-%m-%d %H:%M:%S +0000')
        except ValueError:
            pass

    # Period of report
    m = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d{8})', snippet)
    if not m:
        m = re.search(r'<PERIOD>(\d{8})', snippet)
    if m:
        header['PERIOD_OF_REPORT'] = m.group(1)

    # SIC code
    m = re.search(r'STANDARD INDUSTRIAL CLASSIFICATION:.*\[(\d+)\]', snippet)
    if not m:
        m = re.search(r'<ASSIGNED-SIC>(\d+)', snippet)
    if m:
        header['SIC_CODE'] = m.group(1).zfill(4)

    # Accession number (from filename or header)
    m = re.search(r'ACCESSION NUMBER:\s*(\S+)', snippet)
    if not m:
        m = re.search(r'<ACCESSION-NUMBER>([^\s<]+)', snippet)
    if m:
        header['ACCESSION_NO'] = m.group(1).strip()

    return header


def load_feed_archive(session, feed_date: str, user_agent: str) -> str:
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    FLUSH_EVERY = 100  # Flush to Snowflake every N filings to bound memory

    # Determine quarter from date
    dt = datetime.strptime(feed_date, '%Y-%m-%d')
    year = dt.year
    quarter = (dt.month - 1) // 3 + 1
    date_compact = dt.strftime('%Y%m%d')

    url = f"https://www.sec.gov/Archives/edgar/Feed/{year}/QTR{quarter}/{date_compact}.nc.tar.gz"
    headers = {'User-Agent': user_agent, 'Accept-Encoding': 'gzip, deflate'}

    # Streaming download with retry for large archives (>800MB)
    content = None
    expected_size = 0
    download_attempts = 0
    download_seconds = 0

    for attempt in range(3):
        download_attempts = attempt + 1
        bytes_received = 0
        t0 = time.time()
        try:
            resp = requests.get(url, headers=headers, timeout=600, stream=True)
            if resp.status_code == 404:
                session.sql(f"""
                    MERGE INTO {fqn}._FEED_INGEST_LOG t
                    USING (SELECT '{feed_date}' AS feed_date) s ON t.feed_date = s.feed_date
                    WHEN NOT MATCHED THEN INSERT (feed_date, loaded, status, started_at, updated_at)
                        VALUES (s.feed_date, 0, 'SKIPPED_404', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                    WHEN MATCHED AND t.status NOT IN ('DONE', 'SKIPPED_404') THEN UPDATE
                        SET status = 'SKIPPED_404', updated_at = CURRENT_TIMESTAMP()
                """).collect()
                return f"No feed archive for {feed_date} (HTTP 404 — possibly weekend/holiday)"
            if resp.status_code == 403:
                session.sql(f"""
                    MERGE INTO {fqn}._FEED_INGEST_LOG t
                    USING (SELECT '{feed_date}' AS feed_date) s ON t.feed_date = s.feed_date
                    WHEN NOT MATCHED THEN INSERT (feed_date, loaded, status, started_at, updated_at)
                        VALUES (s.feed_date, 0, 'SKIPPED_403', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                    WHEN MATCHED AND t.status NOT IN ('DONE', 'SKIPPED_403') THEN UPDATE
                        SET status = 'SKIPPED_403', updated_at = CURRENT_TIMESTAMP()
                """).collect()
                return f"No feed archive for {feed_date} (HTTP 403 — access denied)"
            if resp.status_code != 200:
                session.sql(f"""
                    MERGE INTO {fqn}._FEED_INGEST_LOG t
                    USING (SELECT '{feed_date}' AS feed_date) s ON t.feed_date = s.feed_date
                    WHEN NOT MATCHED THEN INSERT (feed_date, loaded, status, started_at, updated_at)
                        VALUES (s.feed_date, 0, 'ERROR', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                    WHEN MATCHED AND t.status NOT IN ('DONE', 'SKIPPED_404', 'SKIPPED_403') THEN UPDATE
                        SET status = 'ERROR', updated_at = CURRENT_TIMESTAMP()
                """).collect()
                return f"ERROR: HTTP {resp.status_code} fetching {url}"
            expected_size = int(resp.headers.get('Content-Length', 0))
            chunks_dl = []
            for chunk in resp.iter_content(chunk_size=10*1024*1024):  # 10MB chunks
                chunks_dl.append(chunk)
                bytes_received += len(chunk)
            content = b''.join(chunks_dl)
            download_seconds = round(time.time() - t0, 1)
            break
        except (requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            download_seconds = round(time.time() - t0, 1)
            if attempt == 2:
                session.sql(f"""
                    MERGE INTO {fqn}._FEED_INGEST_LOG t
                    USING (SELECT '{feed_date}' AS feed_date) s ON t.feed_date = s.feed_date
                    WHEN NOT MATCHED THEN INSERT (feed_date, loaded, status, started_at, updated_at)
                        VALUES (s.feed_date, 0, 'ERROR', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                    WHEN MATCHED AND t.status NOT IN ('DONE') THEN UPDATE
                        SET status = 'ERROR', updated_at = CURRENT_TIMESTAMP()
                """).collect()
                return (f"ERROR: Download failed after 3 attempts for {feed_date}. "
                        f"Last attempt: {bytes_received/(1024*1024):.0f} MB of "
                        f"{expected_size/(1024*1024):.0f} MB expected "
                        f"({download_seconds}s). Error: {str(e)[:200]}")
            time.sleep(30 * (attempt + 1))  # 30s, 60s backoff

    # Log start of processing
    session.sql(f"""
        MERGE INTO {fqn}._FEED_INGEST_LOG t
        USING (SELECT '{feed_date}' AS feed_date) s ON t.feed_date = s.feed_date
        WHEN NOT MATCHED THEN INSERT (feed_date, loaded, status, started_at, updated_at)
            VALUES (s.feed_date, 0, 'DOWNLOADING', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        WHEN MATCHED AND t.status != 'DONE' THEN UPDATE
            SET status = 'DOWNLOADING', updated_at = CURRENT_TIMESTAMP()
    """).collect()

    def _flush_batch(idx_rows, cnt_rows):
        """Flush accumulated rows to Snowflake and clear memory."""
        if not idx_rows:
            return
        df_index = pd.DataFrame(idx_rows).drop_duplicates(subset=['ACCESSION_NO'])
        tmp_idx = f"{fqn}._FEED_INDEX_TMP"
        session.create_dataframe(df_index).write.mode("overwrite").save_as_table(tmp_idx, table_type="temporary")
        session.sql(f"""
            INSERT INTO {fqn}.FILING_INDEX
                (ACCESSION_NO, CIK, COMPANY_NAME, FORM_TYPE, FILED_AT,
                 PRIMARY_DOC_URL, FILING_INDEX_URL, IS_AMENDMENT, PERIOD_OF_REPORT, SIC_CODE)
            SELECT t.ACCESSION_NO, t.CIK, t.COMPANY_NAME, t.FORM_TYPE, t.FILED_AT::TIMESTAMP_TZ,
                   t.PRIMARY_DOC_URL, t.FILING_INDEX_URL, t.IS_AMENDMENT::BOOLEAN,
                   TRY_TO_DATE(t.PERIOD_OF_REPORT, 'YYYYMMDD'), t.SIC_CODE
            FROM {tmp_idx} t
            WHERE NOT EXISTS (
                SELECT 1 FROM {fqn}.FILING_INDEX fi WHERE fi.ACCESSION_NO = t.ACCESSION_NO
            )
        """).collect()
        session.sql(f"""
            UPDATE {fqn}.FILING_INDEX fi
            SET PERIOD_OF_REPORT = COALESCE(fi.PERIOD_OF_REPORT, TRY_TO_DATE(t.PERIOD_OF_REPORT, 'YYYYMMDD')),
                SIC_CODE = COALESCE(fi.SIC_CODE, t.SIC_CODE)
            FROM {tmp_idx} t
            WHERE fi.ACCESSION_NO = t.ACCESSION_NO
              AND (fi.PERIOD_OF_REPORT IS NULL OR fi.SIC_CODE IS NULL)
        """).collect()

        df_content = pd.DataFrame(cnt_rows).drop_duplicates(subset=['ACCESSION_NO'])
        tmp_cnt = f"{fqn}._FEED_CONTENT_TMP"
        session.create_dataframe(df_content).write.mode("overwrite").save_as_table(tmp_cnt, table_type="temporary")
        session.sql(f"""
            INSERT INTO {fqn}.FILING_CONTENT
                (ACCESSION_NO, CONTENT_TEXT, STAGE_FILE_PATH, FILE_SIZE_BYTES,
                 FILE_FORMAT, PARSE_STATUS, PARSE_ERROR)
            SELECT t.ACCESSION_NO, t.CONTENT_TEXT, t.STAGE_FILE_PATH,
                   t.FILE_SIZE_BYTES::NUMBER, t.FILE_FORMAT, t.PARSE_STATUS, t.PARSE_ERROR
            FROM {tmp_cnt} t
            WHERE NOT EXISTS (
                SELECT 1 FROM {fqn}.FILING_CONTENT fc WHERE fc.ACCESSION_NO = t.ACCESSION_NO
            )
        """).collect()
        session.sql(f"""
            UPDATE {fqn}.FILING_INDEX fi
            SET DOWNLOADED_AT = CURRENT_TIMESTAMP()
            WHERE fi.ACCESSION_NO IN (SELECT ACCESSION_NO FROM {tmp_cnt})
              AND fi.DOWNLOADED_AT IS NULL
        """).collect()
        session.sql(f"DROP TABLE IF EXISTS {tmp_idx}").collect()
        session.sql(f"DROP TABLE IF EXISTS {tmp_cnt}").collect()

    # Parse tar.gz archive with batch flushing
    index_rows = []
    content_rows = []
    skipped = 0
    total_loaded = 0

    try:
        tar_bytes = io.BytesIO(content)
        with tarfile.open(fileobj=tar_bytes, mode='r:gz') as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                f = tar.extractfile(member)
                if f is None:
                    continue
                try:
                    raw_text = f.read().decode('latin-1', errors='replace')
                except Exception:
                    skipped += 1
                    continue

                # Parse header
                hdr = parse_sec_header(raw_text)
                form_type = hdr.get('FORM_TYPE', '')

                if form_type not in TARGET_FORMS:
                    skipped += 1
                    continue

                accession_no = hdr.get('ACCESSION_NO')
                if not accession_no:
                    parts = member.name.split('/')
                    if len(parts) >= 3:
                        accession_no = parts[-1].replace('.txt', '')
                if not accession_no:
                    skipped += 1
                    continue

                cik = hdr.get('CIK', '0000000000')
                company_name = hdr.get('COMPANY_NAME', '')
                filed_at = hdr.get('FILED_AT')
                period_of_report = hdr.get('PERIOD_OF_REPORT')
                sic_code = hdr.get('SIC_CODE')

                primary_url = f"https://www.sec.gov/Archives/edgar/data/{cik.lstrip('0')}/{accession_no.replace('-', '')}/{accession_no}.txt"

                index_rows.append({
                    'ACCESSION_NO': accession_no,
                    'CIK': cik,
                    'COMPANY_NAME': company_name,
                    'FORM_TYPE': form_type,
                    'FILED_AT': filed_at,
                    'PRIMARY_DOC_URL': primary_url[:1000],
                    'FILING_INDEX_URL': None,
                    'IS_AMENDMENT': '/' in form_type,
                    'PERIOD_OF_REPORT': period_of_report,
                    'SIC_CODE': sic_code
                })

                # Extract document content from <TEXT> block (the actual filing)
                text_match = re.search(r'<TEXT>(.*?)</TEXT>', raw_text, re.DOTALL)
                doc_content = text_match.group(1) if text_match else raw_text

                content_rows.append({
                    'ACCESSION_NO': accession_no,
                    'CONTENT_TEXT': doc_content[:MAX_TEXT_CHARS],
                    'STAGE_FILE_PATH': None,
                    'FILE_SIZE_BYTES': len(raw_text),
                    'FILE_FORMAT': 'FEED',
                    'PARSE_STATUS': 'PENDING',
                    'PARSE_ERROR': None
                })

                # Batch flush to bound memory
                if len(index_rows) >= FLUSH_EVERY:
                    _flush_batch(index_rows, content_rows)
                    total_loaded += len(index_rows)
                    index_rows = []
                    content_rows = []
                    # Update progress
                    session.sql(f"""
                        UPDATE {fqn}._FEED_INGEST_LOG
                        SET loaded = {total_loaded}, status = 'LOADING', updated_at = CURRENT_TIMESTAMP()
                        WHERE feed_date = '{feed_date}'
                    """).collect()

    except Exception as e:
        # Flush any accumulated rows before reporting error
        if index_rows:
            _flush_batch(index_rows, content_rows)
            total_loaded += len(index_rows)
        return f"ERROR parsing tar.gz (loaded {total_loaded} before error): {str(e)[:500]}"

    # Final flush for remaining rows
    if index_rows:
        _flush_batch(index_rows, content_rows)
        total_loaded += len(index_rows)

    if total_loaded == 0:
        return f"No target filings in feed archive for {feed_date}. Skipped: {skipped}"

    # Mark day as complete
    session.sql(f"""
        UPDATE {fqn}._FEED_INGEST_LOG
        SET loaded = {total_loaded}, status = 'DONE', updated_at = CURRENT_TIMESTAMP()
        WHERE feed_date = '{feed_date}'
    """).collect()

    return (f"Feed archive {feed_date}: loaded {total_loaded} filings, skipped {skipped}. "
            f"Download: {expected_size/(1024*1024):.0f} MB in {download_seconds}s "
            f"({download_attempts} attempt{'s' if download_attempts > 1 else ''})")
$$;


-- =============================================================================
-- SP: LOAD_FEED_DATE_RANGE
-- =============================================================================
-- Loads all feed archives for a date range. Skips weekends/holidays gracefully.
--
-- Usage:
--   CALL LOAD_FEED_DATE_RANGE('2025-01-01', '2025-03-31');
-- =============================================================================

CREATE OR REPLACE PROCEDURE LOAD_FEED_DATE_RANGE(
    START_DATE VARCHAR,
    END_DATE VARCHAR,
    USER_AGENT VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    cur_date DATE;
    end_dt DATE;
    result VARCHAR;
    loaded INT DEFAULT 0;
    skipped INT DEFAULT 0;
    day_status VARCHAR;
BEGIN
    cur_date := TO_DATE(:START_DATE);
    end_dt := TO_DATE(:END_DATE);

    WHILE (:cur_date <= :end_dt) DO
        -- Skip weekends
        IF (DAYOFWEEK(:cur_date) NOT IN (0, 6)) THEN
            -- Skip days already completed or known to have no archive
            day_status := '';
            BEGIN
                SELECT STATUS INTO :day_status FROM _FEED_INGEST_LOG WHERE FEED_DATE = :cur_date;
            EXCEPTION WHEN OTHER THEN day_status := ''; END;

            IF (:day_status IN ('DONE', 'SKIPPED_404', 'SKIPPED_403')) THEN
                skipped := skipped + 1;
            ELSE
                CALL LOAD_FEED_ARCHIVE(TO_VARCHAR(:cur_date, 'YYYY-MM-DD'), :USER_AGENT);
                result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
                IF (NOT CONTAINS(:result, 'ERROR') AND NOT CONTAINS(:result, 'No target')) THEN
                    loaded := loaded + 1;
                ELSE
                    skipped := skipped + 1;
                END IF;
            END IF;
        END IF;
        cur_date := DATEADD('day', 1, :cur_date);
    END WHILE;

    RETURN 'Feed range complete: ' || :loaded || ' days loaded, ' || :skipped || ' skipped';
END;
$$;


-- =============================================================================
-- EXECUTION (PRIMARY ingestion method — recommended for all bulk loads)
-- =============================================================================
-- Quick Start: single day (376 filings, ~2 minutes)
--   CALL LOAD_FEED_ARCHIVE('2025-02-21', $config_user_agent);
--
-- One month:
--   CALL LOAD_FEED_DATE_RANGE($config_ingest_start_year || '-01-01', $config_ingest_start_year || '-01-31', $config_user_agent);
--
-- Full year (use Feed Ingestion DAG for parallel execution):
--   See sql/02_ingestion/05_feed_ingestion_dag.sql
--
-- Monitor progress during long runs:
--   SELECT status, COUNT(*) AS days, SUM(loaded) AS filings
--   FROM _FEED_INGEST_LOG GROUP BY 1;
