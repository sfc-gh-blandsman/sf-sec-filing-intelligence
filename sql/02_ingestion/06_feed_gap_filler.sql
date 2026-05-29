-- =============================================================================
-- 06: Feed Gap Filler (Tier 2 — Individual Filing Download)
-- =============================================================================
-- After Tier 1 (nc.tar.gz bulk ingestion) completes, this SP identifies filings
-- listed in the EDGAR daily index but missing from our FILING_INDEX, and downloads
-- them individually.
--
-- The nc.tar.gz archives are sometimes incomplete (missing 10-30% of filings).
-- This SP closes those gaps by:
--   1. Fetching the EDGAR daily index (company.YYYYMMDD.idx)
--   2. Parsing target form filings + accession numbers
--   3. Comparing vs FILING_INDEX
--   4. Downloading each missing filing individually via its EDGAR URL
--   5. Inserting into FILING_INDEX + FILING_CONTENT
--
-- Parameters:
--   P_FEED_DATE:   Date to gap-fill (YYYY-MM-DD)
--   P_USER_AGENT:  SEC EDGAR user agent string
--
-- Usage:
--   CALL FILL_FEED_GAPS('2025-10-31');
--
-- For bulk audit + fill:
--   CALL VALIDATE_FEED_COMPLETENESS(2025, 2025);
--
-- Dependencies:
--   - SEC_EDGAR_EAI external access integration
--   - FILING_INDEX, FILING_CONTENT, _FEED_INGEST_LOG tables
--   - _CFG() function
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse_ingest);

-- =============================================================================
-- SP: FILL_FEED_GAPS
-- =============================================================================

CREATE OR REPLACE PROCEDURE FILL_FEED_GAPS(
    P_FEED_DATE VARCHAR,
    P_USER_AGENT VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'pandas')
HANDLER = 'fill_feed_gaps'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS $$
import requests
import re
import time
import pandas as pd

TARGET_FORMS = {
    '10-K', '10-K/A', '10-KT', '10-KSB',
    '10-Q', '10-Q/A', '10-QSB',
    '8-K', '8-K/A'
}
MAX_TEXT_CHARS = 16_000_000

def fill_feed_gaps(session, p_feed_date, p_user_agent):
    from datetime import datetime
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    dt = datetime.strptime(p_feed_date, '%Y-%m-%d')
    quarter = (dt.month - 1) // 3 + 1
    date_compact = dt.strftime('%Y%m%d')

    # Step 1: Fetch EDGAR daily index
    idx_url = f"https://www.sec.gov/Archives/edgar/daily-index/{dt.year}/QTR{quarter}/company.{date_compact}.idx"
    headers = {'User-Agent': p_user_agent}

    resp = requests.get(idx_url, headers=headers, timeout=30)
    if resp.status_code != 200:
        return f"Cannot fetch daily index: HTTP {resp.status_code} for {idx_url}"

    # Step 2: Parse the fixed-width index file
    lines = resp.text.strip().split('\n')
    data_start = 0
    for i, line in enumerate(lines):
        if line.startswith('---'):
            data_start = i + 1
            break

    # Extract target filings from index
    index_filings = []  # list of {accession, cik, company, form_type, filename}
    for line in lines[data_start:]:
        if len(line) < 100:
            continue
        # Fixed-width format: Company(62) FormType(12) CIK(12) DateFiled(12) Filename(rest)
        form_type = line[62:74].strip()
        cik_str = line[74:86].strip()
        filename = line[98:].strip() if len(line) > 98 else ''

        if form_type not in TARGET_FORMS:
            continue

        # Extract accession from filename: edgar/data/CIK/ACCESSION.txt
        acc_match = re.search(r'(\d{10}-\d{2}-\d{6})', filename)
        if not acc_match:
            continue

        accession = acc_match.group(1)
        company = line[:62].strip()
        index_filings.append({
            'accession': accession,
            'cik': cik_str.zfill(10),
            'company': company,
            'form_type': form_type,
            'filename': filename
        })

    if not index_filings:
        return f"No target filings in daily index for {p_feed_date}"

    # Step 3: Find what's missing from our DB
    all_accessions = [f["accession"] for f in index_filings]
    # Check in batches of 500
    existing = set()
    for i in range(0, len(all_accessions), 500):
        batch = all_accessions[i:i+500]
        acc_list = ",".join(f"'{a}'" for a in batch)
        rows = session.sql(f"""
            SELECT ACCESSION_NO FROM {fqn}.FILING_INDEX
            WHERE ACCESSION_NO IN ({acc_list})
        """).collect()
        existing.update(r["ACCESSION_NO"] for r in rows)

    missing = [f for f in index_filings if f["accession"] not in existing]

    if not missing:
        # All filings present — mark as complete
        session.sql(f"""
            UPDATE {fqn}._FEED_INGEST_LOG
            SET status = 'DONE', loaded = {len(index_filings)}, updated_at = CURRENT_TIMESTAMP()
            WHERE feed_date = '{p_feed_date}' AND status != 'DONE'
        """).collect()
        return f"No gaps for {p_feed_date}. All {len(index_filings)} target filings present."

    # Step 4: Download each missing filing individually
    downloaded = 0
    errors = []

    for filing in missing:
        acc = filing["accession"]
        cik = filing["cik"].lstrip('0') or '0'
        acc_nodashes = acc.replace('-', '')
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodashes}/{acc}.txt"

        time.sleep(0.5)  # Rate limit: 2 req/sec max

        try:
            file_resp = requests.get(filing_url, headers=headers, timeout=60)
            if file_resp.status_code != 200:
                errors.append(f"{acc}: HTTP {file_resp.status_code}")
                continue

            raw_text = file_resp.text
            if len(raw_text) < 100:
                errors.append(f"{acc}: empty response")
                continue

            # Parse header for metadata
            snippet = raw_text[:5000]
            filed_at = None
            m = re.search(r'FILED AS OF DATE:\s*(\d{8})', snippet)
            if not m:
                m = re.search(r'<FILING-DATE>(\d{8})', snippet)
            if m:
                d = m.group(1)
                filed_at = f"{d[:4]}-{d[4:6]}-{d[6:8]}"

            period_of_report = None
            m = re.search(r'CONFORMED PERIOD OF REPORT:\s*(\d{8})', snippet)
            if not m:
                m = re.search(r'<PERIOD>(\d{8})', snippet)
            if m:
                period_of_report = m.group(1)

            sic_code = None
            m = re.search(r'ASSIGNED SIC:\s*(\d+)', snippet)
            if not m:
                m = re.search(r'<ASSIGNED-SIC>(\d+)', snippet)
            if m:
                sic_code = m.group(1)

            is_amendment = '/' in filing["form_type"]
            primary_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodashes}/{acc}.txt"

            # Extract document content
            text_match = re.search(r'<TEXT>(.*?)</TEXT>', raw_text, re.DOTALL)
            doc_content = text_match.group(1) if text_match else raw_text

            # Insert into FILING_INDEX
            filed_at_sql = f"'{filed_at}'" if filed_at else "NULL"
            por_sql = f"TRY_TO_DATE('{period_of_report}', 'YYYYMMDD')" if period_of_report else "NULL"
            sic_sql = f"'{sic_code}'" if sic_code else "NULL"

            session.sql(f"""
                INSERT INTO {fqn}.FILING_INDEX
                    (ACCESSION_NO, CIK, COMPANY_NAME, FORM_TYPE, FILED_AT,
                     PRIMARY_DOC_URL, IS_AMENDMENT, PERIOD_OF_REPORT, SIC_CODE, DOWNLOADED_AT)
                SELECT '{acc}', '{filing["cik"]}',
                       '{filing["company"].replace(chr(39), chr(39)+chr(39))}',
                       '{filing["form_type"]}',
                       {filed_at_sql}::TIMESTAMP_TZ,
                       '{primary_url}', {is_amendment},
                       {por_sql}, {sic_sql}, CURRENT_TIMESTAMP()
                WHERE NOT EXISTS (
                    SELECT 1 FROM {fqn}.FILING_INDEX WHERE ACCESSION_NO = '{acc}'
                )
            """).collect()

            # Insert into FILING_CONTENT
            content_escaped = doc_content[:MAX_TEXT_CHARS].replace("'", "''").replace("\\", "\\\\")
            # Use DataFrame approach for large content
            df = pd.DataFrame([{
                'ACCESSION_NO': acc,
                'CONTENT_TEXT': doc_content[:MAX_TEXT_CHARS],
                'STAGE_FILE_PATH': None,
                'FILE_SIZE_BYTES': len(raw_text),
                'FILE_FORMAT': 'GAP_FILL',
                'PARSE_STATUS': 'PENDING',
                'PARSE_ERROR': None
            }])
            tmp_tbl = f"{fqn}._GAP_FILL_TMP"
            session.create_dataframe(df).write.mode("overwrite").save_as_table(tmp_tbl, table_type="temporary")
            session.sql(f"""
                INSERT INTO {fqn}.FILING_CONTENT
                    (ACCESSION_NO, CONTENT_TEXT, STAGE_FILE_PATH, FILE_SIZE_BYTES,
                     FILE_FORMAT, PARSE_STATUS, PARSE_ERROR)
                SELECT ACCESSION_NO, CONTENT_TEXT, STAGE_FILE_PATH,
                       FILE_SIZE_BYTES::NUMBER, FILE_FORMAT, PARSE_STATUS, PARSE_ERROR
                FROM {tmp_tbl}
                WHERE NOT EXISTS (
                    SELECT 1 FROM {fqn}.FILING_CONTENT WHERE ACCESSION_NO = '{acc}'
                )
            """).collect()
            session.sql(f"DROP TABLE IF EXISTS {tmp_tbl}").collect()

            downloaded += 1

        except Exception as e:
            errors.append(f"{acc}: {str(e)[:100]}")
            continue

    # Step 5: Update feed log
    new_total = len(existing) + downloaded
    if errors:
        final_status = 'INCOMPLETE'
    else:
        final_status = 'DONE'

    session.sql(f"""
        UPDATE {fqn}._FEED_INGEST_LOG
        SET loaded = {new_total}, status = '{final_status}', updated_at = CURRENT_TIMESTAMP()
        WHERE feed_date = '{p_feed_date}'
    """).collect()

    error_str = f" Errors: {', '.join(errors[:5])}" if errors else ""
    return (f"Gap fill {p_feed_date}: {downloaded} filings downloaded, "
            f"{len(errors)} failed. Total now: {new_total}/{len(index_filings)} "
            f"({final_status}).{error_str}")
$$;


-- =============================================================================
-- SP: VALIDATE_FEED_COMPLETENESS (Standalone Audit)
-- =============================================================================
-- Checks all days in a year range against the EDGAR daily index.
-- Identifies gaps and optionally fills them.
--
-- Usage:
--   CALL VALIDATE_FEED_COMPLETENESS(2025, 2025);        -- audit only
--   CALL VALIDATE_FEED_COMPLETENESS(2021, 2026, TRUE);  -- audit + fill gaps
-- =============================================================================

CREATE OR REPLACE PROCEDURE VALIDATE_FEED_COMPLETENESS(
    P_START_YEAR INT,
    P_END_YEAR INT,
    P_AUTO_FILL BOOLEAN DEFAULT FALSE,
    P_USER_AGENT VARCHAR DEFAULT 'Snowflake SEC-Filing-Project admin@company.com'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'validate_feed_completeness'
EXTERNAL_ACCESS_INTEGRATIONS = (IDENTIFIER($config_eai_name))
EXECUTE AS CALLER
AS $$
import requests
import re
import time

TARGET_FORMS = {
    '10-K', '10-K/A', '10-KT', '10-KSB',
    '10-Q', '10-Q/A', '10-QSB',
    '8-K', '8-K/A'
}

def validate_feed_completeness(session, p_start_year, p_end_year, p_auto_fill, p_user_agent):
    from datetime import datetime, timedelta
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    # Get all DONE days in range
    rows = session.sql(f"""
        SELECT FEED_DATE, LOADED FROM {fqn}._FEED_INGEST_LOG
        WHERE STATUS IN ('DONE', 'PARTIAL', 'INCOMPLETE')
          AND LEFT(FEED_DATE, 4)::INT BETWEEN {p_start_year} AND {p_end_year}
        ORDER BY FEED_DATE
    """).collect()

    if not rows:
        return f"No DONE/PARTIAL days found for {p_start_year}-{p_end_year}"

    gaps_found = []
    days_checked = 0
    total_missing = 0

    for row in rows:
        feed_date = row["FEED_DATE"]
        loaded = row["LOADED"] or 0

        dt = datetime.strptime(feed_date, '%Y-%m-%d')
        quarter = (dt.month - 1) // 3 + 1
        date_compact = dt.strftime('%Y%m%d')

        idx_url = f"https://www.sec.gov/Archives/edgar/daily-index/{dt.year}/QTR{quarter}/company.{date_compact}.idx"
        headers = {'User-Agent': p_user_agent}

        time.sleep(0.5)  # Rate limit

        try:
            resp = requests.get(idx_url, headers=headers, timeout=30)
            if resp.status_code != 200:
                continue

            # Count target filings in index
            lines = resp.text.strip().split('\n')
            data_start = 0
            for i, line in enumerate(lines):
                if line.startswith('---'):
                    data_start = i + 1
                    break

            expected = 0
            for line in lines[data_start:]:
                if len(line) < 74:
                    continue
                form_type = line[62:74].strip()
                if form_type in TARGET_FORMS:
                    expected += 1

            days_checked += 1
            gap = expected - loaded

            if gap > 0:
                gaps_found.append({
                    'date': feed_date,
                    'expected': expected,
                    'loaded': loaded,
                    'gap': gap,
                    'pct': round(100 * loaded / expected, 1) if expected > 0 else 0
                })
                total_missing += gap

                # Mark as INCOMPLETE
                session.sql(f"""
                    UPDATE {fqn}._FEED_INGEST_LOG
                    SET status = 'INCOMPLETE', updated_at = CURRENT_TIMESTAMP()
                    WHERE feed_date = '{feed_date}' AND status != 'INCOMPLETE'
                """).collect()

                # Auto-fill if requested
                if p_auto_fill:
                    try:
                        session.sql(f"CALL {fqn}.FILL_FEED_GAPS('{feed_date}', '{p_user_agent}')").collect()
                    except Exception:
                        pass

        except Exception:
            continue

    # Summary
    if not gaps_found:
        return f"Audit complete: {days_checked} days checked, no gaps found."

    report = f"Audit complete: {days_checked} days checked, {len(gaps_found)} days with gaps, {total_missing} total missing filings.\n\n"
    report += "Top gaps:\n"
    gaps_sorted = sorted(gaps_found, key=lambda g: g['gap'], reverse=True)
    for g in gaps_sorted[:20]:
        report += f"  {g['date']}: {g['loaded']}/{g['expected']} ({g['pct']}%) — {g['gap']} missing\n"

    if p_auto_fill:
        report += f"\nAuto-fill attempted for {len(gaps_found)} days."

    return report
$$;
