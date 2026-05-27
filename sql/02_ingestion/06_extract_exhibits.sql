-- =============================================================================
-- 05: Extract Exhibits from Filing Content
-- =============================================================================
-- Parses full SEC submissions (TXT format) to extract high-value exhibits
-- into the FILING_EXHIBITS table.
--
-- Exhibit types extracted:
--   - EX-99.* (press releases, earnings announcements, investor letters)
--   - EX-10.* (material contracts, credit agreements, employment contracts)
--   - EX-2.*  (plans of acquisition / M&A agreements)
--
-- Skips: XBRL (EX-101.*), graphics, certifications (EX-31/32), admin (EX-21/23/24)
--
-- Only processes TXT format content (batch downloads contain full submissions
-- with exhibits). Feed format only has the primary document.
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

-- =============================================================================
-- SP: EXTRACT_EXHIBITS
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXTRACT_EXHIBITS(
    P_BATCH_SIZE INT DEFAULT 100
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'extract_exhibits'
EXECUTE AS CALLER
AS $$
import re
import pandas as pd

# Exhibit types worth extracting (hedge fund research value)
TARGET_EXHIBIT_TYPES = {'EX-99', 'EX-10', 'EX-2'}
MAX_EXHIBIT_CHARS = 16_000_000  # 16MB per exhibit

def is_target_exhibit(doc_type):
    """Match EX-99.*, EX-10.N*, EX-2.N* only.
    Excludes EX-101.* (XBRL), EX-21/23/24 (admin)."""
    if not doc_type:
        return False
    dt = doc_type.upper().strip()
    if re.match(r'^EX-99', dt): return True
    if re.match(r'^EX-10\.\d', dt): return True
    if re.match(r'^EX-2\.\d', dt): return True
    return False

def parse_documents(content):
    """Parse SEC submission into individual DOCUMENT blocks."""
    docs = []
    # Split on <DOCUMENT> boundaries
    doc_pattern = re.compile(r'<DOCUMENT>(.*?)</DOCUMENT>', re.DOTALL)
    for i, match in enumerate(doc_pattern.finditer(content)):
        doc_text = match.group(1)

        # Parse document header
        type_m = re.search(r'<TYPE>([^\n<]+)', doc_text)
        seq_m = re.search(r'<SEQUENCE>(\d+)', doc_text)
        fn_m = re.search(r'<FILENAME>([^\n<]+)', doc_text)
        desc_m = re.search(r'<DESCRIPTION>([^\n<]+)', doc_text)

        doc_type = type_m.group(1).strip() if type_m else None
        sequence = int(seq_m.group(1)) if seq_m else i + 1
        filename = fn_m.group(1).strip() if fn_m else None
        description = desc_m.group(1).strip() if desc_m else None

        # Extract content between <TEXT> and </TEXT>
        text_m = re.search(r'<TEXT>(.*?)</TEXT>', doc_text, re.DOTALL)
        text_content = text_m.group(1).strip() if text_m else ''

        docs.append({
            'type': doc_type,
            'sequence': sequence,
            'filename': filename,
            'description': description,
            'content': text_content[:MAX_EXHIBIT_CHARS]
        })

    return docs

def extract_exhibits(session, p_batch_size: int) -> str:
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
    fqn = f"{db}.{schema}"

    # Get batch of TXT filings not yet processed for exhibits
    pending = session.sql(f"""
        SELECT fc.ACCESSION_NO, fc.CONTENT_TEXT
        FROM {fqn}.FILING_CONTENT fc
        WHERE fc.FILE_FORMAT = 'TXT'
          AND fc.CONTENT_TEXT IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM {fqn}.FILING_EXHIBITS fe
              WHERE fe.ACCESSION_NO = fc.ACCESSION_NO
          )
        LIMIT {p_batch_size}
    """).to_pandas()

    if pending.empty:
        return "No pending filings for exhibit extraction"

    exhibit_rows = []
    filings_processed = 0
    filings_with_exhibits = 0

    for _, row in pending.iterrows():
        accession_no = row['ACCESSION_NO']
        content = row['CONTENT_TEXT']
        filings_processed += 1

        docs = parse_documents(content)
        filing_has_exhibits = False

        for doc in docs:
            if doc['sequence'] == 1:
                continue  # Skip primary document (already in FILING_CONTENT)
            if not is_target_exhibit(doc['type']):
                continue
            if not doc['content'] or len(doc['content']) < 100:
                continue  # Skip empty/trivial exhibits

            exhibit_id = f"{accession_no}_{doc['sequence']}"
            exhibit_rows.append({
                'EXHIBIT_ID': exhibit_id,
                'ACCESSION_NO': accession_no,
                'DOC_SEQUENCE': doc['sequence'],
                'EXHIBIT_TYPE': doc['type'],
                'FILENAME': doc['filename'],
                'DESCRIPTION': doc['description'],
                'CONTENT_TEXT': doc['content'],
                'FILE_SIZE_CHARS': len(doc['content'])
            })
            filing_has_exhibits = True

        if filing_has_exhibits:
            filings_with_exhibits += 1

    if not exhibit_rows:
        return f"Processed {filings_processed} filings, no target exhibits found"

    # Bulk insert exhibits
    df = pd.DataFrame(exhibit_rows)
    tmp = f"{fqn}._EXHIBITS_TMP"
    session.create_dataframe(df).write.mode("overwrite").save_as_table(tmp)

    session.sql(f"""
        INSERT INTO {fqn}.FILING_EXHIBITS
            (EXHIBIT_ID, ACCESSION_NO, DOC_SEQUENCE, EXHIBIT_TYPE,
             FILENAME, DESCRIPTION, CONTENT_TEXT, FILE_SIZE_CHARS)
        SELECT t.EXHIBIT_ID, t.ACCESSION_NO, t.DOC_SEQUENCE::INT, t.EXHIBIT_TYPE,
               t.FILENAME, t.DESCRIPTION, t.CONTENT_TEXT, t.FILE_SIZE_CHARS::INT
        FROM {tmp} t
        WHERE NOT EXISTS (
            SELECT 1 FROM {fqn}.FILING_EXHIBITS fe WHERE fe.EXHIBIT_ID = t.EXHIBIT_ID
        )
    """).collect()

    session.sql(f"DROP TABLE IF EXISTS {tmp}").collect()

    type_counts = df['EXHIBIT_TYPE'].value_counts().to_dict()
    summary = ", ".join(f"{v} {k}" for k, v in sorted(type_counts.items()))
    return (f"Extracted {len(exhibit_rows)} exhibits from {filings_with_exhibits}/{filings_processed} filings: {summary}")
$$;


-- =============================================================================
-- EXECUTION
-- =============================================================================
-- CALL EXTRACT_EXHIBITS(100);
-- Repeat until "No pending filings" returned.

-- Verify:
-- SELECT EXHIBIT_TYPE, COUNT(*) AS cnt, ROUND(AVG(FILE_SIZE_CHARS)/1000, 1) AS avg_k
-- FROM FILING_EXHIBITS GROUP BY 1 ORDER BY cnt DESC;
