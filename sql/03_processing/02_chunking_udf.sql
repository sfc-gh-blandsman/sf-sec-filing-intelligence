-- =============================================================================
-- 02: Chunking UDF
-- =============================================================================
-- Python UDF for section-aware chunking of SEC filings.
-- Identifies sections by form type (10-K, 10-Q, 8-K) and splits into
-- overlapping chunks of configurable size.
--
-- Default: 1500 chars max (~375 tokens), 200 chars overlap.
-- Per Snowflake docs: chunks ≤512 tokens for best retrieval quality.
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

CREATE OR REPLACE FUNCTION CHUNK_FILING(
    "CONTENT_TEXT" VARCHAR,
    "FORM_TYPE" VARCHAR,
    "MAX_CHARS" NUMBER(38,0) DEFAULT 1500,
    "OVERLAP_CHARS" NUMBER(38,0) DEFAULT 200
)
RETURNS ARRAY
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'chunk_filing'
AS '
import re

SECTION_PATTERNS = {
    ''10-K'': [
        (r''item\\s+1[^a-z\\d]'',  ''Business''),
        (r''item\\s+1a[^a-z\\d]'', ''Risk Factors''),
        (r''item\\s+1b[^a-z\\d]'', ''Unresolved Staff Comments''),
        (r''item\\s+2[^a-z\\d]'',  ''Properties''),
        (r''item\\s+3[^a-z\\d]'',  ''Legal Proceedings''),
        (r''item\\s+7[^a-z\\d]'',  ''MD&A''),
        (r''item\\s+7a[^a-z\\d]'', ''Market Risk''),
        (r''item\\s+8[^a-z\\d]'',  ''Financial Statements''),
        (r''item\\s+9a[^a-z\\d]'', ''Controls and Procedures''),
    ],
    ''10-Q'': [
        (r''item\\s+1[^a-z\\d]'',  ''Financial Statements''),
        (r''item\\s+2[^a-z\\d]'',  ''MD&A''),
        (r''item\\s+3[^a-z\\d]'',  ''Market Risk''),
        (r''item\\s+1a[^a-z\\d]'', ''Risk Factors''),
        (r''item\\s+4[^a-z\\d]'',  ''Controls and Procedures''),
    ],
    ''8-K'': [
        (r''item\\s+1\\.01'', ''Material Agreement''),
        (r''item\\s+1\\.02'', ''Termination of Agreement''),
        (r''item\\s+2\\.01'', ''Completion of Acquisition''),
        (r''item\\s+2\\.02'', ''Results of Operations''),
        (r''item\\s+2\\.05'', ''Departure of Officers''),
        (r''item\\s+5\\.02'', ''Director/Officer Changes''),
        (r''item\\s+7\\.01'', ''Regulation FD Disclosure''),
        (r''item\\s+8\\.01'', ''Other Events''),
        (r''item\\s+9\\.01'', ''Financial Statements and Exhibits''),
    ],
}

def find_sections(text, form_type):
    lower = text.lower()
    patterns = SECTION_PATTERNS.get(form_type.split(''/'')[0].upper(), [])
    boundaries = []
    for pattern, label in patterns:
        for m in re.finditer(pattern, lower):
            boundaries.append((m.start(), label))
    boundaries.sort(key=lambda x: x[0])
    deduped = []
    prev_label = None
    for pos, label in boundaries:
        if label != prev_label:
            deduped.append((pos, label))
            prev_label = label
    return deduped

def chunk_filing(content_text, form_type, max_chars, overlap_chars):
    if not content_text:
        return []
    text = content_text
    sections = find_sections(text, form_type)
    chunks = []
    chunk_index = 0
    if not sections:
        start = 0
        while start < len(text):
            end = min(start + max_chars, len(text))
            ct = text[start:end].strip()
            if ct:
                chunks.append({''section_name'': ''Document'', ''chunk_index'': chunk_index, ''chunk_text'': ct})
                chunk_index += 1
            start += max_chars - overlap_chars
        return chunks
    for i, (sec_start, label) in enumerate(sections):
        sec_end = sections[i+1][0] if i+1 < len(sections) else len(text)
        section_text = text[sec_start:sec_end]
        start = 0
        while start < len(section_text):
            end = min(start + max_chars, len(section_text))
            ct = section_text[start:end].strip()
            if ct:
                chunks.append({''section_name'': label, ''chunk_index'': chunk_index, ''chunk_text'': ct})
                chunk_index += 1
            start += max_chars - overlap_chars
    return chunks
';
