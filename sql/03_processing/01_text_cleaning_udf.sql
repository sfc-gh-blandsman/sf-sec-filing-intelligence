-- =============================================================================
-- 01: Text Cleaning UDF
-- =============================================================================
-- Python UDF to strip HTML/XML tags, decode HTML entities, and normalize
-- whitespace from filing content.
--
-- Entity handling strategy:
--   1. Named entities (&amp;, &nbsp;, etc.) → ASCII equivalents
--   2. Numeric entities (&#NNN;): allowlist common ones → ASCII,
--      smart fallback for rest (printable Unicode → char, else space)
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

CREATE OR REPLACE FUNCTION CLEAN_TEXT("RAW_TEXT" VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'clean_text'
AS $$
import re

# Common numeric HTML entities → ASCII-safe equivalents
ENTITY_MAP = {
    160: ' ',    # non-breaking space
    8211: '-',   # en dash
    8212: '-',   # em dash
    8216: "'",   # left single quote
    8217: "'",   # right single quote
    8218: "'",   # single low-9 quote
    8220: '"',   # left double quote
    8221: '"',   # right double quote
    8222: '"',   # double low-9 quote
    8226: '*',   # bullet
    8230: '...', # ellipsis
    9679: '*',   # black circle (bullet)
    9744: '[ ]', # ballot box
    9745: '[x]', # ballot box with check
    9746: '[x]', # ballot box with x
    174: '(R)',  # registered trademark
    169: '(C)',  # copyright
    176: ' degrees ',  # degree sign
    8364: 'EUR', # euro
    163: 'GBP',  # pound
    165: 'JPY',  # yen
    38: '&',     # ampersand
}

def _decode_entity(m):
    code = int(m.group(1))
    if code in ENTITY_MAP:
        return ENTITY_MAP[code]
    if 32 <= code <= 126:
        return chr(code)
    if 126 < code < 65536:
        c = chr(code)
        if c.isprintable():
            return c
    return ' '

def _decode_hex_entity(m):
    code = int(m.group(1), 16)
    if code in ENTITY_MAP:
        return ENTITY_MAP[code]
    if 32 <= code <= 126:
        return chr(code)
    if 126 < code < 65536:
        c = chr(code)
        if c.isprintable():
            return c
    return ' '

def clean_text(raw_text: str) -> str:
    if not raw_text:
        return ''
    # Remove XBRL metadata block (ix:header contains contexts/units/hidden data)
    # This block is 100K-1.8MB of machine-readable metadata invisible in browsers
    text = re.sub(r'<ix:header>.*?</ix:header>', ' ', raw_text, count=1, flags=re.DOTALL|re.IGNORECASE)
    # Strip HTML/XML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Named entities → ASCII
    for entity, char in [('&nbsp;',' '),('&amp;','&'),('&lt;','<'),
                          ('&gt;','>'),('&quot;','"'),('&#39;',"'"),
                          ('&ndash;','-'),('&mdash;','-'),('&bull;','*')]:
        text = text.replace(entity, char)
    # Numeric entities: allowlist + smart fallback
    text = re.sub(r'&#(\d+);', _decode_entity, text)
    text = re.sub(r'&#x([0-9a-fA-F]+);', _decode_hex_entity, text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text
$$;
