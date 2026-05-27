-- =============================================================================
-- 02: Semantic View
-- =============================================================================
-- Creates the semantic view for Cortex Analyst — enables aggregate analytics
-- over the filing signals corpus.
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);
USE WAREHOUSE IDENTIFIER($config_warehouse);

CREATE OR REPLACE SEMANTIC VIEW IDENTIFIER($config_semantic_view)
  TABLES (
    signals AS FILING_SIGNALS
      PRIMARY KEY (SIGNAL_ID)
      WITH SYNONYMS = ('investment signals', 'filing signals', 'EDGAR signals', 'SEC filings')
      COMMENT = 'AI-extracted investment signals from SEC EDGAR filings. Each row represents one filing with structured event, sentiment, and metrics data.',

    meta AS FILING_INDEX
      PRIMARY KEY (ACCESSION_NO)
      WITH SYNONYMS = ('filing metadata', 'EDGAR index', 'filing registry')
      COMMENT = 'SEC EDGAR filing metadata: accession numbers, CIKs, filing URLs, dates'
  )
  RELATIONSHIPS (
    signals_to_meta AS signals(ACCESSION_NO) REFERENCES meta(ACCESSION_NO)
  )
  FACTS (
    signals.accession_no AS signals.ACCESSION_NO
      WITH SYNONYMS = ('accession number', 'filing id', 'SEC filing identifier')
      COMMENT = 'EDGAR accession number uniquely identifying the filing',

    signals.revenue AS signals.REVENUE_NORMALIZED
      WITH SYNONYMS = ('total revenue', 'net revenue', 'sales', 'top line', 'revenue')
      COMMENT = 'Revenue in millions USD (FLOAT). Normalized from filing-stated units (billions, millions, thousands, or raw dollars). NULL if not extractable.',

    signals.net_income AS signals.NET_INCOME
      WITH SYNONYMS = ('net income', 'net loss', 'profit', 'bottom line', 'net earnings')
      COMMENT = 'Net income or loss figure extracted from filing. Text format. NULL if not found.',

    signals.eps AS signals.EPS_NORMALIZED
      WITH SYNONYMS = ('earnings per share', 'diluted EPS', 'EPS', 'per share')
      COMMENT = 'Normalized single EPS value (e.g., "$2.39"). NULL for multi-class/multi-period structures. Text format.',

    signals.yoy_change AS signals.YOY_CHANGE
      WITH SYNONYMS = ('year over year', 'YoY growth', 'growth rate', 'revenue growth')
      COMMENT = 'Year-over-year change percentage. Text format (e.g., "15%"). NULL if not found.',

    signals.guidance_normalized AS signals.GUIDANCE_NORMALIZED
      WITH SYNONYMS = ('guidance', 'outlook', 'forecast', 'forward looking', 'expected')
      COMMENT = 'Forward-looking financial guidance from MD&A. Text format. NULL if not found or not stated.'
  )
  DIMENSIONS (
    signals.company_name AS signals.COMPANY_NAME
      WITH SYNONYMS = ('company', 'filer', 'issuer', 'corporation', 'firm')
      COMMENT = 'Name of the company that filed the SEC document',

    signals.ticker AS signals.TICKER
      WITH SYNONYMS = ('stock ticker', 'symbol', 'stock symbol', 'equity ticker')
      COMMENT = 'Stock ticker symbol. May be NULL for non-public filers.',

    signals.form_type AS signals.FORM_TYPE
      WITH SYNONYMS = ('filing type', 'document type', 'SEC form', 'form')
      COMMENT = 'Type of SEC filing: 10-K (annual report), 10-Q (quarterly report), 8-K (current report / material event)',

    signals.event_type AS COALESCE(signals.EVENT_TYPE_NORMALIZED, signals.EVENT_TYPE)
      WITH SYNONYMS = ('event', 'event classification', 'filing event', 'signal type')
      COMMENT = 'AI-classified event type (normalized to 12 categories): Earnings, M&A, Leadership Change, Risk Disclosure, Guidance Update, Regulatory, Capital Markets, Bankruptcy, Annual Report, Quarterly Report, Current Report, Other.',

    signals.sentiment AS signals.SENTIMENT
      WITH SYNONYMS = ('tone', 'filing tone', 'document sentiment', 'market sentiment')
      COMMENT = 'AI-assessed overall sentiment: POSITIVE, NEGATIVE, NEUTRAL, MIXED.',

    signals.industry_sector AS signals.INDUSTRY_SECTOR
      WITH SYNONYMS = ('sector', 'industry sector', 'business sector', 'industry', 'GICS sector', 'Healthcare', 'healthcare')
      COMMENT = 'SEC Office-based industry sector. Values: Technology, Life Sciences, Finance, Real Estate & Construction, Energy & Transportation, Manufacturing, Trade & Services, Crypto Assets, Other. Note: Healthcare maps to Life Sciences, Financial Services maps to Finance.',

    signals.industry_title AS signals.INDUSTRY_TITLE
      WITH SYNONYMS = ('specific industry', 'SIC description', 'industry name', 'sub-sector', 'sub-industry')
      COMMENT = 'Specific SEC industry title (~444 values). Use INDUSTRY_SECTOR for broad filtering, INDUSTRY_TITLE for drill-down.',

    signals.is_amendment AS signals.IS_AMENDMENT
      WITH SYNONYMS = ('amendment', 'amended filing', 'restated', 'revision')
      COMMENT = 'TRUE if this is an amended filing (e.g., 10-K/A, 10-Q/A, 8-K/A)',

    meta.cik AS meta.CIK
      WITH SYNONYMS = ('SEC CIK', 'central index key', 'SEC entity ID')
      COMMENT = 'SEC Central Index Key — unique identifier for the filing entity',

    signals.signal_date AS signals.SIGNAL_DATE
      WITH SYNONYMS = ('filing date', 'date filed', 'signal timestamp', 'submission date',
                       'when filed', 'EDGAR receipt date')
      COMMENT = 'The date the SEC received the filing. This is the investment signal date. Use for time-based filtering unless user explicitly asks about the fiscal period.',

    signals.period_of_report AS signals.PERIOD_OF_REPORT
      WITH SYNONYMS = ('fiscal period', 'report period', 'period end', 'fiscal year end',
                       'quarter end', 'coverage period')
      COMMENT = 'Fiscal period end date the filing covers. A 10-K with signal_date=2023-02-15 may have period_of_report=2022-12-31.'
  )
  METRICS (
    signals.filing_count AS COUNT(signals.SIGNAL_ID)
      WITH SYNONYMS = ('number of filings', 'count of signals', 'total filings', 'how many filings')
      COMMENT = 'Total number of filings matching the selected filters',

    signals.positive_signals AS COUNT(CASE WHEN signals.SENTIMENT = 'POSITIVE' THEN 1 END)
      WITH SYNONYMS = ('positive filings', 'bullish signals', 'favorable filings')
      COMMENT = 'Count of filings with positive sentiment',

    signals.negative_signals AS COUNT(CASE WHEN signals.SENTIMENT = 'NEGATIVE' THEN 1 END)
      WITH SYNONYMS = ('negative filings', 'bearish signals', 'adverse filings')
      COMMENT = 'Count of filings with negative sentiment',

    signals.neutral_signals AS COUNT(CASE WHEN signals.SENTIMENT = 'NEUTRAL' THEN 1 END)
      WITH SYNONYMS = ('neutral filings', 'balanced filings')
      COMMENT = 'Count of filings with neutral sentiment',

    signals.earnings_count AS COUNT(CASE WHEN signals.EVENT_TYPE = 'Earnings' THEN 1 END)
      WITH SYNONYMS = ('earnings releases', 'earnings events', 'earnings reports')
      COMMENT = 'Count of earnings-related filing events',

    signals.ma_count AS COUNT(CASE WHEN signals.EVENT_TYPE = 'M&A' THEN 1 END)
      WITH SYNONYMS = ('merger filings', 'acquisition events', 'M&A events', 'deals')
      COMMENT = 'Count of merger and acquisition events',

    signals.risk_disclosure_count AS COUNT(CASE WHEN signals.EVENT_TYPE = 'Risk Disclosure' THEN 1 END)
      WITH SYNONYMS = ('risk disclosures', 'risk events', 'risk warnings')
      COMMENT = 'Count of risk disclosure events',

    signals.leadership_change_count AS COUNT(CASE WHEN signals.EVENT_TYPE = 'Leadership Change' THEN 1 END)
      WITH SYNONYMS = ('leadership events', 'management changes', 'executive changes')
      COMMENT = 'Count of leadership change events',

    signals.guidance_count AS COUNT(CASE WHEN signals.EVENT_TYPE = 'Guidance Update' THEN 1 END)
      WITH SYNONYMS = ('guidance updates', 'forward guidance', 'outlook updates')
      COMMENT = 'Count of guidance update events',

    signals.amendment_count AS COUNT(CASE WHEN signals.IS_AMENDMENT = TRUE THEN 1 END)
      WITH SYNONYMS = ('amended filings', 'restatements', 'corrections')
      COMMENT = 'Count of amended filings',

    signals.negative_sentiment_pct AS
      ROUND(100.0 * COUNT(CASE WHEN signals.SENTIMENT = 'NEGATIVE' THEN 1 END)
            / NULLIF(COUNT(signals.SIGNAL_ID), 0), 2)
      WITH SYNONYMS = ('negative rate', 'percent negative', 'bearish rate')
      COMMENT = 'Percentage of filings with negative sentiment (0-100 scale)'
  )
  COMMENT = 'Investment signal analytics over SEC EDGAR filing corpus. Use SIGNAL_DATE for filing date filters; PERIOD_OF_REPORT for fiscal period filters.'
  AI_SQL_GENERATION 'This semantic view covers SEC EDGAR filings. SIGNAL_DATE is the authoritative filing timestamp — use for date filters unless user asks about fiscal period. PERIOD_OF_REPORT is fiscal end date. EVENT_TYPE and SENTIMENT are AI-extracted and may be NULL — account for NULLs in aggregations. For time-series analysis, DATE_TRUNC on SIGNAL_DATE is recommended.';
