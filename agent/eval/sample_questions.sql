-- =============================================================================
-- Sample Evaluation Questions (20 questions for 2025 SEC filing data)
-- =============================================================================
-- These are the questions loaded into SEC_FILING_EVAL_DATASET.
-- Use to manually test the agent before running the automated eval DAG.
--
-- Industry sectors: Technology, Life Sciences, Finance, Real Estate & Construction,
--   Energy & Transportation, Manufacturing, Trade & Services, Crypto Assets, Other
-- Note: "Healthcare" = "Life Sciences", "Financial Services" = "Finance"
-- =============================================================================

-- Search-type questions (qualitative / narrative answers)
-- 1. What risk factors did pharmaceutical companies disclose in 2025 10-K filings?
-- 3. What did banks discuss about credit losses in their 2025 10-K filings?
-- 5. Find 8-K filings from Technology companies reporting leadership changes.
-- 7. Quote the language about cybersecurity risks in a recent 10-K filing.
-- 9. Find 10-K Risk Factors discussing climate-related risks in Energy & Transportation filings.
-- 11. What did manufacturers discuss about supply chain resilience in their 10-K MD&A section?
-- 13. Find a Real Estate & Construction company filing discussing interest rate impact.
-- 15. Find Life Sciences 8-K filings about acquisitions.
-- 17. What common risk factors appeared across multiple 2025 10-K filings?
-- 19. Find filings discussing artificial intelligence risks or opportunities.

-- Analyst-type questions (structured / numeric answers)
-- 2. How many 10-K filings had negative sentiment in Q1 2025?
-- 4. Show the monthly trend of M&A events in 2025.
-- 6. What is the negative sentiment rate by industry sector?
-- 8. How many filings were submitted by Finance companies in Q1 2025?
-- 10. Compare the number of positive vs negative filings across form types.
-- 12. Which companies had the most 8-K filings in 2025?
-- 14. What is the breakdown of event types for 8-K filings?
-- 16. How many guidance update events occurred in each month of Q1 2025?
-- 18. Show the total filing count by form type.
-- 20. What percentage of amended filings have negative sentiment?

-- =============================================================================
-- Manual agent testing (run these one at a time in a Snowsight worksheet):
-- =============================================================================
-- Paste 00_config.sql first, then:

-- SELECT SNOWFLAKE.CORTEX.AGENT(
--     $config_database || '.' || $config_schema || '.' || $config_agent_name,
--     'What risk factors did pharmaceutical companies disclose in 2025 10-K filings?'
-- );

-- SELECT SNOWFLAKE.CORTEX.AGENT(
--     $config_database || '.' || $config_schema || '.' || $config_agent_name,
--     'How many 10-K filings had negative sentiment in Q1 2025?'
-- );

-- SELECT SNOWFLAKE.CORTEX.AGENT(
--     $config_database || '.' || $config_schema || '.' || $config_agent_name,
--     'What is the negative sentiment rate by industry sector?'
-- );
