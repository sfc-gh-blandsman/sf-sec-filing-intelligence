-- =============================================================================
-- 04: Email Notification Integration
-- =============================================================================
-- Creates the email notification integration used by task DAG finalizers
-- to send completion/failure notifications.
--
-- Prerequisites:
--   - Recipient user must have IS_EMAIL_VERIFIED = true
--   - Verify via: DESCRIBE USER <username> (check IS_EMAIL_VERIFIED)
--   - If not verified: user must verify in Snowsight profile settings
--
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE IDENTIFIER($config_database);
USE SCHEMA IDENTIFIER($config_schema);

CREATE OR REPLACE NOTIFICATION INTEGRATION IDENTIFIER($config_email_integration)
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ($config_email_recipient)
    COMMENT = 'Email notifications for SEC Filing Intelligence pipeline tasks';
