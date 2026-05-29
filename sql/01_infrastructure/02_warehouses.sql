-- =============================================================================
-- 02: Warehouses
-- =============================================================================
-- Creates warehouses for steady-state operations and one-time bulk builds.
-- Run 00_config.sql first to set session variables.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Steady-state warehouse: for daily operations, search refresh, agent queries
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($config_warehouse)
    WAREHOUSE_SIZE = 'LARGE'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8
    COMMENT = 'Steady-state warehouse for search refresh, agent queries, and pipeline tasks';

-- Build warehouse: for bulk processing (chunking, signal extraction)
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($config_warehouse_build)
    WAREHOUSE_SIZE = 'X4LARGE'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8
    COMMENT = 'Bulk processing warehouse for initial pipeline load. Suspend after use.';

-- Grant usage to SYSADMIN
GRANT USAGE ON WAREHOUSE IDENTIFIER($config_warehouse) TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE IDENTIFIER($config_warehouse_build) TO ROLE SYSADMIN;

-- Ingest warehouse: Snowpark-optimized for Python SPs with high memory needs
-- Used by LOAD_FEED_ARCHIVE (downloads + decompresses large tar.gz archives)
-- Multi-cluster for parallel monthly feed ingestion DAG (12 concurrent tasks)
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($config_warehouse_ingest)
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
    WAREHOUSE_SIZE = 'MEDIUM'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 4
    SCALING_POLICY = 'ECONOMY'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Snowpark-optimized warehouse for feed archive ingestion (multi-cluster for parallel tasks)';

GRANT USAGE ON WAREHOUSE IDENTIFIER($config_warehouse_ingest) TO ROLE SYSADMIN;

-- Compute pool for Streamlit container runtime
CREATE COMPUTE POOL IF NOT EXISTS STREAMLIT_COMPUTE_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_SUSPEND_SECS = 3600
    AUTO_RESUME = TRUE
    COMMENT = 'Compute pool for SEC Filing Intelligence Streamlit dashboard (container runtime)';
