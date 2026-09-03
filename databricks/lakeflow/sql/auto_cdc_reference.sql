-- ============================================================================
-- Databricks Lakeflow Declarative Pipelines: AUTO CDC Reference Implementation
-- ============================================================================
-- This SQL file demonstrates the declarative SQL equivalent of the Python
-- Lakeflow pipeline using modern AUTO CDC syntax (CREATE FLOW ... AS AUTO CDC).
--
-- Note: The referenced snapshot and CDC source datasets
-- (e.g. stream(accounts_snapshot_source) and stream(accounts_cdc_source))
-- are streaming-compatible temporary views that share identical, normalized schemas.
--
-- Supported constructs:
--  1. Initial hydration via AUTO CDC ONCE (once = true)
--  2. Continuous change data ingestion with DELETE propagation
--  3. SCD Type 1 current-state tables
--  4. SCD Type 2 history tracking with selective column versioning
-- ============================================================================

-- ============================================================================
-- 1. ACCOUNTS (SCD Type 1 Current-State)
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE accounts_current
COMMENT 'Current-state accounts table managed by Lakeflow AUTO CDC'
TBLPROPERTIES ("pipelines.cdc.tombstoneGCThresholdInSeconds" = "604800");

-- Flow 1A: Initial Snapshot Hydration (once=true)
CREATE FLOW accounts_initial_hydration
AS AUTO CDC ONCE INTO accounts_current
FROM stream(accounts_snapshot_source)
KEYS (account_id)
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;

-- Flow 1B: Continuous CDC Flow
CREATE FLOW accounts_continuous_cdc
AS AUTO CDC INTO accounts_current
FROM stream(accounts_cdc_source)
KEYS (account_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;


-- ============================================================================
-- 2. SUBSCRIPTIONS (SCD Type 1 Current-State + SCD Type 2 Historical Audit)
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE subscriptions_current
COMMENT 'Current-state subscriptions table managed by Lakeflow AUTO CDC'
TBLPROPERTIES ("pipelines.cdc.tombstoneGCThresholdInSeconds" = "604800");

-- Flow 2A: Initial Snapshot Hydration (once=true)
CREATE FLOW subscriptions_initial_hydration
AS AUTO CDC ONCE INTO subscriptions_current
FROM stream(subscriptions_snapshot_source)
KEYS (subscription_id)
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;

-- Flow 2B: Continuous CDC Flow
CREATE FLOW subscriptions_continuous_cdc
AS AUTO CDC INTO subscriptions_current
FROM stream(subscriptions_cdc_source)
KEYS (subscription_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;

-- ----------------------------------------------------------------------------
-- SCD Type 2 Target: subscriptions_history
-- ----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE subscriptions_history
COMMENT 'Historical audit SCD Type 2 subscriptions table managed by Lakeflow AUTO CDC'
TBLPROPERTIES ("pipelines.cdc.tombstoneGCThresholdInSeconds" = "604800");

-- Flow 2C: History Initial Snapshot Hydration (once=true)
CREATE FLOW subscriptions_history_initial_hydration
AS AUTO CDC ONCE INTO subscriptions_history
FROM stream(subscriptions_snapshot_source)
KEYS (subscription_id)
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 2
TRACK HISTORY ON (
  account_id,
  plan_name,
  billing_cycle,
  monthly_amount,
  status,
  start_date,
  end_date
);

-- Flow 2D: History Continuous CDC Flow
CREATE FLOW subscriptions_history_continuous_cdc
AS AUTO CDC INTO subscriptions_history
FROM stream(subscriptions_cdc_source)
KEYS (subscription_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 2
TRACK HISTORY ON (
  account_id,
  plan_name,
  billing_cycle,
  monthly_amount,
  status,
  start_date,
  end_date
);


-- ============================================================================
-- 3. INVOICES (SCD Type 1 Current-State)
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE invoices_current
COMMENT 'Current-state invoices table managed by Lakeflow AUTO CDC'
TBLPROPERTIES ("pipelines.cdc.tombstoneGCThresholdInSeconds" = "604800");

-- Flow 3A: Initial Snapshot Hydration (once=true)
CREATE FLOW invoices_initial_hydration
AS AUTO CDC ONCE INTO invoices_current
FROM stream(invoices_snapshot_source)
KEYS (invoice_id)
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;

-- Flow 3B: Continuous CDC Flow
CREATE FLOW invoices_continuous_cdc
AS AUTO CDC INTO invoices_current
FROM stream(invoices_cdc_source)
KEYS (invoice_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;


-- ============================================================================
-- 4. PAYMENTS (SCD Type 1 Current-State)
-- ============================================================================
CREATE OR REFRESH STREAMING TABLE payments_current
COMMENT 'Current-state payments table managed by Lakeflow AUTO CDC'
TBLPROPERTIES ("pipelines.cdc.tombstoneGCThresholdInSeconds" = "604800");

-- Flow 4A: Initial Snapshot Hydration (once=true)
CREATE FLOW payments_initial_hydration
AS AUTO CDC ONCE INTO payments_current
FROM stream(payments_snapshot_source)
KEYS (payment_id)
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;

-- Flow 4B: Continuous CDC Flow
CREATE FLOW payments_continuous_cdc
AS AUTO CDC INTO payments_current
FROM stream(payments_cdc_source)
KEYS (payment_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (operation, sequence_number)
STORED AS SCD TYPE 1;
