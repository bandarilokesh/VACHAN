"""
Layer 1: Data Product Factory - Complete Pipeline
===================================================
Bronze  → Ingest all 31 raw datasets into DuckDB
Silver  → Clean, standardize, decode lookups
Identity Resolution → Deterministic + graph-based clustering → golden_id
Gold    → Customer 360 view with survivorship rules
Quality → Data quality checks & report generation
"""

import os
import sys
import time
import json
import hashlib
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

# ─── Configuration ──────────────────────────────────────────────────────
DATA_ROOT = Path("d:/VACHAN/maple_data/maple_collections_release/maple_collections_release")
DATA_DIR  = DATA_ROOT / "data"
DB_PATH   = Path("d:/VACHAN/collections_c360/maple.duckdb")

# Ensure output directory exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


# ═══════════════════════════════════════════════════════════════════════
#  PHASE 1: BRONZE — Raw Ingestion
# ═══════════════════════════════════════════════════════════════════════

def load_bronze(con: duckdb.DuckDBPyConnection):
    """Load all 31 raw datasets into DuckDB as Bronze views/tables."""
    print("=" * 70)
    print("  PHASE 1: BRONZE — Raw Ingestion")
    print("=" * 70)

    # CSV tables
    csv_tables = [
        "customers", "card_accounts", "loan_accounts", "deposit_accounts",
        "collections_cases", "contact_history", "promises_to_pay",
        "agent_notes", "call_transcripts", "voice_samples", "external",
        "model_scores", "offers", "metric_definitions", "benchmark_questions",
        "reference_documents", "hardship_programs", "qa_checklist",
        "agents", "case_assignment_history", "agent_shifts",
        "channel_capacity", "ops_events", "geo_reference", "code_lookups"
    ]

    # Parquet tables (Hive-partitioned by year)
    parquet_tables = [
        "loan_instalments", "transactions", "card_statements",
        "account_monthly_snapshot", "bureau_history", "salary_credit_history"
    ]

    existing_tables = set(r[0] for r in con.execute("SHOW TABLES").fetchall())
    bronze_needed_csv = [t for t in csv_tables if f"bronze_{t}" not in existing_tables]
    bronze_needed_parquet = [t for t in parquet_tables if f"bronze_{t}" not in existing_tables]

    if not bronze_needed_csv and not bronze_needed_parquet:
        print("  ✓ All 31 bronze views already exist in DuckDB. Using cached views.")
        return 31

    loaded = 0
    for table in csv_tables:
        csv_path = str(DATA_DIR / f"{table}.csv").replace("\\", "/")
        sql = f"""
            CREATE OR REPLACE VIEW bronze_{table} AS
            SELECT *, '{table}' AS _source, current_timestamp AS _loaded_at
            FROM read_csv('{csv_path}',
                          header = true,
                          sample_size = -1,
                          null_padding = true)
        """
        con.execute(sql)
        count = con.execute(f"SELECT count(*) FROM bronze_{table}").fetchone()[0]
        print(f"  ✓ bronze_{table}: {count:>12,} rows")
        loaded += 1

    for table in parquet_tables:
        parquet_path = str(DATA_DIR / table / "*" / "*.parquet").replace("\\", "/")
        sql = f"""
            CREATE OR REPLACE VIEW bronze_{table} AS
            SELECT * EXCLUDE (year), '{table}' AS _source, current_timestamp AS _loaded_at
            FROM read_parquet('{parquet_path}', hive_partitioning = true)
        """
        con.execute(sql)
        count = con.execute(f"SELECT count(*) FROM bronze_{table}").fetchone()[0]
        print(f"  ✓ bronze_{table}: {count:>12,} rows")
        loaded += 1

    print(f"\n  Bronze layer complete: {loaded} datasets ingested.\n")
    return loaded


# ═══════════════════════════════════════════════════════════════════════
#  PHASE 2: SILVER — Cleansing & Standardization
# ═══════════════════════════════════════════════════════════════════════

def build_silver(con: duckdb.DuckDBPyConnection):
    """Clean and standardize key tables for identity resolution."""
    print("=" * 70)
    print("  PHASE 2: SILVER — Cleansing & Standardization")
    print("=" * 70)

    # ── Silver Customers ───────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_customers AS
        SELECT
            crm_customer_id,
            customer_uuid,
            LOWER(TRIM(first_name))                     AS first_name_clean,
            LOWER(TRIM(last_name))                      AS last_name_clean,
            LOWER(TRIM(COALESCE(preferred_name, '')))   AS preferred_name_clean,
            CAST(date_of_birth AS DATE)                 AS dob,
            LOWER(TRIM(email))                          AS email_clean,
            -- Standardize phone: strip non-digits, keep last 10
            RIGHT(regexp_replace(COALESCE(CAST(primary_phone_e164 AS VARCHAR), CAST(primary_phone_raw AS VARCHAR), ''), '[^0-9]', '', 'g'), 10)
                                                        AS phone_10digit,
            UPPER(TRIM(REPLACE(COALESCE(CAST(postal_code AS VARCHAR), CAST(postal_code_raw AS VARCHAR), ''), ' ', '')))
                                                        AS postal_code_clean,
            UPPER(TRIM(fsa))                            AS fsa_clean,
            province_code,
            city,
            -- Consent: preserve original booleans
            consent_call,
            consent_sms,
            consent_email,
            consent_push,
            consent_letter,
            consent_call_mobile,
            consent_autodialer,
            cease_communication_flag,
            national_dncl_flag,
            internal_do_not_call_flag,
            -- Risk & vulnerability
            vulnerability_flag,
            vulnerability_type,
            deceased_flag,
            bankruptcy_flag,
            consumer_proposal_flag,
            fraud_victim_flag,
            -- Demographics (NOT for decisioning, for fairness audit only)
            gender_code,
            marital_status,
            -- Relationship
            customer_since_date,
            tenure_months,
            segment,
            customer_status,
            -- Identity Resolution metadata
            duplicate_of_crm_id,
            national_id_hash,
            -- Lineage
            record_source,
            src_system,
            batch_id,
            extract_ts,
            record_hash
        FROM bronze_customers
        WHERE is_deleted = false OR is_deleted IS NULL
    """)
    n = con.execute("SELECT count(*) FROM silver_customers").fetchone()[0]
    print(f"  ✓ silver_customers: {n:>12,} rows")

    # ── Silver Card Accounts ───────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_card_accounts AS
        SELECT
            card_account_id,
            src_customer_ref,
            LOWER(TRIM(cardholder_name_raw))                AS cardholder_name_clean,
            cardholder_dob_raw,
            RIGHT(regexp_replace(COALESCE(CAST(cardholder_phone_raw AS VARCHAR), ''), '[^0-9]', '', 'g'), 10)
                                                            AS cardholder_phone_10digit,
            UPPER(TRIM(REPLACE(COALESCE(CAST(cardholder_postal_raw AS VARCHAR), ''), ' ', '')))
                                                            AS cardholder_postal_clean,
            product_code,
            card_network,
            card_tier,
            open_date,
            account_status,
            credit_limit,
            current_balance,
            utilisation,
            past_due_amount,
            dpd,
            dpd_bucket,
            chargeoff_flag,
            chargeoff_date,
            chargeoff_amount,
            hardship_program_flag,
            hardship_program_type,
            src_system,
            batch_id,
            extract_ts,
            record_hash
        FROM bronze_card_accounts
    """)
    n = con.execute("SELECT count(*) FROM silver_card_accounts").fetchone()[0]
    print(f"  ✓ silver_card_accounts: {n:>12,} rows")

    # ── Silver Loan Accounts ──────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_loan_accounts AS
        SELECT
            loan_id,
            src_customer_ref,
            LOWER(TRIM(borrower_name_raw))                  AS borrower_name_clean,
            borrower_dob_raw,
            RIGHT(regexp_replace(COALESCE(CAST(borrower_phone_raw AS VARCHAR), ''), '[^0-9]', '', 'g'), 10)
                                                            AS borrower_phone_10digit,
            UPPER(TRIM(REPLACE(COALESCE(CAST(borrower_postal_raw AS VARCHAR), ''), ' ', '')))
                                                            AS borrower_postal_clean,
            product_type,
            product_code,
            origination_date,
            principal_balance,
            total_outstanding,
            past_due_amount,
            dpd,
            dpd_bucket,
            chargeoff_flag,
            chargeoff_date,
            chargeoff_amount,
            hardship_program_flag,
            hardship_program_type,
            src_system,
            batch_id,
            extract_ts,
            record_hash
        FROM bronze_loan_accounts
    """)
    n = con.execute("SELECT count(*) FROM silver_loan_accounts").fetchone()[0]
    print(f"  ✓ silver_loan_accounts: {n:>12,} rows")

    # ── Silver Deposit Accounts ───────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_deposit_accounts AS
        SELECT
            deposit_account_id,
            src_customer_ref,
            LOWER(TRIM(holder_name_raw))                    AS holder_name_clean,
            holder_dob_raw,
            RIGHT(regexp_replace(COALESCE(CAST(holder_phone_raw AS VARCHAR), ''), '[^0-9]', '', 'g'), 10)
                                                            AS holder_phone_10digit,
            UPPER(TRIM(REPLACE(COALESCE(CAST(holder_postal_raw AS VARCHAR), ''), ' ', '')))
                                                            AS holder_postal_clean,
            product_type,
            product_code,
            open_date,
            account_status,
            current_balance,
            available_balance,
            overdraft_limit,
            overdraft_used,
            payroll_deposit_flag,
            last_payroll_date,
            last_payroll_amount,
            payroll_avg_amount_3m,
            ei_deposit_flag,
            cpp_oas_deposit_flag,
            garnishment_flag,
            overdraft_dpd,
            overdraft_dpd_bucket,
            src_system,
            batch_id,
            extract_ts,
            record_hash
        FROM bronze_deposit_accounts
    """)
    n = con.execute("SELECT count(*) FROM silver_deposit_accounts").fetchone()[0]
    print(f"  ✓ silver_deposit_accounts: {n:>12,} rows")

    # ── Silver Collections Cases ──────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_collections_cases AS
        SELECT
            case_id,
            coll_customer_ref,
            case_open_date,
            case_open_reason,
            case_status,
            primary_account_id,
            primary_product,
            accounts_in_case_count,
            entry_dpd,
            current_dpd,
            max_dpd_episode,
            current_bucket,
            total_overdue,
            total_exposure,
            assigned_agent_id,
            strategy_code,
            current_treatment,
            contact_attempts_total,
            rpc_count,
            promises_total,
            promises_kept,
            promises_broken,
            cure_flag,
            cure_date,
            days_to_cure,
            recidivism_flag,
            hardship_flag,
            hardship_reason,
            vulnerable_customer_flag,
            cease_contact_flag,
            chargeoff_flag,
            chargeoff_date,
            province_code,
            days_in_collections,
            outcome,
            outcome_date,
            src_system,
            batch_id,
            extract_ts,
            record_hash
        FROM bronze_collections_cases
    """)
    n = con.execute("SELECT count(*) FROM silver_collections_cases").fetchone()[0]
    print(f"  ✓ silver_collections_cases: {n:>12,} rows")

    print(f"\n  Silver layer complete.\n")


# ═══════════════════════════════════════════════════════════════════════
#  PHASE 3: IDENTITY RESOLUTION
# ═══════════════════════════════════════════════════════════════════════

def resolve_identities(con: duckdb.DuckDBPyConnection):
    """
    Link customer records across CRM, Cards, Loans, Deposits, and External
    systems into a single golden_id using deterministic matching.

    Customer Keys (per README):
      CRM:        customers.crm_customer_id
      Cards:      card_accounts.src_customer_ref
      Loans:      loan_accounts.src_customer_ref
      Deposits:   deposit_accounts.src_customer_ref
      External:   external.bank_subject_ref
      Collections: collections_cases.coll_customer_ref (= CRM ID)
    """
    print("=" * 70)
    print("  PHASE 3: IDENTITY RESOLUTION")
    print("=" * 70)

    # Step 1: Collect all CRM records and their self-declared duplicates
    print("  → Step 1: Building CRM duplicate clusters ...")
    con.execute("""
        CREATE OR REPLACE TABLE ir_crm_clusters AS
        WITH RECURSIVE cluster_walk AS (
            -- Seed: every CRM record points to itself
            SELECT crm_customer_id AS member_id,
                   crm_customer_id AS root_id
            FROM silver_customers

            UNION

            -- Walk: follow duplicate_of_crm_id links
            SELECT c.crm_customer_id AS member_id,
                   cw.root_id
            FROM silver_customers c
            JOIN cluster_walk cw ON c.duplicate_of_crm_id = cw.member_id
            WHERE c.duplicate_of_crm_id IS NOT NULL
              AND c.duplicate_of_crm_id != ''
        )
        SELECT member_id AS crm_customer_id,
               FIRST_VALUE(root_id) OVER (
                   PARTITION BY member_id ORDER BY root_id
               ) AS crm_root_id
        FROM cluster_walk
        QUALIFY ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY root_id) = 1
    """)
    n = con.execute("SELECT count(*) FROM ir_crm_clusters").fetchone()[0]
    print(f"    ✓ ir_crm_clusters: {n:>10,} CRM records clustered")

    # Step 2: Link Card accounts → CRM via name + DOB + phone/postal matching
    print("  → Step 2: Linking Card accounts to CRM records ...")
    con.execute("""
        CREATE OR REPLACE TABLE ir_card_to_crm AS
        SELECT DISTINCT
            ca.src_customer_ref       AS card_customer_ref,
            cl.crm_root_id            AS crm_root_id,
            'card_deterministic'      AS match_method
        FROM silver_card_accounts ca
        JOIN silver_customers c
          ON ca.cardholder_phone_10digit = c.phone_10digit
         AND ca.cardholder_postal_clean  = c.postal_code_clean
        JOIN ir_crm_clusters cl ON c.crm_customer_id = cl.crm_customer_id
        WHERE ca.cardholder_phone_10digit != ''
          AND ca.cardholder_postal_clean  != ''
    """)
    n = con.execute("SELECT count(*) FROM ir_card_to_crm").fetchone()[0]
    print(f"    ✓ ir_card_to_crm: {n:>10,} card refs linked")

    # Step 3: Link Loan accounts → CRM
    print("  → Step 3: Linking Loan accounts to CRM records ...")
    con.execute("""
        CREATE OR REPLACE TABLE ir_loan_to_crm AS
        SELECT DISTINCT
            la.src_customer_ref       AS loan_customer_ref,
            cl.crm_root_id            AS crm_root_id,
            'loan_deterministic'      AS match_method
        FROM silver_loan_accounts la
        JOIN silver_customers c
          ON la.borrower_phone_10digit = c.phone_10digit
         AND la.borrower_postal_clean  = c.postal_code_clean
        JOIN ir_crm_clusters cl ON c.crm_customer_id = cl.crm_customer_id
        WHERE la.borrower_phone_10digit != ''
          AND la.borrower_postal_clean  != ''
    """)
    n = con.execute("SELECT count(*) FROM ir_loan_to_crm").fetchone()[0]
    print(f"    ✓ ir_loan_to_crm: {n:>10,} loan refs linked")

    # Step 4: Link Deposit accounts → CRM via CIF (10-digit src_customer_ref)
    print("  → Step 4: Linking Deposit accounts to CRM records ...")
    con.execute("""
        CREATE OR REPLACE TABLE ir_deposit_to_crm AS
        SELECT DISTINCT
            da.src_customer_ref        AS deposit_customer_ref,
            cl.crm_root_id             AS crm_root_id,
            'deposit_deterministic'    AS match_method
        FROM silver_deposit_accounts da
        JOIN silver_customers c
          ON da.holder_phone_10digit = c.phone_10digit
         AND da.holder_postal_clean  = c.postal_code_clean
        JOIN ir_crm_clusters cl ON c.crm_customer_id = cl.crm_customer_id
        WHERE da.holder_phone_10digit != ''
          AND da.holder_postal_clean  != ''
    """)
    n = con.execute("SELECT count(*) FROM ir_deposit_to_crm").fetchone()[0]
    print(f"    ✓ ir_deposit_to_crm: {n:>10,} deposit refs linked")

    # Step 5: Build unified identity map → golden_id
    print("  → Step 5: Building unified golden identity map ...")
    con.execute("""
        CREATE OR REPLACE TABLE gold_identity_map AS
        WITH all_refs AS (
            -- CRM roots
            SELECT crm_root_id, crm_customer_id, NULL AS card_ref, NULL AS loan_ref, NULL AS deposit_ref
            FROM ir_crm_clusters

            UNION ALL

            -- Card refs mapped to CRM root
            SELECT crm_root_id, NULL, card_customer_ref, NULL, NULL
            FROM ir_card_to_crm

            UNION ALL

            -- Loan refs mapped to CRM root
            SELECT crm_root_id, NULL, NULL, loan_customer_ref, NULL
            FROM ir_loan_to_crm

            UNION ALL

            -- Deposit refs mapped to CRM root
            SELECT crm_root_id, NULL, NULL, NULL, deposit_customer_ref
            FROM ir_deposit_to_crm
        )
        SELECT
            'G-' || LPAD(CAST(DENSE_RANK() OVER (ORDER BY crm_root_id) AS VARCHAR), 7, '0') AS golden_id,
            crm_root_id,
            crm_customer_id,
            card_ref,
            loan_ref,
            deposit_ref
        FROM all_refs
    """)
    n_golden = con.execute("SELECT count(DISTINCT golden_id) FROM gold_identity_map").fetchone()[0]
    n_records = con.execute("SELECT count(*) FROM gold_identity_map").fetchone()[0]
    print(f"    ✓ gold_identity_map: {n_records:>10,} records → {n_golden:>10,} unique golden customers")

    print(f"\n  Identity Resolution complete.\n")
    return n_golden


# ═══════════════════════════════════════════════════════════════════════
#  PHASE 4: GOLD — Customer 360 View
# ═══════════════════════════════════════════════════════════════════════

def build_gold_c360(con: duckdb.DuckDBPyConnection):
    """Build the Golden Customer 360 table with survivorship rules."""
    print("=" * 70)
    print("  PHASE 4: GOLD — Customer 360 View")
    print("=" * 70)

    # Gold C360 Customer - Survivorship: latest profile, most conservative consent
    con.execute("""
        CREATE OR REPLACE TABLE gold_c360_customer AS
        WITH ranked AS (
            SELECT
                gim.golden_id,
                c.*,
                ROW_NUMBER() OVER (
                    PARTITION BY gim.golden_id
                    ORDER BY c.extract_ts DESC, c.crm_customer_id
                ) AS rn
            FROM gold_identity_map gim
            JOIN silver_customers c ON gim.crm_customer_id = c.crm_customer_id
            WHERE gim.crm_customer_id IS NOT NULL
        ),
        -- Get most conservative consent per golden_id (AND logic)
        consent_agg AS (
            SELECT
                gim.golden_id,
                BOOL_AND(c.consent_call)           AS consent_call_safe,
                BOOL_AND(c.consent_sms)            AS consent_sms_safe,
                BOOL_AND(c.consent_email)          AS consent_email_safe,
                BOOL_AND(c.consent_push)           AS consent_push_safe,
                BOOL_AND(c.consent_letter)         AS consent_letter_safe,
                BOOL_OR(c.cease_communication_flag)   AS cease_communication_any,
                BOOL_OR(c.national_dncl_flag)         AS dncl_any,
                BOOL_OR(c.internal_do_not_call_flag)  AS internal_dnc_any,
                BOOL_OR(c.vulnerability_flag)         AS vulnerability_any,
                BOOL_OR(c.deceased_flag)              AS deceased_any,
                BOOL_OR(c.bankruptcy_flag)            AS bankruptcy_any,
                BOOL_OR(c.consumer_proposal_flag)     AS consumer_proposal_any,
                BOOL_OR(c.fraud_victim_flag)          AS fraud_victim_any,
                COUNT(DISTINCT c.crm_customer_id)     AS crm_record_count
            FROM gold_identity_map gim
            JOIN silver_customers c ON gim.crm_customer_id = c.crm_customer_id
            WHERE gim.crm_customer_id IS NOT NULL
            GROUP BY gim.golden_id
        )
        SELECT
            r.golden_id,
            r.crm_customer_id        AS primary_crm_id,
            r.first_name_clean       AS first_name,
            r.last_name_clean        AS last_name,
            r.dob,
            r.email_clean            AS email,
            r.phone_10digit          AS phone,
            r.postal_code_clean      AS postal_code,
            r.fsa_clean              AS fsa,
            r.province_code,
            r.city,
            r.segment,
            r.customer_status,
            r.customer_since_date,
            r.tenure_months,
            -- Conservative consent
            ca.consent_call_safe     AS consent_call,
            ca.consent_sms_safe      AS consent_sms,
            ca.consent_email_safe    AS consent_email,
            ca.consent_push_safe     AS consent_push,
            ca.consent_letter_safe   AS consent_letter,
            ca.cease_communication_any AS cease_communication,
            ca.dncl_any              AS national_dncl,
            ca.internal_dnc_any      AS internal_do_not_call,
            -- Flags (OR logic: if ANY record is flagged, the person is flagged)
            ca.vulnerability_any     AS vulnerability_flag,
            ca.deceased_any          AS deceased_flag,
            ca.bankruptcy_any        AS bankruptcy_flag,
            ca.consumer_proposal_any AS consumer_proposal_flag,
            ca.fraud_victim_any      AS fraud_victim_flag,
            ca.crm_record_count
        FROM ranked r
        JOIN consent_agg ca ON r.golden_id = ca.golden_id
        WHERE r.rn = 1
    """)
    n = con.execute("SELECT count(*) FROM gold_c360_customer").fetchone()[0]
    print(f"  ✓ gold_c360_customer: {n:>12,} golden customer records")

    # Gold C360 Accounts — all accounts linked to golden_id
    con.execute("""
        CREATE OR REPLACE TABLE gold_c360_accounts AS
        WITH card_accts AS (
            SELECT
                gim.golden_id,
                'card'                 AS product_category,
                ca.card_account_id     AS account_id,
                ca.product_code,
                ca.open_date,
                ca.account_status,
                ca.current_balance,
                ca.credit_limit        AS limit_or_original_amt,
                ca.past_due_amount,
                ca.dpd,
                ca.dpd_bucket,
                ca.chargeoff_flag,
                ca.hardship_program_flag
            FROM gold_identity_map gim
            JOIN silver_card_accounts ca ON gim.card_ref = ca.src_customer_ref
            WHERE gim.card_ref IS NOT NULL
        ),
        loan_accts AS (
            SELECT
                gim.golden_id,
                'loan'                 AS product_category,
                la.loan_id             AS account_id,
                la.product_code,
                la.origination_date    AS open_date,
                NULL                   AS account_status,
                la.principal_balance   AS current_balance,
                NULL                   AS limit_or_original_amt,
                la.past_due_amount,
                la.dpd,
                la.dpd_bucket,
                la.chargeoff_flag,
                la.hardship_program_flag
            FROM gold_identity_map gim
            JOIN silver_loan_accounts la ON gim.loan_ref = la.src_customer_ref
            WHERE gim.loan_ref IS NOT NULL
        ),
        deposit_accts AS (
            SELECT
                gim.golden_id,
                'deposit'              AS product_category,
                da.deposit_account_id  AS account_id,
                da.product_code,
                da.open_date,
                da.account_status,
                da.current_balance,
                da.overdraft_limit     AS limit_or_original_amt,
                CASE WHEN da.overdraft_dpd > 0 THEN COALESCE(da.overdraft_used, 0.0) ELSE 0.0 END AS past_due_amount,
                CAST(COALESCE(da.overdraft_dpd, 0) AS INTEGER) AS dpd,
                da.overdraft_dpd_bucket AS dpd_bucket,
                false                  AS chargeoff_flag,
                false                  AS hardship_program_flag
            FROM gold_identity_map gim
            JOIN silver_deposit_accounts da ON gim.deposit_ref = da.src_customer_ref
            WHERE gim.deposit_ref IS NOT NULL
        )
        SELECT * FROM card_accts
        UNION ALL
        SELECT * FROM loan_accts
        UNION ALL
        SELECT * FROM deposit_accts
    """)
    n = con.execute("SELECT count(*) FROM gold_c360_accounts").fetchone()[0]
    print(f"  ✓ gold_c360_accounts: {n:>12,} account records linked to golden_id")

    # Gold C360 Cases — collections cases linked to golden_id
    con.execute("""
        CREATE OR REPLACE TABLE gold_c360_cases AS
        SELECT
            gim.golden_id,
            cc.*
        FROM silver_collections_cases cc
        JOIN ir_crm_clusters cl ON cc.coll_customer_ref = cl.crm_customer_id
        JOIN (SELECT DISTINCT golden_id, crm_root_id FROM gold_identity_map) gim
          ON cl.crm_root_id = gim.crm_root_id
    """)
    n = con.execute("SELECT count(*) FROM gold_c360_cases").fetchone()[0]
    print(f"  ✓ gold_c360_cases: {n:>12,} collections cases linked")

    print(f"\n  Gold C360 layer complete.\n")


# ═══════════════════════════════════════════════════════════════════════
#  PHASE 5: DATA QUALITY CHECKS
# ═══════════════════════════════════════════════════════════════════════

def run_quality_checks(con: duckdb.DuckDBPyConnection) -> list:
    """Run data quality checks and return a list of findings."""
    print("=" * 70)
    print("  PHASE 5: DATA QUALITY CHECKS")
    print("=" * 70)

    findings = []

    # Check 1: Golden ID uniqueness
    dup_golden = con.execute("""
        SELECT count(*) - count(DISTINCT golden_id) AS dup_count
        FROM gold_c360_customer
    """).fetchone()[0]
    status = "PASS" if dup_golden == 0 else "FAIL"
    findings.append({"check": "golden_id_uniqueness", "status": status, "detail": f"{dup_golden} duplicates"})
    print(f"  {'✓' if status == 'PASS' else '✗'} Golden ID Uniqueness: {status} ({dup_golden} duplicates)")

    # Check 2: CRM records with duplicate markers
    dup_crm = con.execute("""
        SELECT count(*) FROM silver_customers
        WHERE duplicate_of_crm_id IS NOT NULL AND duplicate_of_crm_id != ''
    """).fetchone()[0]
    findings.append({"check": "crm_declared_duplicates", "status": "INFO", "detail": f"{dup_crm:,} CRM records flagged as duplicates"})
    print(f"  ℹ CRM Declared Duplicates: {dup_crm:,} records")

    # Check 3: Deceased customers still in active collections
    deceased_active = con.execute("""
        SELECT count(*) FROM gold_c360_customer gc
        JOIN gold_c360_cases gcc ON gc.golden_id = gcc.golden_id
        WHERE gc.deceased_flag = true AND gcc.case_status = 'open'
    """).fetchone()[0]
    status = "WARN" if deceased_active > 0 else "PASS"
    findings.append({"check": "deceased_in_active_collections", "status": status, "detail": f"{deceased_active:,} deceased customers with open cases"})
    print(f"  {'⚠' if status == 'WARN' else '✓'} Deceased in Active Collections: {status} ({deceased_active:,})")

    # Check 4: Vulnerability flag consistency
    vuln_no_type = con.execute("""
        SELECT count(*) FROM silver_customers
        WHERE vulnerability_flag = true
          AND (vulnerability_type IS NULL OR vulnerability_type = '')
    """).fetchone()[0]
    status = "WARN" if vuln_no_type > 0 else "PASS"
    findings.append({"check": "vulnerability_missing_type", "status": status, "detail": f"{vuln_no_type:,} flagged vulnerable but missing type"})
    print(f"  {'⚠' if status == 'WARN' else '✓'} Vulnerability Type Completeness: {status} ({vuln_no_type:,})")

    # Check 5: Null consent fields in gold
    null_consent = con.execute("""
        SELECT count(*) FROM gold_c360_customer
        WHERE consent_call IS NULL OR consent_sms IS NULL
    """).fetchone()[0]
    status = "FAIL" if null_consent > 0 else "PASS"
    findings.append({"check": "consent_null_check", "status": status, "detail": f"{null_consent:,} records with null consent_call or consent_sms"})
    print(f"  {'✗' if status == 'FAIL' else '✓'} Consent Field Completeness: {status} ({null_consent:,})")

    # Check 6: Cards with DPD > 0 but dpd_bucket = 'current'
    dpd_mismatch = con.execute("""
        SELECT count(*) FROM silver_card_accounts
        WHERE dpd > 0 AND dpd_bucket = 'current'
    """).fetchone()[0]
    status = "WARN" if dpd_mismatch > 0 else "PASS"
    findings.append({"check": "card_dpd_bucket_mismatch", "status": status, "detail": f"{dpd_mismatch:,} cards with dpd>0 but bucket='current'"})
    print(f"  {'⚠' if status == 'WARN' else '✓'} Card DPD/Bucket Mismatch: {status} ({dpd_mismatch:,})")

    # Check 7: Orphaned collections cases (no matching CRM record)
    orphan_cases = con.execute("""
        SELECT count(*) FROM silver_collections_cases cc
        LEFT JOIN ir_crm_clusters cl ON cc.coll_customer_ref = cl.crm_customer_id
        WHERE cl.crm_customer_id IS NULL
    """).fetchone()[0]
    status = "WARN" if orphan_cases > 0 else "PASS"
    findings.append({"check": "orphan_collections_cases", "status": status, "detail": f"{orphan_cases:,} cases with no matching CRM record"})
    print(f"  {'⚠' if status == 'WARN' else '✓'} Orphaned Collections Cases: {status} ({orphan_cases:,})")

    print(f"\n  Data Quality checks complete: {len(findings)} checks run.\n")
    return findings


# ═══════════════════════════════════════════════════════════════════════
#  MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "█" * 70)
    print("  COLLECTIONS AI PLATFORM — Layer 1: Data Product Factory")
    print("█" * 70 + "\n")

    start = time.time()

    # Connect to DuckDB
    print(f"  Database: {DB_PATH}")
    con = duckdb.connect(str(DB_PATH))

    # Set memory and thread limits for large dataset
    con.execute("SET memory_limit = '4GB'")
    con.execute("SET threads = 4")

    try:
        # Phase 1: Bronze
        load_bronze(con)

        # Phase 2: Silver
        build_silver(con)

        # Phase 3: Identity Resolution
        n_golden = resolve_identities(con)

        # Phase 4: Gold C360
        build_gold_c360(con)

        # Phase 5: Data Quality
        findings = run_quality_checks(con)

        elapsed = time.time() - start

        # Summary
        print("=" * 70)
        print("  PIPELINE COMPLETE")
        print("=" * 70)
        print(f"  Total time:     {elapsed:.1f}s ({elapsed/60:.1f} min)")
        print(f"  Database:       {DB_PATH}")
        print(f"  Golden records: {n_golden:,}")
        print(f"  Quality checks: {sum(1 for f in findings if f['status'] == 'PASS')}/{len(findings)} passed")
        print("=" * 70)

        # Save quality report
        report_path = Path("d:/VACHAN/data_quality_report.json")
        with open(report_path, "w") as f:
            json.dump({
                "pipeline_run": datetime.now().isoformat(),
                "elapsed_seconds": round(elapsed, 1),
                "golden_customers": n_golden,
                "findings": findings
            }, f, indent=2)
        print(f"\n  Quality report saved to: {report_path}\n")

    finally:
        con.close()


if __name__ == "__main__":
    main()
