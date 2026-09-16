DO $$
BEGIN
    IF to_regclass('silver.loan_applications') IS NOT NULL THEN
        ALTER TABLE silver.loan_applications
            ADD COLUMN IF NOT EXISTS pipeline_version TEXT;
    END IF;

    IF to_regclass('gold.fact_loan_applications') IS NOT NULL THEN
        ALTER TABLE gold.fact_loan_applications
            ADD COLUMN IF NOT EXISTS run_id UUID;
        ALTER TABLE gold.fact_loan_applications
            ADD COLUMN IF NOT EXISTS ingested_at TIMESTAMPTZ;
    END IF;
END $$;
