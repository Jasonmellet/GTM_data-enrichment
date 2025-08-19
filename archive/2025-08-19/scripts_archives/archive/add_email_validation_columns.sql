-- Add email validation columns to contacts table
ALTER TABLE summer_camps.contacts 
ADD COLUMN IF NOT EXISTS email_validation_status VARCHAR(50),
ADD COLUMN IF NOT EXISTS email_validation_score INTEGER,
ADD COLUMN IF NOT EXISTS email_validation_risk_score INTEGER,
ADD COLUMN IF NOT EXISTS email_validation_timestamp TIMESTAMP,
ADD COLUMN IF NOT EXISTS email_validation_provider VARCHAR(50),
ADD COLUMN IF NOT EXISTS email_validation_details JSONB;

-- Add comments for documentation
COMMENT ON COLUMN summer_camps.contacts.email_validation_status IS 'Validation result: valid, invalid, catch-all, disposable, unknown';
COMMENT ON COLUMN summer_camps.contacts.email_validation_score IS 'Deliverability score 0-100 from ZeroBounce';
COMMENT ON COLUMN summer_camps.contacts.email_validation_risk_score IS 'Risk score 0-100 from ZeroBounce (lower is better)';
COMMENT ON COLUMN summer_camps.contacts.email_validation_timestamp IS 'When the validation was performed';
COMMENT ON COLUMN summer_camps.contacts.email_validation_provider IS 'Validation service used (zerobounce, sendgrid, etc)';
COMMENT ON COLUMN summer_camps.contacts.email_validation_details IS 'Full validation response details in JSON format';

-- Verify the columns were added
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'contacts' 
AND table_schema = 'summer_camps' 
AND column_name LIKE '%validation%'
ORDER BY column_name;
