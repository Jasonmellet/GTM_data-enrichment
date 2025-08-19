-- Add email prediction columns to contacts table
ALTER TABLE summer_camps.contacts 
ADD COLUMN IF NOT EXISTS predicted_email VARCHAR(255),
ADD COLUMN IF NOT EXISTS email_prediction_confidence VARCHAR(50),
ADD COLUMN IF NOT EXISTS email_prediction_timestamp TIMESTAMP,
ADD COLUMN IF NOT EXISTS email_prediction_method VARCHAR(100);

-- Add comments for documentation
COMMENT ON COLUMN summer_camps.contacts.predicted_email IS 'Best predicted email address based on pattern matching and AI analysis';
COMMENT ON COLUMN summer_camps.contacts.email_prediction_confidence IS 'Confidence level of the prediction (high, medium, low)';
COMMENT ON COLUMN summer_camps.contacts.email_prediction_timestamp IS 'When the prediction was made';
COMMENT ON COLUMN summer_camps.contacts.email_prediction_method IS 'Method used for prediction (pattern_based, ai_analysis, combined)';

-- Verify the columns were added
SELECT column_name, data_type, is_nullable 
FROM information_schema.columns 
WHERE table_name = 'contacts' 
AND table_schema = 'summer_camps' 
AND column_name LIKE '%prediction%'
ORDER BY column_name;
