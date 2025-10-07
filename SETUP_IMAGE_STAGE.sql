-- =============================================================================
-- SNOWFLAKE IMAGE STAGING SETUP
-- =============================================================================

USE DATABASE RETAIL_STREAMING_DEMO;
USE SCHEMA PUBLIC;

-- Create internal stage for images with server-side encryption
CREATE OR REPLACE STAGE IMAGE_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- List the stage (will be empty until you upload files)
LIST @IMAGE_STAGE;

-- =============================================================================
-- INSTRUCTIONS:
-- 1. Upload images using PUT command:
--    PUT file:///path/to/your/images/*.jpg @IMAGE_STAGE;
--
-- 2. Then run this query to create the table:
-- =============================================================================

-- CREATE OR REPLACE TABLE IMAGE_CATALOG AS
--SELECT 
--    TO_FILE(FILE_URL) AS IMG_FILE, 
--    * 
-- FROM DIRECTORY(@IMAGE_STAGE);

