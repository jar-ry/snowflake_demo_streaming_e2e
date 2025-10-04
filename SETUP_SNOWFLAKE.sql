-- =============================================================================
-- RETAIL STREAMING DEMO - ONE-CLICK SETUP
-- =============================================================================
-- Run this entire script in Snowflake to set up the complete demo
-- No external tools or CI/CD required - everything runs in Snowflake!

-- =============================================================================
-- STEP 1: CREATE DATABASES AND SCHEMAS
-- =============================================================================

-- Main database for the retail streaming demo
CREATE OR REPLACE DATABASE RETAIL_STREAMING_DEMO
  COMMENT = 'Main database for retail streaming data pipeline demo';

USE DATABASE RETAIL_STREAMING_DEMO;

-- Step 1: Raw data landing zone
CREATE SCHEMA IF NOT EXISTS S1_RAW_DATA
  COMMENT = 'Step 1: Raw data from Snowpark generator and external sources';

-- Step 2: Bronze layer (cleaned staging data)
CREATE SCHEMA IF NOT EXISTS S2_BRONZE
  COMMENT = 'Step 2: Bronze layer - Cleaned and parsed data from raw sources';

-- Step 3: Gold layer (business ready)
CREATE SCHEMA IF NOT EXISTS S3_GOLD
  COMMENT = 'Step 3: Gold layer - Business-ready dimensions and facts for analytics';

-- Reference data (configuration)
CREATE SCHEMA IF NOT EXISTS REFERENCE_DATA
  COMMENT = 'Reference data and configuration tables';

CREATE SCHEMA IF NOT EXISTS DATA_QUALITY
  COMMENT = 'Data quality monitoring and validation results';

-- =============================================================================
-- STEP 2: CREATE VIRTUAL WAREHOUSES
-- =============================================================================

-- Data loading warehouse
CREATE OR REPLACE WAREHOUSE RETAIL_DATA_LOADING_WH
WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Snowpark data generation and raw data loading';

-- Transformation warehouse
CREATE OR REPLACE WAREHOUSE RETAIL_TRANSFORM_WH
WITH 
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for transformations and processing';

-- Analytics warehouse
CREATE OR REPLACE WAREHOUSE RETAIL_ANALYTICS_WH
WITH 
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for analytics queries and business intelligence';

-- =============================================================================
-- STEP 3: CREATE RAW DATA TABLE AND STREAM
-- =============================================================================

USE SCHEMA S1_RAW_DATA;

-- Main landing table for all retail events
CREATE OR REPLACE TABLE RAW_RETAIL_EVENTS (
    EVENT_ID STRING NOT NULL,
    EVENT_TYPE STRING NOT NULL,
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    RAW_DATA VARIANT NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) 
COMMENT = 'Raw landing table for all retail events from Snowpark data generator'
CLUSTER BY (EVENT_TYPE, DATE_TRUNC('DAY', TIMESTAMP));

-- Move to PUBLIC schema for streams
USE SCHEMA PUBLIC;

-- Stream for change data capture
CREATE OR REPLACE STREAM RAW_RETAIL_EVENTS_STREAM 
ON TABLE S1_RAW_DATA.RAW_RETAIL_EVENTS
COMMENT = 'Change data capture stream for incremental processing';

-- =============================================================================
-- STEP 3B: CREATE REFERENCE DATA TABLES
-- =============================================================================

USE SCHEMA REFERENCE_DATA;

-- Customer segment configuration
CREATE OR REPLACE TABLE CUSTOMER_SEGMENTS (
    SEGMENT_NAME STRING PRIMARY KEY,
    WEIGHT FLOAT NOT NULL,
    AVG_ORDER_VALUE FLOAT NOT NULL,
    PURCHASE_FREQUENCY FLOAT NOT NULL,
    DESCRIPTION STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Product category configuration
CREATE OR REPLACE TABLE PRODUCT_CATEGORIES (
    CATEGORY_NAME STRING PRIMARY KEY,
    SEASONAL_PEAK_MONTHS STRING NOT NULL, -- JSON array of months
    MARGIN_RATE FLOAT NOT NULL,
    RETURN_RATE FLOAT NOT NULL,
    DESCRIPTION STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Marketing channel configuration
CREATE OR REPLACE TABLE MARKETING_CHANNELS (
    CHANNEL_NAME STRING PRIMARY KEY,
    COST_PER_ACQUISITION FLOAT NOT NULL,
    CONVERSION_RATE FLOAT NOT NULL,
    ENGAGEMENT_RATE FLOAT NOT NULL,
    DESCRIPTION STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Warehouse configuration
CREATE OR REPLACE TABLE WAREHOUSES (
    WAREHOUSE_ID STRING PRIMARY KEY,
    LOCATION_NAME STRING NOT NULL,
    CAPACITY INTEGER NOT NULL,
    DESCRIPTION STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Data generation parameters
CREATE OR REPLACE TABLE GENERATION_PARAMETERS (
    PARAMETER_NAME STRING PRIMARY KEY,
    PARAMETER_VALUE VARIANT NOT NULL,
    PARAMETER_TYPE STRING NOT NULL, -- 'STRING', 'NUMBER', 'BOOLEAN', 'ARRAY', 'OBJECT'
    DESCRIPTION STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Customer lifecycle tracking table
CREATE OR REPLACE TABLE CUSTOMER_LIFECYCLE (
    CUSTOMER_TYPE STRING PRIMARY KEY, -- 'CORE_LOYAL', 'SLIDING_WINDOW'
    MIN_CUSTOMER_ID_EPOCH INTEGER NOT NULL,
    MAX_CUSTOMER_ID_EPOCH INTEGER NOT NULL,
    CUSTOMER_COUNT INTEGER NOT NULL,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CHURN_PROBABILITY FLOAT DEFAULT 0.0,
    DESCRIPTION STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Data generator sliding window status tracking
-- This table tracks which customers are currently active in the generator's sliding window
CREATE OR REPLACE TABLE GENERATOR_CUSTOMER_STATUS (
    CUSTOMER_ID STRING PRIMARY KEY,
    CUSTOMER_TYPE STRING NOT NULL, -- 'CORE_LOYAL', 'SLIDING_WINDOW'
    IS_ACTIVE_IN_GENERATOR BOOLEAN NOT NULL DEFAULT TRUE,
    LAST_ACTIVITY_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CHURN_DATE TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert default customer segments
INSERT INTO CUSTOMER_SEGMENTS (SEGMENT_NAME, WEIGHT, AVG_ORDER_VALUE, PURCHASE_FREQUENCY, DESCRIPTION) VALUES
('premium', 0.15, 150.0, 0.3, 'High-value customers with frequent purchases'),
('regular', 0.60, 75.0, 0.15, 'Standard customers with moderate spending'),
('occasional', 0.25, 45.0, 0.05, 'Infrequent customers with lower spending');

-- Insert default product categories
INSERT INTO PRODUCT_CATEGORIES (CATEGORY_NAME, SEASONAL_PEAK_MONTHS, MARGIN_RATE, RETURN_RATE, DESCRIPTION) VALUES
('electronics', '[11, 12]', 0.25, 0.08, 'Electronic devices and gadgets'),
('clothing', '[3, 4, 9, 10]', 0.60, 0.15, 'Apparel and fashion items'),
('home_garden', '[4, 5, 6]', 0.45, 0.05, 'Home and garden products'),
('sports_outdoor', '[5, 6, 7, 8]', 0.40, 0.07, 'Sports and outdoor equipment'),
('beauty_health', '[1, 12]', 0.55, 0.03, 'Beauty and health products'),
('books_media', '[9, 10, 11]', 0.35, 0.02, 'Books and media content');

-- Insert default marketing channels
INSERT INTO MARKETING_CHANNELS (CHANNEL_NAME, COST_PER_ACQUISITION, CONVERSION_RATE, ENGAGEMENT_RATE, DESCRIPTION) VALUES
('email', 15.0, 0.03, 0.25, 'Email marketing campaigns'),
('social_media', 25.0, 0.02, 0.08, 'Social media advertising'),
('search_ads', 35.0, 0.05, 0.12, 'Search engine advertising'),
('display_ads', 20.0, 0.015, 0.05, 'Display advertising banners'),
('affiliate', 45.0, 0.08, 0.35, 'Affiliate marketing partnerships'),
('direct', 0.0, 0.12, 0.60, 'Direct traffic and referrals');

-- Insert default warehouses
INSERT INTO WAREHOUSES (WAREHOUSE_ID, LOCATION_NAME, CAPACITY, DESCRIPTION) VALUES
('WH_001', 'West Coast', 25000, 'Primary west coast distribution center'),
('WH_002', 'East Coast', 30000, 'Primary east coast distribution center'),
('WH_003', 'Central', 20000, 'Central region distribution center'),
('WH_004', 'South', 18000, 'Southern region distribution center');

-- Insert default generation parameters
INSERT INTO GENERATION_PARAMETERS (PARAMETER_NAME, PARAMETER_VALUE, PARAMETER_TYPE, DESCRIPTION)
SELECT 'core_loyal_customers', TO_VARIANT('3500'), 'NUMBER', 'Number of core loyal customers who continue spending forever'
UNION ALL SELECT 'sliding_window_customers', TO_VARIANT('1000'), 'NUMBER', 'Number of customers in the sliding window (join and churn)'
UNION ALL SELECT 'sliding_window_days', TO_VARIANT('90'), 'NUMBER', 'Number of days for sliding window customer lifecycle'
UNION ALL SELECT 'new_customer_join_rate', TO_VARIANT('0.08'), 'NUMBER', 'Probability of generating new customers in sliding window per run'
UNION ALL SELECT 'sliding_customer_churn_rate', TO_VARIANT('0.03'), 'NUMBER', 'Probability of sliding window customers churning per run'
UNION ALL SELECT 'repeat_customer_probability', TO_VARIANT('0.8'), 'NUMBER', 'Probability of selecting an existing customer for purchases'
UNION ALL SELECT 'customer_preference_probability', TO_VARIANT('0.6'), 'NUMBER', 'Probability customer buys from preferred category'
UNION ALL SELECT 'income_correlation_premium', TO_VARIANT('11.5'), 'NUMBER', 'Log-normal parameter for premium customer income'
UNION ALL SELECT 'income_correlation_regular', TO_VARIANT('11.0'), 'NUMBER', 'Log-normal parameter for regular customer income'
UNION ALL SELECT 'income_correlation_occasional', TO_VARIANT('10.5'), 'NUMBER', 'Log-normal parameter for occasional customer income'
UNION ALL SELECT 'age_mean', TO_VARIANT('42'), 'NUMBER', 'Mean age for customer generation'
UNION ALL SELECT 'age_std', TO_VARIANT('15'), 'NUMBER', 'Standard deviation for customer age'
UNION ALL SELECT 'price_range_min', TO_VARIANT('10'), 'NUMBER', 'Minimum product price'
UNION ALL SELECT 'price_range_max', TO_VARIANT('500'), 'NUMBER', 'Maximum product price'
UNION ALL SELECT 'tax_rate', TO_VARIANT('0.08'), 'NUMBER', 'Tax rate applied to purchases'
UNION ALL SELECT 'shipping_cost_max', TO_VARIANT('15'), 'NUMBER', 'Maximum shipping cost'
UNION ALL SELECT 'communication_channels', PARSE_JSON('["email", "sms", "push", "mail"]'), 'ARRAY', 'Available communication channels'
UNION ALL SELECT 'payment_methods', PARSE_JSON('["credit_card", "debit_card", "paypal", "apple_pay", "cash"]'), 'ARRAY', 'Available payment methods'
UNION ALL SELECT 'platforms', PARSE_JSON('["web", "mobile_app", "in_store", "phone"]'), 'ARRAY', 'Available sales platforms'
UNION ALL SELECT 'device_types', PARSE_JSON('["desktop", "mobile", "tablet", "pos"]'), 'ARRAY', 'Available device types'
UNION ALL SELECT 'shipping_methods', PARSE_JSON('["standard", "express", "overnight", "pickup"]'), 'ARRAY', 'Available shipping methods';

-- Insert customer lifecycle configuration
-- Core loyal customers: epoch range 1000000000 to 1100000000 (covers ~3 years, plenty of IDs)
-- Sliding window customers: epoch range 1700000000 onwards (starts from 2023, uses current time)
INSERT INTO CUSTOMER_LIFECYCLE (CUSTOMER_TYPE, MIN_CUSTOMER_ID_EPOCH, MAX_CUSTOMER_ID_EPOCH, CUSTOMER_COUNT, IS_ACTIVE, CHURN_PROBABILITY, DESCRIPTION) VALUES
('CORE_LOYAL', 1000000000, 1100000000, 3500, TRUE, 0.001, 'Core loyal customers who continue spending forever with very low churn'),
('SLIDING_WINDOW', 1700000000, EXTRACT(EPOCH FROM CURRENT_TIMESTAMP())::INTEGER, 1000, TRUE, 0.05, 'Sliding window customers who join and churn over time');

-- Audit table for data generation runs
CREATE OR REPLACE TABLE DATA_GENERATION_AUDIT (
    RUN_ID STRING PRIMARY KEY,
    RUN_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    CUSTOMER_EVENTS_REQUESTED INTEGER,
    PURCHASE_EVENTS_REQUESTED INTEGER,
    MARKETING_EVENTS_REQUESTED INTEGER,
    SUPPLY_CHAIN_EVENTS_REQUESTED INTEGER,
    EVENTS_GENERATED INTEGER,
    EXECUTION_STATUS STRING, -- 'SUCCESS', 'FAILED', 'PARTIAL'
    ERROR_MESSAGE STRING,
    EXECUTION_TIME_SECONDS FLOAT,
    -- Snapshot of reference data at time of generation
    CUSTOMER_SEGMENTS_SNAPSHOT VARIANT,
    PRODUCT_CATEGORIES_SNAPSHOT VARIANT,
    MARKETING_CHANNELS_SNAPSHOT VARIANT,
    WAREHOUSES_SNAPSHOT VARIANT,
    GENERATION_PARAMETERS_SNAPSHOT VARIANT,
    CUSTOMER_LIFECYCLE_SNAPSHOT VARIANT,
    -- Reference data checksums for change tracking
    CUSTOMER_SEGMENTS_CHECKSUM STRING,
    PRODUCT_CATEGORIES_CHECKSUM STRING,
    MARKETING_CHANNELS_CHECKSUM STRING,
    WAREHOUSES_CHECKSUM STRING,
    GENERATION_PARAMETERS_CHECKSUM STRING,
    CUSTOMER_LIFECYCLE_CHECKSUM STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STEP 4: CREATE BRONZE LAYER TABLES (STAGING)
-- =============================================================================

USE SCHEMA S2_BRONZE;

-- Customers staging table
CREATE OR REPLACE TABLE STG_CUSTOMERS (
    CUSTOMER_ID STRING NOT NULL PRIMARY KEY,
    AGE INTEGER,
    ANNUAL_INCOME INTEGER,
    CUSTOMER_SEGMENT STRING,
    STATE STRING,
    CITY STRING,
    ZIP_CODE STRING,
    DAYS_SINCE_LAST_PURCHASE INTEGER,
    TOTAL_ORDERS INTEGER,
    AVG_ORDER_VALUE FLOAT,
    CHURN_PROBABILITY FLOAT,
    LIFETIME_VALUE FLOAT,
    PRIMARY_CATEGORY STRING,
    COMMUNICATION_CHANNEL STRING,
    PRICE_SENSITIVITY FLOAT,
    LAST_PROFILE_UPDATE TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Purchases staging table
CREATE OR REPLACE TABLE STG_PURCHASES (
    EVENT_ID STRING NOT NULL PRIMARY KEY,
    CUSTOMER_ID STRING NOT NULL,
    ORDER_ID STRING NOT NULL,
    PRODUCT_ID STRING,
    PRODUCT_CATEGORY STRING,
    PRODUCT_SUBCATEGORY STRING,
    BRAND STRING,
    UNIT_PRICE FLOAT,
    QUANTITY INTEGER,
    LINE_TOTAL FLOAT,
    SUBTOTAL FLOAT,
    DISCOUNT_AMOUNT FLOAT,
    TAX_AMOUNT FLOAT,
    SHIPPING_COST FLOAT,
    FINAL_TOTAL FLOAT,
    SALES_CHANNEL STRING,
    DEVICE_TYPE STRING,
    PAYMENT_METHOD STRING,
    WAREHOUSE_ID STRING,
    SHIPPING_METHOD STRING,
    ESTIMATED_DELIVERY_DATE TIMESTAMP_NTZ,
    CUSTOMER_SEGMENT STRING,
    IS_REPEAT_CUSTOMER BOOLEAN,
    CART_ABANDONMENT_RISK FLOAT,
    RETURN_PROBABILITY FLOAT,
    UPSELL_POTENTIAL FLOAT,
    PURCHASE_TIMESTAMP TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STEP 5: CREATE GOLD LAYER TABLES (BUSINESS READY)
-- =============================================================================

USE SCHEMA S3_GOLD;

-- Customer dimension table
CREATE OR REPLACE TABLE DIM_CUSTOMERS (
    CUSTOMER_ID STRING NOT NULL PRIMARY KEY,
    CUSTOMER_SEGMENT STRING,
    AGE INTEGER,
    AGE_GROUP STRING,
    ANNUAL_INCOME INTEGER,
    INCOME_BRACKET STRING,
    STATE STRING,
    CITY STRING,
    PRIMARY_CATEGORY STRING,
    COMMUNICATION_CHANNEL STRING,
    PRICE_SENSITIVITY FLOAT,
    TOTAL_ORDERS INTEGER,
    TOTAL_SPENT FLOAT,
    AVG_ORDER_VALUE FLOAT,
    FIRST_PURCHASE_DATE TIMESTAMP_NTZ,
    LAST_PURCHASE_DATE TIMESTAMP_NTZ,
    CUSTOMER_TENURE_DAYS INTEGER,
    ANNUAL_PURCHASE_FREQUENCY FLOAT,
    PREFERRED_SALES_CHANNEL STRING,
    PREFERRED_PAYMENT_METHOD STRING,
    VALUE_SEGMENT STRING,
    ENGAGEMENT_LEVEL STRING,
    CHURN_PROBABILITY FLOAT,
    LIFETIME_VALUE FLOAT,
    LOYALTY_TIER STRING,
    DIGITAL_ADOPTION_LEVEL STRING,
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Sales fact table
CREATE OR REPLACE TABLE FCT_SALES (
    EVENT_ID STRING NOT NULL PRIMARY KEY,
    CUSTOMER_ID STRING NOT NULL,
    ORDER_ID STRING NOT NULL,
    PRODUCT_ID STRING,
    PRODUCT_CATEGORY STRING,
    BRAND STRING,
    PURCHASE_TIMESTAMP TIMESTAMP_NTZ,
    PURCHASE_DATE DATE,
    PURCHASE_YEAR INTEGER,
    PURCHASE_QUARTER INTEGER,
    PURCHASE_MONTH INTEGER,
    QUANTITY INTEGER,
    UNIT_PRICE FLOAT,
    FINAL_TOTAL FLOAT,
    DISCOUNT_AMOUNT FLOAT,
    ESTIMATED_GROSS_MARGIN FLOAT,
    SALES_CHANNEL STRING,
    CUSTOMER_SEGMENT STRING,
    IS_REPEAT_CUSTOMER BOOLEAN,
    HIGH_VALUE_ORDER BOOLEAN,
    REVENUE_TIER STRING,
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- STEP 6: CREATE PYTHON DATA GENERATION STORED PROCEDURE
-- =============================================================================

USE SCHEMA PUBLIC;

-- Advanced Python data generation procedure with consistent customers and realistic patterns
CREATE OR REPLACE PROCEDURE GENERATE_RETAIL_DATA(
    CUSTOMER_EVENTS INTEGER DEFAULT 10,
    PURCHASE_EVENTS INTEGER DEFAULT 25, 
    MARKETING_EVENTS INTEGER DEFAULT 8,
    SUPPLY_CHAIN_EVENTS INTEGER DEFAULT 5
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'faker', 'numpy', 'scipy')
HANDLER = 'main'
COMMENT = 'Generates realistic retail streaming data with consistent customer behavior'
AS
$$
import random
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional

# Snowpark imports
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, VariantType

# Third-party libraries
import numpy as np
from faker import Faker
from scipy import stats


class RetailDataGenerator:
    """Generates realistic streaming retail data for multiple business domains."""
    
    def __init__(self, session: Session, batch_size: int = 1000):
        self.session = session
        self.batch_size = batch_size
        self.fake = Faker()
        Faker.seed(42)  # Fixed seed for consistent customer generation
        
        # Load configuration from reference tables
        self._load_reference_data()
        self._init_customer_pool()
    
    def _load_reference_data(self):
        """Load configuration data from reference tables."""
        # Load customer segments
        segments_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.CUSTOMER_SEGMENTS").collect()
        self.customer_segments = {}
        for row in segments_df:
            self.customer_segments[row['SEGMENT_NAME']] = {
                "weight": row['WEIGHT'],
                "avg_order_value": row['AVG_ORDER_VALUE'],
                "purchase_frequency": row['PURCHASE_FREQUENCY']
            }
        
        # Load product categories
        categories_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.PRODUCT_CATEGORIES").collect()
        self.product_categories = {}
        for row in categories_df:
            self.product_categories[row['CATEGORY_NAME']] = {
                "seasonal_peak": json.loads(row['SEASONAL_PEAK_MONTHS']),
                "margin": row['MARGIN_RATE'],
                "return_rate": row['RETURN_RATE']
            }
        
        # Load warehouses
        warehouses_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.WAREHOUSES").collect()
        self.warehouses = []
        for row in warehouses_df:
            self.warehouses.append({
                "id": row['WAREHOUSE_ID'],
                "location": row['LOCATION_NAME'],
                "capacity": row['CAPACITY']
            })
        
        # Load marketing channels
        channels_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.MARKETING_CHANNELS").collect()
        self.marketing_channels = {}
        for row in channels_df:
            self.marketing_channels[row['CHANNEL_NAME']] = {
                "cost_per_acquisition": row['COST_PER_ACQUISITION'],
                "conversion_rate": row['CONVERSION_RATE'],
                "engagement_rate": row['ENGAGEMENT_RATE']
            }
        
        # Load customer lifecycle configuration
        lifecycle_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.CUSTOMER_LIFECYCLE").collect()
        self.customer_lifecycle = {}
        for row in lifecycle_df:
            self.customer_lifecycle[row['CUSTOMER_TYPE']] = {
                "min_epoch": row['MIN_CUSTOMER_ID_EPOCH'],
                "max_epoch": row['MAX_CUSTOMER_ID_EPOCH'],
                "customer_count": row['CUSTOMER_COUNT'],
                "is_active": row['IS_ACTIVE'],
                "churn_probability": row['CHURN_PROBABILITY']
            }
        
        # Load generation parameters
        params_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.GENERATION_PARAMETERS").collect()
        self.params = {}
        for row in params_df:
            param_name = row['PARAMETER_NAME']
            param_value = row['PARAMETER_VALUE']
            param_type = row['PARAMETER_TYPE']
            
            # Handle VARIANT values properly - they come as JSON strings
            if param_type == 'NUMBER':
                # Remove quotes if present and convert to float
                if isinstance(param_value, str):
                    # Handle quoted JSON strings like '"5000"'
                    clean_value = param_value.strip('"')
                    self.params[param_name] = float(clean_value)
                else:
                    self.params[param_name] = float(param_value)
            elif param_type == 'BOOLEAN':
                if isinstance(param_value, str):
                    clean_value = param_value.strip('"').lower()
                    self.params[param_name] = clean_value in ('true', '1', 'yes')
                else:
                    self.params[param_name] = bool(param_value)
            elif param_type == 'ARRAY':
                if isinstance(param_value, str):
                    # If it's already a JSON string, parse it directly
                    self.params[param_name] = json.loads(param_value)
                else:
                    # If it's a VARIANT, convert to string first then parse
                    self.params[param_name] = json.loads(str(param_value))
            elif param_type == 'OBJECT':
                if isinstance(param_value, str):
                    self.params[param_name] = json.loads(param_value)
                else:
                    self.params[param_name] = json.loads(str(param_value))
            else:  # STRING
                if isinstance(param_value, str):
                    self.params[param_name] = param_value.strip('"')
                else:
                    self.params[param_name] = str(param_value)
    
    def _init_customer_pool(self):
        """Initialize customer pools using epoch-based IDs and sliding window logic."""
        import time
        
        self.customer_pool = {}
        self.active_sliding_customers = set()
        current_epoch = int(time.time())
        
        # Initialize core loyal customers (fixed epoch range)
        core_config = self.customer_lifecycle.get('CORE_LOYAL', {})
        core_count = core_config.get('customer_count', 3500)
        core_min_epoch = core_config.get('min_epoch', 1000000000)
        core_max_epoch = core_config.get('max_epoch', 1100000000)
        
        # Generate core loyal customers with deterministic epoch-based IDs
        epoch_step = (core_max_epoch - core_min_epoch) // core_count
        for i in range(core_count):
            customer_epoch = core_min_epoch + (i * epoch_step)
            customer_id = f"CUST_{customer_epoch}"
            
            # Use epoch as seed for consistent customer attributes
            np.random.seed(customer_epoch % 2147483647)  # Ensure seed is within int32 range
            
            # Assign segment based on weights
            segment_weights = [seg["weight"] for seg in self.customer_segments.values()]
            segment = np.random.choice(list(self.customer_segments.keys()), p=segment_weights)
            
            # Generate consistent demographic data
            age_mean = self.params.get('age_mean', 42)
            age_std = self.params.get('age_std', 15)
            age = int(np.random.normal(age_mean, age_std))
            age = max(18, min(80, age))
            
            # Income correlates with segment
            if segment == "premium":
                income_param = self.params.get('income_correlation_premium', 11.5)
                income = int(np.random.lognormal(income_param, 0.5))
            elif segment == "regular":
                income_param = self.params.get('income_correlation_regular', 11.0)
                income = int(np.random.lognormal(income_param, 0.4))
            else:
                income_param = self.params.get('income_correlation_occasional', 10.5)
                income = int(np.random.lognormal(income_param, 0.3))
            
            # Generate consistent location and preferences
            state = self.fake.state()
            city = self.fake.city()
            zip_code = self.fake.zipcode()
            primary_category = random.choice(list(self.product_categories.keys()))
            communication_channel = random.choice(self.params.get('communication_channels', ["email", "sms", "push", "mail"]))
            price_sensitivity = round(random.uniform(0.1, 1.0), 2)
            
            self.customer_pool[customer_id] = {
                "customer_id": customer_id,
                "customer_type": "CORE_LOYAL",
                "segment": segment,
                "age": age,
                "income": income,
                "state": state,
                "city": city,
                "zip_code": zip_code,
                "primary_category": primary_category,
                "communication_channel": communication_channel,
                "price_sensitivity": price_sensitivity,
                "total_orders": 0,
                "total_spent": 0.0,
                "last_purchase_date": None,
                "join_date": datetime.fromtimestamp(customer_epoch),
                "is_active": True
            }
        
        # Initialize sliding window customers (recent epoch range)
        sliding_config = self.customer_lifecycle.get('SLIDING_WINDOW', {})
        sliding_count = sliding_config.get('customer_count', 1000)
        sliding_window_days = int(self.params.get('sliding_window_days', 90))
        
        # Calculate sliding window epoch range (last N days)
        window_start_epoch = current_epoch - (sliding_window_days * 24 * 60 * 60)
        # Update max epoch to current time
        sliding_config['max_epoch'] = current_epoch
        
        # Generate sliding window customers distributed across the time window
        for i in range(sliding_count):
            # Distribute customers across the sliding window timeframe
            customer_epoch = window_start_epoch + int((current_epoch - window_start_epoch) * (i / sliding_count))
            customer_id = f"CUST_{customer_epoch}"
            
            # Use epoch as seed for consistent customer attributes
            np.random.seed(customer_epoch % 2147483647)
            
            # Sliding window customers tend to be newer, so different segment distribution
            segment_weights = [0.2, 0.5, 0.3]  # More occasional customers in sliding window
            segment = np.random.choice(list(self.customer_segments.keys()), p=segment_weights)
            
            # Generate customer attributes (similar to core customers)
            age_mean = self.params.get('age_mean', 42)
            age_std = self.params.get('age_std', 15)
            age = int(np.random.normal(age_mean, age_std))
            age = max(18, min(80, age))
            
            if segment == "premium":
                income_param = self.params.get('income_correlation_premium', 11.5)
                income = int(np.random.lognormal(income_param, 0.5))
            elif segment == "regular":
                income_param = self.params.get('income_correlation_regular', 11.0)
                income = int(np.random.lognormal(income_param, 0.4))
            else:
                income_param = self.params.get('income_correlation_occasional', 10.5)
                income = int(np.random.lognormal(income_param, 0.3))
            
            state = self.fake.state()
            city = self.fake.city()
            zip_code = self.fake.zipcode()
            primary_category = random.choice(list(self.product_categories.keys()))
            communication_channel = random.choice(self.params.get('communication_channels', ["email", "sms", "push", "mail"]))
            price_sensitivity = round(random.uniform(0.1, 1.0), 2)
            
            # Determine if this sliding customer is still active based on churn probability
            days_since_join = (current_epoch - customer_epoch) / (24 * 60 * 60)
            churn_prob = sliding_config.get('churn_probability', 0.05)
            # Higher churn probability for older sliding window customers
            adjusted_churn_prob = churn_prob * (days_since_join / sliding_window_days)
            is_active = random.random() > adjusted_churn_prob
            
            if is_active:
                self.active_sliding_customers.add(customer_id)
            
            self.customer_pool[customer_id] = {
                "customer_id": customer_id,
                "customer_type": "SLIDING_WINDOW",
                "segment": segment,
                "age": age,
                "income": income,
                "state": state,
                "city": city,
                "zip_code": zip_code,
                "primary_category": primary_category,
                "communication_channel": communication_channel,
                "price_sensitivity": price_sensitivity,
                "total_orders": 0,
                "total_spent": 0.0,
                "last_purchase_date": None,
                "join_date": datetime.fromtimestamp(customer_epoch),
                "is_active": is_active
            }
        
        # Reset random seed to current time for subsequent random operations
        np.random.seed(None)
    
    def _get_customer_profile(self, customer_id: str = None, segment_preference: str = None) -> dict:
        """Get a customer profile, checking database for current active status."""
        
        # If specific customer requested, check if they exist and are active
        if customer_id:
            if customer_id in self.customer_pool:
                customer = self.customer_pool[customer_id]
                # Core loyal customers are always active
                if customer["customer_type"] == "CORE_LOYAL":
                    return customer.copy()
                # For sliding window customers, check database for latest status
                else:
                    return self._get_customer_from_database(customer_id)
            else:
                # Customer not in current pool, check database
                return self._get_customer_from_database(customer_id)
        
        # Get random active customer from database
        return self._get_random_active_customer_from_database(segment_preference)
    
    def _get_customer_from_database(self, customer_id: str) -> dict:
        """Get specific customer profile from database with actual generator status."""
        try:
            # Query customer profile with generator status
            query = f"""
            SELECT 
                c.customer_id,
                c.customer_segment,
                c.age,
                c.annual_income,
                c.state,
                c.city,
                c.zip_code,
                c.primary_category,
                c.communication_channel,
                c.price_sensitivity,
                c.total_orders,
                c.avg_order_value,
                c.lifetime_value,
                c.last_profile_update,
                -- Get actual generator status from database
                COALESCE(g.is_active_in_generator, 
                    CASE WHEN c.customer_id LIKE 'CUST_10%' THEN TRUE ELSE FALSE END
                ) as is_active
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_profile_update DESC) as rn
                FROM RETAIL_STREAMING_DEMO.S2_BRONZE.STG_CUSTOMERS
                WHERE customer_id = '{customer_id}'
            ) c
            LEFT JOIN RETAIL_STREAMING_DEMO.REFERENCE_DATA.GENERATOR_CUSTOMER_STATUS g
                ON c.customer_id = g.customer_id
            WHERE c.rn = 1
            """
            
            result = self.session.sql(query).collect()
            
            if result and len(result) > 0:
                row = result[0]
                # Only return if customer is active in generator
                if row['IS_ACTIVE']:
                    return {
                        "customer_id": row['CUSTOMER_ID'],
                        "segment": row['CUSTOMER_SEGMENT'],
                        "age": row['AGE'],
                        "income": row['ANNUAL_INCOME'],
                        "state": row['STATE'],
                        "city": row['CITY'],
                        "zip_code": row['ZIP_CODE'],
                        "primary_category": row['PRIMARY_CATEGORY'],
                        "communication_channel": row['COMMUNICATION_CHANNEL'],
                        "price_sensitivity": row['PRICE_SENSITIVITY'],
                        "total_orders": row['TOTAL_ORDERS'] or 0,
                        "total_spent": (row['TOTAL_ORDERS'] or 0) * (row['AVG_ORDER_VALUE'] or 0),
                        "avg_order_value": row['AVG_ORDER_VALUE'] or 0,
                        "lifetime_value": row['LIFETIME_VALUE'] or 0,
                        "last_purchase_date": row['LAST_PROFILE_UPDATE'],
                        "is_active": row['IS_ACTIVE'],
                        "customer_type": self._determine_customer_type(row['CUSTOMER_ID'])
                    }
            
            return None  # Customer not found or inactive
            
        except Exception as e:
            # If database query fails, fall back to in-memory pool
            if customer_id in self.customer_pool:
                return self.customer_pool[customer_id].copy()
            return None
    
    def _get_random_active_customer_from_database(self, segment_preference: str = None) -> dict:
        """Get random active customer from database based on generator status."""
        try:
            # Build query to get customers active in the generator
            segment_filter = f"AND c.customer_segment = '{segment_preference}'" if segment_preference else ""
            
            query = f"""
            SELECT 
                c.customer_id,
                c.customer_segment,
                c.age,
                c.annual_income,
                c.state,
                c.city,
                c.zip_code,
                c.primary_category,
                c.communication_channel,
                c.price_sensitivity,
                c.total_orders,
                c.avg_order_value,
                c.lifetime_value,
                c.last_profile_update
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_profile_update DESC) as rn
                FROM RETAIL_STREAMING_DEMO.S2_BRONZE.STG_CUSTOMERS
                WHERE 1=1
                {segment_filter}
            ) c
            LEFT JOIN RETAIL_STREAMING_DEMO.REFERENCE_DATA.GENERATOR_CUSTOMER_STATUS g
                ON c.customer_id = g.customer_id
            WHERE c.rn = 1
                AND (
                    -- Core loyal customers are always active
                    c.customer_id LIKE 'CUST_10%' 
                    OR 
                    -- Sliding window customers must be active in generator
                    (g.is_active_in_generator = TRUE OR g.customer_id IS NULL)
                )
            ORDER BY RANDOM()
            LIMIT 1
            """
            
            result = self.session.sql(query).collect()
            
            if result and len(result) > 0:
                row = result[0]
                return {
                    "customer_id": row['CUSTOMER_ID'],
                    "segment": row['CUSTOMER_SEGMENT'],
                    "age": row['AGE'],
                    "income": row['ANNUAL_INCOME'],
                    "state": row['STATE'],
                    "city": row['CITY'],
                    "zip_code": row['ZIP_CODE'],
                    "primary_category": row['PRIMARY_CATEGORY'],
                    "communication_channel": row['COMMUNICATION_CHANNEL'],
                    "price_sensitivity": row['PRICE_SENSITIVITY'],
                    "total_orders": row['TOTAL_ORDERS'] or 0,
                    "total_spent": (row['TOTAL_ORDERS'] or 0) * (row['AVG_ORDER_VALUE'] or 0),
                    "avg_order_value": row['AVG_ORDER_VALUE'] or 0,
                    "lifetime_value": row['LIFETIME_VALUE'] or 0,
                    "last_purchase_date": row['LAST_PROFILE_UPDATE'],
                    "is_active": True,
                    "customer_type": self._determine_customer_type(row['CUSTOMER_ID'])
                }
            
            # Fallback to in-memory pool if no database customers found
            return self._get_random_customer_from_pool(segment_preference)
            
        except Exception as e:
            # If database query fails, fall back to in-memory pool
            return self._get_random_customer_from_pool(segment_preference)
    
    def _determine_customer_type(self, customer_id: str) -> str:
        """Determine customer type based on customer ID epoch."""
        try:
            epoch_str = customer_id.replace("CUST_", "")
            epoch = int(epoch_str)
            
            # Core loyal customers: 1000000000 to 1100000000
            if 1000000000 <= epoch <= 1100000000:
                return "CORE_LOYAL"
            else:
                return "SLIDING_WINDOW"
        except:
            return "SLIDING_WINDOW"  # Default fallback
    
    def _get_random_customer_from_pool(self, segment_preference: str = None) -> dict:
        """Fallback method to get customer from in-memory pool."""
        available_customers = []
        for cid, customer in self.customer_pool.items():
            # Only include active customers
            is_available = (
                customer["customer_type"] == "CORE_LOYAL" or 
                (customer["customer_type"] == "SLIDING_WINDOW" and customer.get("is_active", True))
            )
            
            if is_available:
                if not segment_preference or customer["segment"] == segment_preference:
                    available_customers.append(cid)
        
        if not available_customers:
            # Fallback to all core loyal customers (they're always active)
            available_customers = [
                cid for cid, customer in self.customer_pool.items() 
                if customer["customer_type"] == "CORE_LOYAL"
            ]
        
        if available_customers:
            selected_customer_id = random.choice(available_customers)
            return self.customer_pool[selected_customer_id].copy()
        else:
            return None  # No active customers available
    
    def _manage_sliding_window_customers(self):
        """Manage sliding window customers: add new ones and churn existing ones."""
        import time
        
        current_epoch = int(time.time())
        sliding_config = self.customer_lifecycle.get('SLIDING_WINDOW', {})
        target_sliding_customers = sliding_config.get('customer_count', 1000)
        
        # Count current active sliding window customers
        current_sliding_customers = [
            cid for cid, customer in self.customer_pool.items()
            if customer["customer_type"] == "SLIDING_WINDOW" and customer["is_active"]
        ]
        current_count = len(current_sliding_customers)
        
        # Calculate how many customers we need to add/remove to maintain target
        new_customer_rate = self.params.get('new_customer_join_rate', 0.08)  # 8% chance
        churn_rate = self.params.get('sliding_customer_churn_rate', 0.03)    # 3% chance
        
        # Add new customers (higher probability if below target)
        add_probability = new_customer_rate
        if current_count < target_sliding_customers * 0.8:  # If below 80% of target
            add_probability = new_customer_rate * 2  # Double the add rate
        elif current_count > target_sliding_customers * 1.2:  # If above 120% of target
            add_probability = new_customer_rate * 0.5  # Reduce the add rate
        
        if random.random() < add_probability:
            self._add_new_sliding_customer(current_epoch)
        
        # Churn existing customers (lower probability if below target)
        churn_probability = churn_rate
        if current_count < target_sliding_customers * 0.9:  # If below 90% of target
            churn_probability = churn_rate * 0.5  # Reduce churn rate
        elif current_count > target_sliding_customers * 1.1:  # If above 110% of target
            churn_probability = churn_rate * 1.5  # Increase churn rate
        
        for customer_id in current_sliding_customers:
            if random.random() < churn_probability:
                self.customer_pool[customer_id]["is_active"] = False
                self.active_sliding_customers.discard(customer_id)
                # Update database status
                self._update_generator_customer_status(customer_id, False, "SLIDING_WINDOW")
    
    def _add_new_sliding_customer(self, current_epoch: int):
        """Add a new customer to the sliding window."""
        # Find next available epoch-based ID to avoid collisions
        base_epoch = current_epoch
        customer_id = f"CUST_{base_epoch}"
        
        # If ID already exists, increment epoch until we find an unused one
        while customer_id in self.customer_pool:
            base_epoch += 1
            customer_id = f"CUST_{base_epoch}"
        
        # Use current epoch as seed for consistent attributes
        np.random.seed(current_epoch % 2147483647)
        
        # New customers tend to be occasional segment initially
        segment_weights = [0.1, 0.4, 0.5]  # Favor occasional for new customers
        segment = np.random.choice(list(self.customer_segments.keys()), p=segment_weights)
        
        # Generate customer attributes
        age_mean = self.params.get('age_mean', 42)
        age_std = self.params.get('age_std', 15)
        age = int(np.random.normal(age_mean, age_std))
        age = max(18, min(80, age))
        
        if segment == "premium":
            income_param = self.params.get('income_correlation_premium', 11.5)
            income = int(np.random.lognormal(income_param, 0.5))
        elif segment == "regular":
            income_param = self.params.get('income_correlation_regular', 11.0)
            income = int(np.random.lognormal(income_param, 0.4))
        else:
            income_param = self.params.get('income_correlation_occasional', 10.5)
            income = int(np.random.lognormal(income_param, 0.3))
        
        state = self.fake.state()
        city = self.fake.city()
        zip_code = self.fake.zipcode()
        primary_category = random.choice(list(self.product_categories.keys()))
        communication_channel = random.choice(self.params.get('communication_channels', ["email", "sms", "push", "mail"]))
        price_sensitivity = round(random.uniform(0.1, 1.0), 2)
        
        self.customer_pool[customer_id] = {
            "customer_id": customer_id,
            "customer_type": "SLIDING_WINDOW",
            "segment": segment,
            "age": age,
            "income": income,
            "state": state,
            "city": city,
            "zip_code": zip_code,
            "primary_category": primary_category,
            "communication_channel": communication_channel,
            "price_sensitivity": price_sensitivity,
            "total_orders": 0,
            "total_spent": 0.0,
            "last_purchase_date": None,
            "join_date": datetime.fromtimestamp(current_epoch),
            "is_active": True
        }
        
        self.active_sliding_customers.add(customer_id)
        
        # Update database status for new customer
        self._update_generator_customer_status(customer_id, True, "SLIDING_WINDOW")
        
        # Reset random seed
        np.random.seed(None)
    
    def _update_customer_stats(self, customer_id: str, purchase_amount: float, purchase_date: datetime):
        """Update customer statistics based on new purchase."""
        if customer_id in self.customer_pool:
            customer = self.customer_pool[customer_id]
            customer["total_orders"] += 1
            customer["total_spent"] += purchase_amount
            if customer["last_purchase_date"] is None or purchase_date > customer["last_purchase_date"]:
                customer["last_purchase_date"] = purchase_date
            
            # Reactivate customer if they make a purchase (in case they were marked inactive)
            if customer["customer_type"] == "SLIDING_WINDOW":
                customer["is_active"] = True
                # Update database status
                self._update_generator_customer_status(customer_id, True, "SLIDING_WINDOW")
    
    def _get_active_customer_count(self) -> dict:
        """Get count of active customers by type."""
        counts = {"CORE_LOYAL": 0, "SLIDING_WINDOW_ACTIVE": 0, "SLIDING_WINDOW_INACTIVE": 0}
        
        for customer in self.customer_pool.values():
            if customer["customer_type"] == "CORE_LOYAL":
                counts["CORE_LOYAL"] += 1
            elif customer["customer_type"] == "SLIDING_WINDOW":
                if customer["is_active"]:
                    counts["SLIDING_WINDOW_ACTIVE"] += 1
                else:
                    counts["SLIDING_WINDOW_INACTIVE"] += 1
        
        return counts
    
    def _scale_events_by_customer_count(self, base_events: int) -> int:
        """Scale the number of events based on current active customer count."""
        # Get target customer counts
        core_config = self.customer_lifecycle.get('CORE_LOYAL', {})
        sliding_config = self.customer_lifecycle.get('SLIDING_WINDOW', {})
        
        target_core_customers = core_config.get('customer_count', 3500)
        target_sliding_customers = sliding_config.get('customer_count', 1000)
        target_total_customers = target_core_customers + target_sliding_customers  # 4500
        
        # Count current active customers
        current_active_customers = 0
        for customer in self.customer_pool.values():
            if customer["customer_type"] == "CORE_LOYAL" or customer.get("is_active", True):
                current_active_customers += 1
        
        # Calculate scaling factor
        if target_total_customers > 0:
            scaling_factor = current_active_customers / target_total_customers
        else:
            scaling_factor = 1.0
        
        # Apply scaling with some bounds to prevent extreme values
        scaling_factor = max(0.5, min(2.0, scaling_factor))  # Between 50% and 200%
        
        scaled_events = int(base_events * scaling_factor)
        
        # Ensure we always generate at least some events
        return max(1, scaled_events)
    
    def _update_generator_customer_status(self, customer_id: str, is_active: bool, customer_type: str = None):
        """Update the generator customer status in the database."""
        try:
            if customer_type is None:
                customer_type = self._determine_customer_type(customer_id)
            
            if is_active:
                # Mark customer as active
                query = f"""
                MERGE INTO RETAIL_STREAMING_DEMO.REFERENCE_DATA.GENERATOR_CUSTOMER_STATUS AS target
                USING (SELECT '{customer_id}' as customer_id, '{customer_type}' as customer_type) AS source
                ON target.customer_id = source.customer_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        is_active_in_generator = TRUE,
                        last_activity_date = CURRENT_TIMESTAMP(),
                        churn_date = NULL,
                        updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (customer_id, customer_type, is_active_in_generator, last_activity_date, created_at, updated_at)
                    VALUES ('{customer_id}', '{customer_type}', TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """
            else:
                # Mark customer as churned
                query = f"""
                MERGE INTO RETAIL_STREAMING_DEMO.REFERENCE_DATA.GENERATOR_CUSTOMER_STATUS AS target
                USING (SELECT '{customer_id}' as customer_id, '{customer_type}' as customer_type) AS source
                ON target.customer_id = source.customer_id
                WHEN MATCHED THEN
                    UPDATE SET 
                        is_active_in_generator = FALSE,
                        churn_date = CURRENT_TIMESTAMP(),
                        updated_at = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (customer_id, customer_type, is_active_in_generator, churn_date, created_at, updated_at)
                    VALUES ('{customer_id}', '{customer_type}', FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
                """
            
            self.session.sql(query).collect()
            
        except Exception as e:
            # Don't fail generation if status update fails
            print(f"Warning: Failed to update generator customer status for {customer_id}: {str(e)}")

    def generate_customer_events(self, num_events: int) -> List[Dict[str, Any]]:
        """Generate customer interaction and transaction events."""
        events = []
        current_time = datetime.utcnow()
        
        # Manage sliding window customers before generating events
        self._manage_sliding_window_customers()
        
        # Scale customer events based on active customer count
        scaled_events = self._scale_events_by_customer_count(num_events)
        
        for _ in range(scaled_events):
            customer_profile = self._get_customer_profile()
            if customer_profile is None:
                continue  # Skip if no active customers available
            
            customer_id = customer_profile["customer_id"]
            segment = customer_profile["segment"]
            segment_data = self.customer_segments[segment]
            
            # Calculate days since last purchase, handling both date and datetime types
            days_since_last_purchase = int(np.random.exponential(30))  # Default
            if customer_profile["last_purchase_date"]:
                last_purchase = customer_profile["last_purchase_date"]
                # Convert to date if it's a datetime object
                if hasattr(last_purchase, 'date'):
                    last_purchase = last_purchase.date()
                days_since_last_purchase = (current_time.date() - last_purchase).days
            
            total_orders = customer_profile["total_orders"]
            if total_orders == 0:
                total_orders = int(np.random.negative_binomial(10, 0.3))
            
            churn_prob = self._calculate_churn_probability(
                days_since_last_purchase, total_orders, 
                segment_data["avg_order_value"], segment
            )
            
            event_time = current_time - timedelta(
                minutes=random.randint(0, 1440),
                seconds=random.randint(0, 59)
            )
            
            event = {
                "event_id": f"CUST_EVT_{random.randint(1000000, 9999999)}",
                "customer_id": customer_id,
                "event_type": "customer_profile",
                "timestamp": event_time.isoformat(),
                "data": {
                    "customer_segment": segment,
                    "age": customer_profile["age"],
                    "income": customer_profile["income"],
                    "location": {
                        "state": customer_profile["state"],
                        "city": customer_profile["city"],
                        "zip_code": customer_profile["zip_code"]
                    },
                    "behavioral_metrics": {
                        "days_since_last_purchase": days_since_last_purchase,
                        "total_orders": total_orders,
                        "avg_order_value": round(
                            customer_profile["total_spent"] / max(1, total_orders), 2
                        ) if customer_profile["total_spent"] > 0 else round(segment_data["avg_order_value"] * random.uniform(0.7, 1.3), 2),
                        "churn_probability": round(churn_prob, 4),
                        "lifetime_value": round(customer_profile["total_spent"] if customer_profile["total_spent"] > 0 
                                              else total_orders * segment_data["avg_order_value"] * random.uniform(0.8, 1.5), 2)
                    },
                    "preferences": {
                        "primary_category": customer_profile["primary_category"],
                        "communication_channel": customer_profile["communication_channel"],
                        "price_sensitivity": customer_profile["price_sensitivity"]
                    }
                }
            }
            events.append(event)
        
        return events

    def generate_purchase_events(self, num_events: int) -> List[Dict[str, Any]]:
        """Generate purchase transaction events with realistic patterns."""
        events = []
        current_time = datetime.utcnow()
        
        # Manage sliding window customers before generating events
        self._manage_sliding_window_customers()
        
        # Scale purchase events based on active customer count
        scaled_events = self._scale_events_by_customer_count(num_events)
        
        for _ in range(scaled_events):
            hour = int(np.random.choice(24, p=self._get_hourly_purchase_distribution()))
            event_time = current_time.replace(
                hour=hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            ) - timedelta(hours=random.randint(0, 168))
            
            # Use configurable repeat customer probability
            repeat_prob = self.params.get('repeat_customer_probability', 0.7)
            if random.random() < repeat_prob:
                preferred_segments = ["premium", "regular"] if random.random() < 0.8 else None
                if preferred_segments:
                    segment_choice = random.choice(preferred_segments)
                    customer_profile = self._get_customer_profile(segment_preference=segment_choice)
                else:
                    customer_profile = self._get_customer_profile()
            else:
                customer_profile = self._get_customer_profile()
            
            if customer_profile is None:
                continue  # Skip if no active customers available
            
            customer_id = customer_profile["customer_id"]
            segment = customer_profile["segment"]
            
            # Product selection based on customer preferences
            month = event_time.month
            pref_prob = self.params.get('customer_preference_probability', 0.6)
            if random.random() < pref_prob:
                category = customer_profile["primary_category"]
            else:
                category = self._select_category_by_season(month)
            
            category_data = self.product_categories[category]
            
            price_min = self.params.get('price_range_min', 10)
            price_max = self.params.get('price_range_max', 500)
            base_price = random.uniform(price_min, price_max)
            seasonal_multiplier = self._get_seasonal_multiplier(month, category)
            final_price = base_price * seasonal_multiplier
            
            avg_quantity = max(1, int(5 * np.exp(-final_price / 100)))
            quantity = max(1, int(np.random.poisson(avg_quantity)))
            
            base_discount_prob = {
                "premium": 0.1, "regular": 0.3, "occasional": 0.5
            }[segment]
            
            discount_prob = base_discount_prob * customer_profile["price_sensitivity"] * random.uniform(0.5, 1.5)
            
            discount_amount = 0
            if random.random() < discount_prob:
                discount_amount = final_price * random.uniform(0.05, 0.25)
            
            subtotal = round(final_price * quantity, 2)
            tax_rate = self.params.get('tax_rate', 0.08)
            tax_amount = round((subtotal - discount_amount) * tax_rate, 2)
            shipping_max = self.params.get('shipping_cost_max', 15)
            shipping_cost = round(random.uniform(0, shipping_max), 2)
            final_total = round(subtotal - discount_amount + tax_amount + shipping_cost, 2)
            
            self._update_customer_stats(customer_id, final_total, event_time)
            
            is_repeat_customer = customer_profile["total_orders"] > 0
            
            event = {
                "event_id": f"PURCH_EVT_{random.randint(1000000, 9999999)}",
                "customer_id": customer_id,
                "event_type": "purchase",
                "timestamp": event_time.isoformat(),
                "data": {
                    "order_id": f"ORD_{random.randint(1000000, 9999999)}",
                    "product_details": {
                        "product_id": f"{category.upper()[:3]}_{random.randint(1000, 9999)}",
                        "category": category,
                        "subcategory": self.fake.word(),
                        "brand": self.fake.company(),
                        "unit_price": round(final_price, 2),
                        "quantity": quantity,
                        "total_amount": subtotal
                    },
                    "pricing": {
                        "subtotal": subtotal,
                        "discount_amount": round(discount_amount, 2),
                        "tax_amount": tax_amount,
                        "shipping_cost": shipping_cost,
                        "final_total": final_total
                    },
                    "channel": {
                        "platform": random.choice(self.params.get('platforms', ["web", "mobile_app", "in_store", "phone"])),
                        "device_type": random.choice(self.params.get('device_types', ["desktop", "mobile", "tablet", "pos"])),
                        "payment_method": random.choice(self.params.get('payment_methods', ["credit_card", "debit_card", "paypal", "apple_pay", "cash"]))
                    },
                    "fulfillment": {
                        "warehouse_id": random.choice([wh["id"] for wh in self.warehouses]),
                        "shipping_method": random.choice(self.params.get('shipping_methods', ["standard", "express", "overnight", "pickup"])),
                        "estimated_delivery": (event_time + timedelta(days=random.randint(1, 7))).isoformat()
                    },
                    "ml_features": {
                        "customer_segment": segment,
                        "is_repeat_customer": is_repeat_customer,
                        "cart_abandonment_risk": round(random.uniform(0, 1), 3),
                        "return_probability": round(category_data["return_rate"] * random.uniform(0.5, 1.5), 3),
                        "upsell_potential": round(random.uniform(0, 1), 3)
                    }
                }
            }
            events.append(event)
        
        return events

    def _calculate_churn_probability(self, days_since_last_purchase: int, total_orders: int, 
                                   avg_order_value: float, customer_segment: str) -> float:
        base_prob = min(0.8, days_since_last_purchase / 365.0)
        value_factor = max(0.1, 1.0 - (avg_order_value / 200.0))
        loyalty_factor = max(0.1, 1.0 - (total_orders / 50.0))
        segment_multipliers = {"premium": 0.3, "regular": 0.7, "occasional": 1.2}
        segment_factor = segment_multipliers.get(customer_segment, 1.0)
        churn_prob = base_prob * value_factor * loyalty_factor * segment_factor
        return min(0.95, max(0.01, churn_prob))

    def _get_hourly_purchase_distribution(self) -> List[float]:
        base_probs = [0.01, 0.01, 0.01, 0.01, 0.01, 0.02,
                     0.03, 0.04, 0.05, 0.06, 0.07, 0.08,
                     0.08, 0.07, 0.06, 0.05, 0.06, 0.07,
                     0.08, 0.09, 0.08, 0.06, 0.04, 0.02]
        total = sum(base_probs)
        return [p / total for p in base_probs]

    def _select_category_by_season(self, month: int) -> str:
        weights = []
        categories = []
        for category, data in self.product_categories.items():
            seasonal_boost = 2.0 if month in data["seasonal_peak"] else 1.0
            weights.append(seasonal_boost)
            categories.append(category)
        return np.random.choice(categories, p=np.array(weights) / sum(weights))

    def _get_seasonal_multiplier(self, month: int, category: str) -> float:
        peak_months = self.product_categories.get(category, {}).get("seasonal_peak", [])
        if month in peak_months:
            return random.uniform(1.5, 2.5)
        elif month in [(m + 1) % 12 or 12 for m in peak_months]:
            return random.uniform(1.1, 1.4)
        else:
            return random.uniform(0.7, 1.0)


def _capture_reference_audit_data(session: Session) -> dict:
    """Capture snapshots and checksums of all reference data."""
    import hashlib
    
    audit_data = {}
    
    # Capture customer segments
    segments_df = session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.CUSTOMER_SEGMENTS").collect()
    segments_data = [dict(row.as_dict()) for row in segments_df]
    audit_data['customer_segments_snapshot'] = segments_data
    audit_data['customer_segments_checksum'] = hashlib.md5(json.dumps(segments_data, sort_keys=True, default=str).encode()).hexdigest()
    
    # Capture product categories
    categories_df = session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.PRODUCT_CATEGORIES").collect()
    categories_data = [dict(row.as_dict()) for row in categories_df]
    audit_data['product_categories_snapshot'] = categories_data
    audit_data['product_categories_checksum'] = hashlib.md5(json.dumps(categories_data, sort_keys=True, default=str).encode()).hexdigest()
    
    # Capture marketing channels
    channels_df = session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.MARKETING_CHANNELS").collect()
    channels_data = [dict(row.as_dict()) for row in channels_df]
    audit_data['marketing_channels_snapshot'] = channels_data
    audit_data['marketing_channels_checksum'] = hashlib.md5(json.dumps(channels_data, sort_keys=True, default=str).encode()).hexdigest()
    
    # Capture warehouses
    warehouses_df = session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.WAREHOUSES").collect()
    warehouses_data = [dict(row.as_dict()) for row in warehouses_df]
    audit_data['warehouses_snapshot'] = warehouses_data
    audit_data['warehouses_checksum'] = hashlib.md5(json.dumps(warehouses_data, sort_keys=True, default=str).encode()).hexdigest()
    
    # Capture generation parameters
    params_df = session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.GENERATION_PARAMETERS").collect()
    params_data = [dict(row.as_dict()) for row in params_df]
    audit_data['generation_parameters_snapshot'] = params_data
    audit_data['generation_parameters_checksum'] = hashlib.md5(json.dumps(params_data, sort_keys=True, default=str).encode()).hexdigest()
    
    # Capture customer lifecycle configuration
    lifecycle_df = session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.CUSTOMER_LIFECYCLE").collect()
    lifecycle_data = [dict(row.as_dict()) for row in lifecycle_df]
    audit_data['customer_lifecycle_snapshot'] = lifecycle_data
    audit_data['customer_lifecycle_checksum'] = hashlib.md5(json.dumps(lifecycle_data, sort_keys=True, default=str).encode()).hexdigest()
    
    return audit_data


def _log_generation_run(session: Session, run_id: str, customer_events: int, purchase_events: int,
                       marketing_events: int, supply_chain_events: int, events_generated: int,
                       execution_status: str, error_message: str, execution_time: float,
                       audit_data: dict):
    """Log the generation run to the audit table."""
    try:
        # Create audit record
        audit_row_data = [{
            'RUN_ID': run_id,
            'RUN_TIMESTAMP': datetime.now(),
            'CUSTOMER_EVENTS_REQUESTED': customer_events,
            'PURCHASE_EVENTS_REQUESTED': purchase_events,
            'MARKETING_EVENTS_REQUESTED': marketing_events,
            'SUPPLY_CHAIN_EVENTS_REQUESTED': supply_chain_events,
            'EVENTS_GENERATED': events_generated,
            'EXECUTION_STATUS': execution_status,
            'ERROR_MESSAGE': error_message,
            'EXECUTION_TIME_SECONDS': round(execution_time, 3),
            'CUSTOMER_SEGMENTS_SNAPSHOT': json.dumps(audit_data['customer_segments_snapshot']),
            'PRODUCT_CATEGORIES_SNAPSHOT': json.dumps(audit_data['product_categories_snapshot']),
            'MARKETING_CHANNELS_SNAPSHOT': json.dumps(audit_data['marketing_channels_snapshot']),
            'WAREHOUSES_SNAPSHOT': json.dumps(audit_data['warehouses_snapshot']),
            'GENERATION_PARAMETERS_SNAPSHOT': json.dumps(audit_data['generation_parameters_snapshot']),
            'CUSTOMER_LIFECYCLE_SNAPSHOT': json.dumps(audit_data['customer_lifecycle_snapshot']),
            'CUSTOMER_SEGMENTS_CHECKSUM': audit_data['customer_segments_checksum'],
            'PRODUCT_CATEGORIES_CHECKSUM': audit_data['product_categories_checksum'],
            'MARKETING_CHANNELS_CHECKSUM': audit_data['marketing_channels_checksum'],
            'WAREHOUSES_CHECKSUM': audit_data['warehouses_checksum'],
            'GENERATION_PARAMETERS_CHECKSUM': audit_data['generation_parameters_checksum'],
            'CUSTOMER_LIFECYCLE_CHECKSUM': audit_data['customer_lifecycle_checksum']
        }]
        
        audit_schema = StructType([
            StructField("RUN_ID", StringType()),
            StructField("RUN_TIMESTAMP", TimestampType()),
            StructField("CUSTOMER_EVENTS_REQUESTED", IntegerType()),
            StructField("PURCHASE_EVENTS_REQUESTED", IntegerType()),
            StructField("MARKETING_EVENTS_REQUESTED", IntegerType()),
            StructField("SUPPLY_CHAIN_EVENTS_REQUESTED", IntegerType()),
            StructField("EVENTS_GENERATED", IntegerType()),
            StructField("EXECUTION_STATUS", StringType()),
            StructField("ERROR_MESSAGE", StringType()),
            StructField("EXECUTION_TIME_SECONDS", FloatType()),
            StructField("CUSTOMER_SEGMENTS_SNAPSHOT", VariantType()),
            StructField("PRODUCT_CATEGORIES_SNAPSHOT", VariantType()),
            StructField("MARKETING_CHANNELS_SNAPSHOT", VariantType()),
            StructField("WAREHOUSES_SNAPSHOT", VariantType()),
            StructField("GENERATION_PARAMETERS_SNAPSHOT", VariantType()),
            StructField("CUSTOMER_LIFECYCLE_SNAPSHOT", VariantType()),
            StructField("CUSTOMER_SEGMENTS_CHECKSUM", StringType()),
            StructField("PRODUCT_CATEGORIES_CHECKSUM", StringType()),
            StructField("MARKETING_CHANNELS_CHECKSUM", StringType()),
            StructField("WAREHOUSES_CHECKSUM", StringType()),
            StructField("GENERATION_PARAMETERS_CHECKSUM", StringType()),
            StructField("CUSTOMER_LIFECYCLE_CHECKSUM", StringType())
        ])
        
        audit_df_data = []
        for record in audit_row_data:
            audit_df_data.append([
                record['RUN_ID'],
                record['RUN_TIMESTAMP'],
                record['CUSTOMER_EVENTS_REQUESTED'],
                record['PURCHASE_EVENTS_REQUESTED'],
                record['MARKETING_EVENTS_REQUESTED'],
                record['SUPPLY_CHAIN_EVENTS_REQUESTED'],
                record['EVENTS_GENERATED'],
                record['EXECUTION_STATUS'],
                record['ERROR_MESSAGE'],
                record['EXECUTION_TIME_SECONDS'],
                record['CUSTOMER_SEGMENTS_SNAPSHOT'],
                record['PRODUCT_CATEGORIES_SNAPSHOT'],
                record['MARKETING_CHANNELS_SNAPSHOT'],
                record['WAREHOUSES_SNAPSHOT'],
                record['GENERATION_PARAMETERS_SNAPSHOT'],
                record['CUSTOMER_LIFECYCLE_SNAPSHOT'],
                record['CUSTOMER_SEGMENTS_CHECKSUM'],
                record['PRODUCT_CATEGORIES_CHECKSUM'],
                record['MARKETING_CHANNELS_CHECKSUM'],
                record['WAREHOUSES_CHECKSUM'],
                record['GENERATION_PARAMETERS_CHECKSUM'],
                record['CUSTOMER_LIFECYCLE_CHECKSUM']
            ])
        
        audit_df = session.create_dataframe(audit_df_data, audit_schema)
        audit_df.write.mode("append").save_as_table("REFERENCE_DATA.DATA_GENERATION_AUDIT")
        
    except Exception as e:
        # Don't fail the main operation if audit logging fails
        print(f"Warning: Failed to log audit record: {str(e)}")


def main(session: Session, customer_events: int = 10, purchase_events: int = 25,
         marketing_events: int = 8, supply_chain_events: int = 5) -> str:
    """Main function for the stored procedure."""
    import uuid
    import time
    
    run_id = str(uuid.uuid4())
    start_time = time.time()
    execution_status = 'FAILED'
    error_message = None
    events_generated = 0
    audit_data = {}
    
    try:
        # Capture reference data snapshots and checksums before generation
        audit_data = _capture_reference_audit_data(session)
        
        generator = RetailDataGenerator(session)
        
        all_events = []
        all_events.extend(generator.generate_customer_events(customer_events))
        all_events.extend(generator.generate_purchase_events(purchase_events))
        
        events_generated = len(all_events)
        
        schema = StructType([
            StructField("EVENT_ID", StringType()),
            StructField("EVENT_TYPE", StringType()),
            StructField("TIMESTAMP", TimestampType()),
            StructField("RAW_DATA", VariantType()),
            StructField("CREATED_AT", TimestampType())
        ])
        
        df_data = []
        for event in all_events:
            df_data.append([
                event["event_id"],
                event["event_type"],
                event["timestamp"],
                json.dumps(event),
                datetime.utcnow().isoformat()
            ])
        
        if all_events:
            df = session.create_dataframe(df_data, schema)
            df.write.mode("append").save_as_table("S1_RAW_DATA.RAW_RETAIL_EVENTS")
            execution_status = 'SUCCESS'
        else:
            execution_status = 'PARTIAL'
            error_message = 'No events generated'
            
    except Exception as e:
        execution_status = 'FAILED'
        error_message = str(e)
        
    finally:
        # Log the run to audit table
        execution_time = time.time() - start_time
        _log_generation_run(
            session, run_id, customer_events, purchase_events, marketing_events, 
            supply_chain_events, events_generated, execution_status, error_message, 
            execution_time, audit_data
        )
    
    if execution_status == 'SUCCESS':
        return f"Successfully generated {events_generated} events (Run ID: {run_id})"
    else:
        return f"Generation {execution_status.lower()}: {error_message} (Run ID: {run_id})"
$$;

-- =============================================================================
-- STEP 7: CREATE TRANSFORMATION PROCEDURES (IN PUBLIC SCHEMA)
-- =============================================================================

USE SCHEMA PUBLIC;

-- Bronze layer transformation procedure
CREATE OR REPLACE PROCEDURE TRANSFORM_TO_BRONZE()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    customer_count INTEGER;
    purchase_count INTEGER;
BEGIN
    -- Create a temporary staging table to read stream data ONCE
    -- This prevents the stream from being consumed on the first read
    CREATE OR REPLACE TEMPORARY TABLE TEMP_STREAM_DATA AS
    SELECT 
        parse_json(RAW_DATA) as JSON_DATA,
        METADATA$ACTION as DML_ACTION
    FROM PUBLIC.RAW_RETAIL_EVENTS_STREAM
    WHERE METADATA$ACTION = 'INSERT';
    
    -- Insert customer profiles from temp table
    INSERT INTO S2_BRONZE.STG_CUSTOMERS (
        customer_id, age, annual_income, customer_segment, state, city, zip_code,
        days_since_last_purchase, total_orders, avg_order_value, churn_probability,
        lifetime_value, primary_category, communication_channel, price_sensitivity,
        last_profile_update, updated_at
    )
    SELECT
        JSON_DATA:customer_id::string as customer_id,
        JSON_DATA:data.age::integer as age,
        JSON_DATA:data.income::number as annual_income,
        JSON_DATA:data.customer_segment::string as customer_segment,
        JSON_DATA:data.location.state::string as state,
        JSON_DATA:data.location.city::string as city,
        JSON_DATA:data.location.zip_code::string as zip_code,
        JSON_DATA:data.behavioral_metrics.days_since_last_purchase::integer as days_since_last_purchase,
        JSON_DATA:data.behavioral_metrics.total_orders::integer as total_orders,
        JSON_DATA:data.behavioral_metrics.avg_order_value::float as avg_order_value,
        JSON_DATA:data.behavioral_metrics.churn_probability::float as churn_probability,
        JSON_DATA:data.behavioral_metrics.lifetime_value::float as lifetime_value,
        JSON_DATA:data.preferences.primary_category::string as primary_category,
        JSON_DATA:data.preferences.communication_channel::string as communication_channel,
        JSON_DATA:data.preferences.price_sensitivity::float as price_sensitivity,
        JSON_DATA:timestamp::timestamp_ntz as last_profile_update,
        CURRENT_TIMESTAMP() as updated_at
    FROM TEMP_STREAM_DATA
    WHERE JSON_DATA:event_type::string = 'customer_profile';
    
    customer_count := SQLROWCOUNT;

    -- Insert purchases from temp table
    INSERT INTO S2_BRONZE.STG_PURCHASES 
    SELECT 
        JSON_DATA:event_id::STRING as event_id,
        JSON_DATA:customer_id::STRING as customer_id,
        JSON_DATA:data.order_id::STRING as order_id,
        JSON_DATA:data.product_details.product_id::STRING as product_id,
        JSON_DATA:data.product_details.category::STRING as product_category,
        JSON_DATA:data.product_details.subcategory::STRING as product_subcategory,
        JSON_DATA:data.product_details.brand::STRING as brand,
        JSON_DATA:data.product_details.unit_price::FLOAT as unit_price,
        JSON_DATA:data.product_details.quantity::INTEGER as quantity,
        JSON_DATA:data.product_details.total_amount::FLOAT as line_total,
        JSON_DATA:data.pricing.subtotal::FLOAT as subtotal,
        JSON_DATA:data.pricing.discount_amount::FLOAT as discount_amount,
        JSON_DATA:data.pricing.tax_amount::FLOAT as tax_amount,
        JSON_DATA:data.pricing.shipping_cost::FLOAT as shipping_cost,
        JSON_DATA:data.pricing.final_total::FLOAT as final_total,
        JSON_DATA:data.channel.platform::STRING as sales_channel,
        JSON_DATA:data.channel.device_type::STRING as device_type,
        JSON_DATA:data.channel.payment_method::STRING as payment_method,
        JSON_DATA:data.fulfillment.warehouse_id::STRING as warehouse_id,
        JSON_DATA:data.fulfillment.shipping_method::STRING as shipping_method,
        JSON_DATA:data.fulfillment.estimated_delivery::TIMESTAMP as estimated_delivery_date,
        JSON_DATA:data.ml_features.customer_segment::STRING as customer_segment,
        JSON_DATA:data.ml_features.is_repeat_customer::BOOLEAN as is_repeat_customer,
        JSON_DATA:data.ml_features.cart_abandonment_risk::FLOAT as cart_abandonment_risk,
        JSON_DATA:data.ml_features.return_probability::FLOAT as return_probability,
        JSON_DATA:data.ml_features.upsell_potential::FLOAT as upsell_potential,
        JSON_DATA:timestamp::timestamp_ntz as purchase_timestamp,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
    FROM TEMP_STREAM_DATA
    WHERE JSON_DATA:event_type::string = 'purchase';
    
    purchase_count := SQLROWCOUNT;
    
    -- Clean up temp table
    DROP TABLE IF EXISTS TEMP_STREAM_DATA;
        
    RETURN 'Bronze transformation completed: ' || customer_count || ' customers, ' || purchase_count || ' purchases';
END;
$$;

-- Gold layer transformation procedure
CREATE OR REPLACE PROCEDURE TRANSFORM_TO_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Insert new customer dimension records (append-only from latest bronze profiles)
    -- Only insert customers that don't already exist in the gold layer
    INSERT INTO S3_GOLD.DIM_CUSTOMERS (
        customer_id, customer_segment, age, age_group, annual_income, income_bracket,
        state, city, primary_category, communication_channel, price_sensitivity,
        total_orders, total_spent, avg_order_value, first_purchase_date, last_purchase_date,
        customer_tenure_days, annual_purchase_frequency, preferred_sales_channel,
        preferred_payment_method, value_segment, engagement_level, churn_probability,
        lifetime_value, loyalty_tier, digital_adoption_level, updated_at
    )
    SELECT 
        customer_id,
        customer_segment,
        age,
        CASE 
            WHEN age BETWEEN 18 AND 25 THEN '18-25'
            WHEN age BETWEEN 26 AND 35 THEN '26-35'
            WHEN age BETWEEN 36 AND 45 THEN '36-45'
            WHEN age BETWEEN 46 AND 55 THEN '46-55'
            WHEN age BETWEEN 56 AND 65 THEN '56-65'
            ELSE '65+'
        END as age_group,
        annual_income,
        CASE 
            WHEN annual_income < 30000 THEN 'Low Income'
            WHEN annual_income BETWEEN 30000 AND 60000 THEN 'Middle Income'
            WHEN annual_income BETWEEN 60001 AND 100000 THEN 'Upper Middle Income'
            ELSE 'High Income'
        END as income_bracket,
        state,
        city,
        primary_category,
        communication_channel,
        price_sensitivity,
        total_orders,
        total_orders * avg_order_value as total_spent,
        avg_order_value,
        NULL as first_purchase_date,
        NULL as last_purchase_date,
        COALESCE(days_since_last_purchase, 0) as customer_tenure_days,
        0 as annual_purchase_frequency,
        'web' as preferred_sales_channel,
        'credit_card' as preferred_payment_method,
        CASE 
            WHEN avg_order_value >= 100 THEN 'High Value'
            WHEN avg_order_value >= 50 THEN 'Medium Value'
            ELSE 'Low Value'
        END as value_segment,
        CASE 
            WHEN days_since_last_purchase <= 30 THEN 'Highly Engaged'
            WHEN days_since_last_purchase <= 90 THEN 'Moderately Engaged'
            ELSE 'At Risk'
        END as engagement_level,
        churn_probability,
        lifetime_value,
        CASE 
            WHEN total_orders >= 5 THEN 'Loyal'
            WHEN total_orders >= 2 THEN 'Repeat'
            ELSE 'New'
        END as loyalty_tier,
        'Digital Adopter' as digital_adoption_level,
        CURRENT_TIMESTAMP() as updated_at
    FROM (
        -- Get the latest profile for each customer from bronze layer
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last_profile_update DESC) as rn
            FROM S2_BRONZE.STG_CUSTOMERS
        ) ranked
        WHERE rn = 1
    ) latest_profiles
    WHERE customer_id NOT IN (
        SELECT customer_id FROM S3_GOLD.DIM_CUSTOMERS
    );

    -- Transform to sales fact
    INSERT INTO S3_GOLD.FCT_SALES
    SELECT 
        p.event_id,
        p.customer_id,
        p.order_id,
        p.product_id,
        p.product_category,
        p.brand,
        p.purchase_timestamp,
        DATE(p.purchase_timestamp) as purchase_date,
        EXTRACT(YEAR FROM p.purchase_timestamp) as purchase_year,
        EXTRACT(QUARTER FROM p.purchase_timestamp) as purchase_quarter,
        EXTRACT(MONTH FROM p.purchase_timestamp) as purchase_month,
        p.quantity,
        p.unit_price,
        p.final_total,
        p.discount_amount,
        p.unit_price * 0.4 * p.quantity as estimated_gross_margin,
        p.sales_channel,
        p.customer_segment,
        p.is_repeat_customer,
        CASE WHEN p.final_total >= 100 THEN TRUE ELSE FALSE END as high_value_order,
        CASE 
            WHEN p.final_total >= 200 THEN 'High Value'
            WHEN p.final_total >= 100 THEN 'Medium Value'
            ELSE 'Standard Value'
        END as revenue_tier,
        CURRENT_TIMESTAMP() as updated_at
    FROM S2_BRONZE.STG_PURCHASES p
    WHERE p.event_id NOT IN (SELECT event_id FROM S3_GOLD.FCT_SALES);
    
    RETURN 'Gold transformation completed successfully';
END;
$$;

-- =============================================================================
-- STEP 7B: CREATE HISTORICAL DATA GENERATION PROCEDURE
-- =============================================================================

-- Historical data generation procedure for the last 3 years
CREATE OR REPLACE PROCEDURE GENERATE_HISTORICAL_DATA(
    START_DATE DATE DEFAULT DATEADD('year', -3, CURRENT_DATE()),
    END_DATE DATE DEFAULT CURRENT_DATE(),
    EVENTS_PER_DAY INTEGER DEFAULT 100
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'faker', 'numpy', 'scipy')
HANDLER = 'main'
COMMENT = 'Generates 3 years of historical retail data with realistic patterns and customer evolution'
AS
$$
import random
import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional
import time

# Snowpark imports
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, current_timestamp
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, VariantType

# Third-party libraries
import numpy as np
from faker import Faker
from scipy import stats


class HistoricalRetailDataGenerator:
    """Generates historical retail data with realistic patterns over multiple years."""
    
    def __init__(self, session: Session):
        self.session = session
        self.fake = Faker()
        Faker.seed(42)  # Fixed seed for consistent generation
        
        # Load configuration from reference tables
        self._load_reference_data()
        
        # Historical customer evolution tracking
        self.historical_customers = {}
        self.customer_join_dates = {}
        self.customer_churn_dates = {}
        
    def _load_reference_data(self):
        """Load configuration data from reference tables."""
        # Load customer segments
        segments_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.CUSTOMER_SEGMENTS").collect()
        self.customer_segments = {}
        for row in segments_df:
            self.customer_segments[row['SEGMENT_NAME']] = {
                "weight": row['WEIGHT'],
                "avg_order_value": row['AVG_ORDER_VALUE'],
                "purchase_frequency": row['PURCHASE_FREQUENCY']
            }
        
        # Load product categories
        categories_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.PRODUCT_CATEGORIES").collect()
        self.product_categories = {}
        for row in categories_df:
            self.product_categories[row['CATEGORY_NAME']] = {
                "seasonal_peak": json.loads(row['SEASONAL_PEAK_MONTHS']),
                "margin": row['MARGIN_RATE'],
                "return_rate": row['RETURN_RATE']
            }
        
        # Load warehouses
        warehouses_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.WAREHOUSES").collect()
        self.warehouses = []
        for row in warehouses_df:
            self.warehouses.append({
                "id": row['WAREHOUSE_ID'],
                "location": row['LOCATION_NAME'],
                "capacity": row['CAPACITY']
            })
        
        # Load marketing channels
        channels_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.MARKETING_CHANNELS").collect()
        self.marketing_channels = {}
        for row in channels_df:
            self.marketing_channels[row['CHANNEL_NAME']] = {
                "cost_per_acquisition": row['COST_PER_ACQUISITION'],
                "conversion_rate": row['CONVERSION_RATE'],
                "engagement_rate": row['ENGAGEMENT_RATE']
            }
        
        # Load customer lifecycle configuration
        lifecycle_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.CUSTOMER_LIFECYCLE").collect()
        self.customer_lifecycle = {}
        for row in lifecycle_df:
            self.customer_lifecycle[row['CUSTOMER_TYPE']] = {
                "min_epoch": row['MIN_CUSTOMER_ID_EPOCH'],
                "max_epoch": row['MAX_CUSTOMER_ID_EPOCH'],
                "customer_count": row['CUSTOMER_COUNT'],
                "is_active": row['IS_ACTIVE'],
                "churn_probability": row['CHURN_PROBABILITY']
            }
        
        # Load generation parameters
        params_df = self.session.table("RETAIL_STREAMING_DEMO.REFERENCE_DATA.GENERATION_PARAMETERS").collect()
        self.params = {}
        for row in params_df:
            param_name = row['PARAMETER_NAME']
            param_value = row['PARAMETER_VALUE']
            param_type = row['PARAMETER_TYPE']
            
            # Handle VARIANT values properly
            if param_type == 'NUMBER':
                if isinstance(param_value, str):
                    clean_value = param_value.strip('"')
                    self.params[param_name] = float(clean_value)
                else:
                    self.params[param_name] = float(param_value)
            elif param_type == 'BOOLEAN':
                if isinstance(param_value, str):
                    clean_value = param_value.strip('"').lower()
                    self.params[param_name] = clean_value in ('true', '1', 'yes')
                else:
                    self.params[param_name] = bool(param_value)
            elif param_type == 'ARRAY':
                if isinstance(param_value, str):
                    self.params[param_name] = json.loads(param_value)
                else:
                    self.params[param_name] = json.loads(str(param_value))
            elif param_type == 'OBJECT':
                if isinstance(param_value, str):
                    self.params[param_name] = json.loads(param_value)
                else:
                    self.params[param_name] = json.loads(str(param_value))
            else:  # STRING
                if isinstance(param_value, str):
                    self.params[param_name] = param_value.strip('"')
                else:
                    self.params[param_name] = str(param_value)
    
    def _get_seasonal_multiplier(self, date_obj: datetime, category: str) -> float:
        """Get seasonal multiplier for a given date and category."""
        month = date_obj.month
        peak_months = self.product_categories.get(category, {}).get("seasonal_peak", [])
        
        if month in peak_months:
            return random.uniform(1.8, 2.5)  # Peak season
        elif month in [(m + 1) % 12 or 12 for m in peak_months]:
            return random.uniform(1.2, 1.6)  # Pre/post peak
        else:
            return random.uniform(0.6, 1.0)  # Off season
    
    def _get_yearly_growth_multiplier(self, date_obj: datetime, start_date: datetime) -> float:
        """Get growth multiplier based on years since start."""
        years_elapsed = (date_obj - start_date).days / 365.25
        # Simulate business growth: 15% year-over-year growth
        return (1.15 ** years_elapsed)
    
    def _get_day_of_week_multiplier(self, date_obj: datetime) -> float:
        """Get multiplier based on day of week (higher on weekends)."""
        day_of_week = date_obj.weekday()  # 0=Monday, 6=Sunday
        multipliers = {
            0: 0.8,   # Monday
            1: 0.9,   # Tuesday
            2: 0.9,   # Wednesday
            3: 1.0,   # Thursday
            4: 1.2,   # Friday
            5: 1.4,   # Saturday
            6: 1.3    # Sunday
        }
        return multipliers.get(day_of_week, 1.0)
    
    def _evolve_customer_base(self, current_date: datetime):
        """Evolve the customer base for a given date - add new customers and churn existing ones."""
        current_epoch = int(current_date.timestamp())
        
        # Add new customers based on business growth
        years_since_start = (current_date.year - 2022)  # Assuming we start from 2022
        base_new_customers_per_day = 5
        growth_adjusted_new_customers = int(base_new_customers_per_day * (1.2 ** years_since_start))
        
        # Add seasonal variation to new customer acquisition
        seasonal_multiplier = 1.0
        if current_date.month in [11, 12, 1]:  # Holiday season
            seasonal_multiplier = 1.5
        elif current_date.month in [6, 7, 8]:  # Summer
            seasonal_multiplier = 1.2
        
        new_customers_today = int(growth_adjusted_new_customers * seasonal_multiplier * random.uniform(0.7, 1.3))
        
        # Generate new customers
        for _ in range(new_customers_today):
            # Find next available epoch-based ID to avoid collisions
            base_epoch = current_epoch
            customer_id = f"CUST_{base_epoch}"
            
            # If ID already exists, increment epoch until we find an unused one
            while customer_id in self.historical_customers:
                base_epoch += 1
                customer_id = f"CUST_{base_epoch}"
            
            # New customers start as occasional, may upgrade over time
            if customer_id not in self.historical_customers:
                segment_weights = [0.05, 0.35, 0.60]  # Favor occasional for new customers
                segment = np.random.choice(list(self.customer_segments.keys()), p=segment_weights)
                
                self.historical_customers[customer_id] = {
                    "customer_id": customer_id,
                    "segment": segment,
                    "join_date": current_date,
                    "is_active": True,
                    "total_orders": 0,
                    "total_spent": 0.0,
                    "last_purchase_date": None
                }
                self.customer_join_dates[customer_id] = current_date
        
        # Churn some existing customers
        active_customers = [cid for cid, customer in self.historical_customers.items() 
                          if customer["is_active"] and cid not in self.customer_churn_dates]
        
        for customer_id in active_customers:
            customer = self.historical_customers[customer_id]
            days_since_join = (current_date - customer["join_date"]).days
            
            # Base churn probability increases with time since last purchase
            days_since_last_purchase = 0
            if customer["last_purchase_date"]:
                days_since_last_purchase = (current_date - customer["last_purchase_date"]).days
            else:
                days_since_last_purchase = days_since_join
            
            # Churn probability based on segment and recency
            base_churn_rates = {"premium": 0.0001, "regular": 0.0003, "occasional": 0.0008}
            base_churn = base_churn_rates.get(customer["segment"], 0.0005)
            
            # Increase churn probability with days since last purchase
            recency_multiplier = min(10.0, 1.0 + (days_since_last_purchase / 30.0))
            daily_churn_prob = base_churn * recency_multiplier
            
            if random.random() < daily_churn_prob:
                customer["is_active"] = False
                self.customer_churn_dates[customer_id] = current_date
    
    def _get_active_customers_for_date(self, current_date: datetime) -> List[str]:
        """Get list of active customer IDs for a given date."""
        active_customers = []
        for customer_id, customer in self.historical_customers.items():
            join_date = self.customer_join_dates.get(customer_id, customer["join_date"])
            churn_date = self.customer_churn_dates.get(customer_id)
            
            # Customer is active if they've joined and haven't churned yet
            if join_date <= current_date and (churn_date is None or churn_date > current_date):
                active_customers.append(customer_id)
        
        return active_customers
    
    def generate_historical_events(self, start_date: date, end_date: date, events_per_day: int) -> List[Dict[str, Any]]:
        """Generate historical events for the specified date range."""
        all_events = []
        current_date = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.min.time())
        
        # Initialize with some core loyal customers from the beginning
        core_config = self.customer_lifecycle.get('CORE_LOYAL', {})
        core_count = min(1000, core_config.get('customer_count', 3500))  # Start with subset
        core_min_epoch = core_config.get('min_epoch', 1000000000)
        
        # Add initial core customers
        for i in range(core_count):
            customer_epoch = core_min_epoch + (i * 100)  # Spread them out
            customer_id = f"CUST_{customer_epoch}"
            
            # Use epoch as seed for consistent attributes
            np.random.seed(customer_epoch % 2147483647)
            
            segment_weights = [seg["weight"] for seg in self.customer_segments.values()]
            segment = np.random.choice(list(self.customer_segments.keys()), p=segment_weights)
            
            self.historical_customers[customer_id] = {
                "customer_id": customer_id,
                "segment": segment,
                "join_date": current_date - timedelta(days=random.randint(0, 365)),  # Some history
                "is_active": True,
                "total_orders": 0,
                "total_spent": 0.0,
                "last_purchase_date": None
            }
            self.customer_join_dates[customer_id] = self.historical_customers[customer_id]["join_date"]
        
        # Reset random seed
        np.random.seed(None)
        
        # Generate events day by day
        while current_date <= end_datetime:
            # Evolve customer base for this date
            self._evolve_customer_base(current_date)
            
            # Get active customers for this date
            active_customers = self._get_active_customers_for_date(current_date)
            
            if not active_customers:
                current_date += timedelta(days=1)
                continue
            
            # Calculate events for this day with various multipliers
            base_events = events_per_day
            growth_multiplier = self._get_yearly_growth_multiplier(current_date, datetime.combine(start_date, datetime.min.time()))
            day_multiplier = self._get_day_of_week_multiplier(current_date)
            
            # Scale events based on customer count for this date
            customer_count_multiplier = len(active_customers) / 1000.0  # Normalize to initial 1000 customers
            customer_count_multiplier = max(0.1, min(3.0, customer_count_multiplier))  # Bound between 10% and 300%
            
            # Add some randomness
            random_multiplier = random.uniform(0.7, 1.3)
            
            total_events_today = int(base_events * growth_multiplier * day_multiplier * customer_count_multiplier * random_multiplier)
            
            # Generate events for this day
            for _ in range(total_events_today):
                # Select random customer
                customer_id = random.choice(active_customers)
                customer = self.historical_customers[customer_id]
                
                # Generate purchase event
                event_time = current_date + timedelta(
                    hours=random.randint(8, 22),  # Business hours
                    minutes=random.randint(0, 59),
                    seconds=random.randint(0, 59)
                )
                
                # Select category with seasonal influence
                category = random.choice(list(self.product_categories.keys()))
                seasonal_multiplier = self._get_seasonal_multiplier(current_date, category)
                
                # Generate purchase details
                segment_data = self.customer_segments[customer["segment"]]
                base_price = segment_data["avg_order_value"] * random.uniform(0.5, 2.0)
                final_price = base_price * seasonal_multiplier
                
                quantity = max(1, int(np.random.poisson(2)))
                subtotal = round(final_price * quantity, 2)
                
                # Apply discounts based on segment and season
                discount_prob = {"premium": 0.1, "regular": 0.25, "occasional": 0.4}[customer["segment"]]
                discount_amount = 0
                if random.random() < discount_prob:
                    discount_amount = subtotal * random.uniform(0.05, 0.20)
                
                tax_amount = round((subtotal - discount_amount) * 0.08, 2)
                shipping_cost = round(random.uniform(0, 15), 2)
                final_total = round(subtotal - discount_amount + tax_amount + shipping_cost, 2)
                
                # Update customer stats
                customer["total_orders"] += 1
                customer["total_spent"] += final_total
                customer["last_purchase_date"] = current_date
                
                # Create purchase event
                purchase_event = {
                    "event_id": f"HIST_PURCH_{int(event_time.timestamp())}_{random.randint(1000, 9999)}",
                    "customer_id": customer_id,
                    "event_type": "purchase",
                    "timestamp": event_time.isoformat(),
                    "data": {
                        "order_id": f"ORD_{int(event_time.timestamp())}_{random.randint(1000, 9999)}",
                        "product_details": {
                            "product_id": f"{category.upper()[:3]}_{random.randint(1000, 9999)}",
                            "category": category,
                            "subcategory": self.fake.word(),
                            "brand": self.fake.company(),
                            "unit_price": round(final_price, 2),
                            "quantity": quantity,
                            "total_amount": subtotal
                        },
                        "pricing": {
                            "subtotal": subtotal,
                            "discount_amount": round(discount_amount, 2),
                            "tax_amount": tax_amount,
                            "shipping_cost": shipping_cost,
                            "final_total": final_total
                        },
                        "channel": {
                            "platform": random.choice(["web", "mobile_app", "in_store", "phone"]),
                            "device_type": random.choice(["desktop", "mobile", "tablet", "pos"]),
                            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay", "cash"])
                        },
                        "fulfillment": {
                            "warehouse_id": random.choice([wh["id"] for wh in self.warehouses]),
                            "shipping_method": random.choice(["standard", "express", "overnight", "pickup"]),
                            "estimated_delivery": (event_time + timedelta(days=random.randint(1, 7))).isoformat()
                        },
                        "ml_features": {
                            "customer_segment": customer["segment"],
                            "is_repeat_customer": customer["total_orders"] > 1,
                            "cart_abandonment_risk": round(random.uniform(0, 1), 3),
                            "return_probability": round(self.product_categories[category]["return_rate"] * random.uniform(0.5, 1.5), 3),
                            "upsell_potential": round(random.uniform(0, 1), 3)
                        }
                    }
                }
                all_events.append(purchase_event)
                
                # Occasionally generate customer profile events
                if random.random() < 0.1:  # 10% chance
                    customer_event = {
                        "event_id": f"HIST_CUST_{int(event_time.timestamp())}_{random.randint(1000, 9999)}",
                        "customer_id": customer_id,
                        "event_type": "customer_profile",
                        "timestamp": event_time.isoformat(),
                        "data": {
                            "customer_segment": customer["segment"],
                            "age": random.randint(25, 65),
                            "income": int(np.random.lognormal(11.0, 0.4)),
                            "location": {
                                "state": self.fake.state(),
                                "city": self.fake.city(),
                                "zip_code": self.fake.zipcode()
                            },
                            "behavioral_metrics": {
                                "days_since_last_purchase": 0,
                                "total_orders": customer["total_orders"],
                                "avg_order_value": round(customer["total_spent"] / max(1, customer["total_orders"]), 2),
                                "churn_probability": round(random.uniform(0.01, 0.3), 4),
                                "lifetime_value": round(customer["total_spent"], 2)
                            },
                            "preferences": {
                                "primary_category": category,
                                "communication_channel": random.choice(["email", "sms", "push", "mail"]),
                                "price_sensitivity": round(random.uniform(0.1, 1.0), 2)
                            }
                        }
                    }
                    all_events.append(customer_event)
            
            current_date += timedelta(days=1)
        
        return all_events


def main(session: Session, start_date: date = None, end_date: date = None, events_per_day: int = 100) -> str:
    """Main function for the historical data generation stored procedure."""
    import uuid
    import time as time_module
    
    # Set default dates if not provided
    if start_date is None:
        start_date = date.today() - timedelta(days=3*365)  # 3 years ago
    if end_date is None:
        end_date = date.today()
    
    run_id = str(uuid.uuid4())
    start_time = time_module.time()
    execution_status = 'FAILED'
    error_message = None
    events_generated = 0
    
    try:
        generator = HistoricalRetailDataGenerator(session)
        
        # Generate historical events
        all_events = generator.generate_historical_events(start_date, end_date, events_per_day)
        events_generated = len(all_events)
        
        if all_events:
            # Create schema for the events
            schema = StructType([
                StructField("EVENT_ID", StringType()),
                StructField("EVENT_TYPE", StringType()),
                StructField("TIMESTAMP", TimestampType()),
                StructField("RAW_DATA", VariantType()),
                StructField("CREATED_AT", TimestampType())
            ])
            
            # Process events in batches to avoid memory issues
            batch_size = 10000
            total_batches = (len(all_events) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(all_events))
                batch_events = all_events[start_idx:end_idx]
                
                df_data = []
                for event in batch_events:
                    df_data.append([
                        event["event_id"],
                        event["event_type"],
                        event["timestamp"],
                        json.dumps(event),
                        datetime.utcnow().isoformat()
                    ])
                
                df = session.create_dataframe(df_data, schema)
                df.write.mode("append").save_as_table("S1_RAW_DATA.RAW_RETAIL_EVENTS")
            
            execution_status = 'SUCCESS'
        else:
            execution_status = 'PARTIAL'
            error_message = 'No events generated'
            
    except Exception as e:
        execution_status = 'FAILED'
        error_message = str(e)
    
    execution_time = time_module.time() - start_time
    
    if execution_status == 'SUCCESS':
        return f"Successfully generated {events_generated} historical events from {start_date} to {end_date} (Run ID: {run_id}, Time: {execution_time:.2f}s)"
    else:
        return f"Historical generation {execution_status.lower()}: {error_message} (Run ID: {run_id})"
$$;

-- =============================================================================
-- STEP 8: CREATE AUTOMATED TASKS
-- =============================================================================

-- Root task for data generation (every 30 minutes)
CREATE OR REPLACE TASK DATA_GENERATION_TASK
  WAREHOUSE = RETAIL_DATA_LOADING_WH
  SCHEDULE = 'USING CRON 0,30 * * * * UTC'
  COMMENT = 'Generates streaming retail data every 30 minutes'
AS
  CALL GENERATE_RETAIL_DATA(5, 10, 3, 2);

-- Bronze transformation task
CREATE OR REPLACE TASK BRONZE_TRANSFORMATION_TASK
  WAREHOUSE = RETAIL_TRANSFORM_WH
  COMMENT = 'Transforms raw data into Bronze layer'
  AFTER DATA_GENERATION_TASK
AS
  CALL TRANSFORM_TO_BRONZE();

-- Gold transformation task
CREATE OR REPLACE TASK GOLD_TRANSFORMATION_TASK
  WAREHOUSE = RETAIL_TRANSFORM_WH
  COMMENT = 'Transforms Bronze data into Gold layer'
  AFTER BRONZE_TRANSFORMATION_TASK
AS
  CALL TRANSFORM_TO_GOLD();

-- =============================================================================
-- STEP 9: CREATE MANAGEMENT PROCEDURES
-- =============================================================================

-- Start all tasks
CREATE OR REPLACE PROCEDURE START_PIPELINE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- First, suspend all tasks to ensure clean state (this allows task graph modifications)
  ALTER TASK IF EXISTS DATA_GENERATION_TASK SUSPEND;
  ALTER TASK IF EXISTS BRONZE_TRANSFORMATION_TASK SUSPEND;
  ALTER TASK IF EXISTS GOLD_TRANSFORMATION_TASK SUSPEND;
  
  -- Resume tasks in reverse dependency order (leaf to root)
  -- This ensures child tasks are ready before parent tasks start
  ALTER TASK GOLD_TRANSFORMATION_TASK RESUME;
  ALTER TASK BRONZE_TRANSFORMATION_TASK RESUME;
  ALTER TASK DATA_GENERATION_TASK RESUME;  -- Root task last
  
  RETURN 'Pipeline started successfully! Data will be generated every 30 minutes.';
END;
$$;

-- Stop all tasks
CREATE OR REPLACE PROCEDURE STOP_PIPELINE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  ALTER TASK DATA_GENERATION_TASK SUSPEND;
  ALTER TASK BRONZE_TRANSFORMATION_TASK SUSPEND;
  ALTER TASK GOLD_TRANSFORMATION_TASK SUSPEND;
  RETURN 'Pipeline stopped successfully';
END;
$$;

-- Data summary function (UDF)
CREATE OR REPLACE FUNCTION DATA_SUMMARY()
RETURNS TABLE (LAYER STRING, TABLE_NAME STRING, RECORD_COUNT INTEGER)
AS
$$
  SELECT 'Raw' as layer, 'RAW_RETAIL_EVENTS' as table_name, COUNT(*)::INTEGER as record_count 
  FROM S1_RAW_DATA.RAW_RETAIL_EVENTS
  UNION ALL
  SELECT 'Bronze', 'STG_CUSTOMERS', COUNT(*)::INTEGER FROM S2_BRONZE.STG_CUSTOMERS
  UNION ALL
  SELECT 'Bronze', 'STG_PURCHASES', COUNT(*)::INTEGER FROM S2_BRONZE.STG_PURCHASES
  UNION ALL
  SELECT 'Gold', 'DIM_CUSTOMERS', COUNT(*)::INTEGER FROM S3_GOLD.DIM_CUSTOMERS
  UNION ALL
  SELECT 'Gold', 'FCT_SALES', COUNT(*)::INTEGER FROM S3_GOLD.FCT_SALES
  ORDER BY layer, table_name
$$;

-- Function to view current reference data configuration
CREATE OR REPLACE FUNCTION VIEW_REFERENCE_CONFIG()
RETURNS TABLE (SECTION STRING, NAME STRING, VALUE OBJECT, DESCRIPTION STRING)
AS
$$
  -- Customer segments
  SELECT 'Customer Segments' as section, '' as name, null as value, '' as description
  UNION ALL
  SELECT 'SEGMENT' as section, segment_name as name, 
         object_construct('weight', weight, 'avg_order_value', avg_order_value, 'purchase_frequency', purchase_frequency) as value,
         description 
  FROM REFERENCE_DATA.CUSTOMER_SEGMENTS 
  
  UNION ALL
  
  -- Product categories 
  SELECT 'Product Categories' as section, '' as name, null as value, '' as description
  UNION ALL
  SELECT 'CATEGORY' as section, category_name as name,
         object_construct('seasonal_peaks', seasonal_peak_months, 'margin_rate', margin_rate, 'return_rate', return_rate) as value,
         description
  FROM REFERENCE_DATA.PRODUCT_CATEGORIES
  
  UNION ALL
  
  -- Generation parameters
  SELECT 'Generation Parameters' as section, '' as name, null as value, '' as description
  UNION ALL
  SELECT 'PARAMETER' as section, parameter_name as name, 
         object_construct('type', parameter_type, 'value', parameter_value) as value, 
         description
  FROM REFERENCE_DATA.GENERATION_PARAMETERS
  
  UNION ALL
  
  -- Marketing channels
  SELECT 'Marketing Channels' as section, '' as name, null as value, '' as description
  UNION ALL
  SELECT 'CHANNEL' as section, channel_name as name,
         object_construct('cost_per_acquisition', cost_per_acquisition, 'conversion_rate', conversion_rate, 'engagement_rate', engagement_rate) as value,
         description
  FROM REFERENCE_DATA.MARKETING_CHANNELS
  
  UNION ALL
  
  -- Warehouses
  SELECT 'Warehouses' as section, '' as name, null as value, '' as description
  UNION ALL
  SELECT 'WAREHOUSE' as section, warehouse_id as name,
         object_construct('location', location_name, 'capacity', capacity) as value,
         description
  FROM REFERENCE_DATA.WAREHOUSES
  
  UNION ALL
  
  -- Customer Lifecycle
  SELECT 'Customer Lifecycle' as section, '' as name, null as value, '' as description
  UNION ALL
  SELECT 'LIFECYCLE' as section, customer_type as name,
         object_construct('min_epoch', min_customer_id_epoch, 'max_epoch', max_customer_id_epoch, 
                         'customer_count', customer_count, 'is_active', is_active, 
                         'churn_probability', churn_probability) as value,
         description
  FROM REFERENCE_DATA.CUSTOMER_LIFECYCLE
  
  ORDER BY section, name
$$;

-- Procedure to compare reference data between two runs
CREATE OR REPLACE PROCEDURE COMPARE_GENERATION_RUNS(RUN_ID_1 STRING, RUN_ID_2 STRING)
RETURNS TABLE (COMPONENT STRING, STATUS STRING, DETAILS STRING)
LANGUAGE SQL
AS
$$
    WITH run_comparison AS (
        SELECT 
            r1.RUN_ID as run1_id,
            r1.RUN_TIMESTAMP as run1_timestamp,
            r1.CUSTOMER_SEGMENTS_CHECKSUM as run1_segments_checksum,
            r1.PRODUCT_CATEGORIES_CHECKSUM as run1_categories_checksum,
            r1.GENERATION_PARAMETERS_CHECKSUM as run1_params_checksum,
            r2.RUN_ID as run2_id,
            r2.RUN_TIMESTAMP as run2_timestamp,
            r2.CUSTOMER_SEGMENTS_CHECKSUM as run2_segments_checksum,
            r2.PRODUCT_CATEGORIES_CHECKSUM as run2_categories_checksum,
            r2.GENERATION_PARAMETERS_CHECKSUM as run2_params_checksum
        FROM REFERENCE_DATA.DATA_GENERATION_AUDIT r1
        CROSS JOIN REFERENCE_DATA.DATA_GENERATION_AUDIT r2
        WHERE r1.RUN_ID = RUN_ID_1 AND r2.RUN_ID = RUN_ID_2
    )
    SELECT 'Run Info' as COMPONENT, 'INFO' as STATUS, 
           CONCAT('Run 1: ', run1_timestamp, ' | Run 2: ', run2_timestamp) as DETAILS
    FROM run_comparison
    
    UNION ALL
    
    SELECT 'Customer Segments' as COMPONENT,
           IFF(run1_segments_checksum = run2_segments_checksum, 'UNCHANGED', 'CHANGED') as STATUS,
           IFF(run1_segments_checksum = run2_segments_checksum, 'No changes detected', 
               CONCAT('Checksum changed: ', run1_segments_checksum, ' → ', run2_segments_checksum)) as DETAILS
    FROM run_comparison
    
    UNION ALL
    
    SELECT 'Product Categories' as COMPONENT,
           IFF(run1_categories_checksum = run2_categories_checksum, 'UNCHANGED', 'CHANGED') as STATUS,
           IFF(run1_categories_checksum = run2_categories_checksum, 'No changes detected',
               CONCAT('Checksum changed: ', run1_categories_checksum, ' → ', run2_categories_checksum)) as DETAILS
    FROM run_comparison
    
    UNION ALL
    
    SELECT 'Generation Parameters' as COMPONENT,
           IFF(run1_params_checksum = run2_params_checksum, 'UNCHANGED', 'CHANGED') as STATUS,
           IFF(run1_params_checksum = run2_params_checksum, 'No changes detected',
               CONCAT('Checksum changed: ', run1_params_checksum, ' → ', run2_params_checksum)) as DETAILS
    FROM run_comparison;
$$;

-- Procedure to view reference data snapshot from a specific run
CREATE OR REPLACE PROCEDURE VIEW_RUN_SNAPSHOT(RUN_ID_INPUT STRING, COMPONENT STRING DEFAULT 'ALL')
RETURNS TABLE (COMPONENT_NAME STRING, DATA_SNAPSHOT VARIANT)
LANGUAGE SQL
AS
$$
    SELECT 'Customer Segments' as COMPONENT_NAME, CUSTOMER_SEGMENTS_SNAPSHOT as DATA_SNAPSHOT
    FROM REFERENCE_DATA.DATA_GENERATION_AUDIT 
    WHERE RUN_ID = RUN_ID_INPUT AND (COMPONENT = 'ALL' OR COMPONENT = 'CUSTOMER_SEGMENTS')
    
    UNION ALL
    
    SELECT 'Product Categories' as COMPONENT_NAME, PRODUCT_CATEGORIES_SNAPSHOT as DATA_SNAPSHOT
    FROM REFERENCE_DATA.DATA_GENERATION_AUDIT 
    WHERE RUN_ID = RUN_ID_INPUT AND (COMPONENT = 'ALL' OR COMPONENT = 'PRODUCT_CATEGORIES')
    
    UNION ALL
    
    SELECT 'Marketing Channels' as COMPONENT_NAME, MARKETING_CHANNELS_SNAPSHOT as DATA_SNAPSHOT
    FROM REFERENCE_DATA.DATA_GENERATION_AUDIT 
    WHERE RUN_ID = RUN_ID_INPUT AND (COMPONENT = 'ALL' OR COMPONENT = 'MARKETING_CHANNELS')
    
    UNION ALL
    
    SELECT 'Warehouses' as COMPONENT_NAME, WAREHOUSES_SNAPSHOT as DATA_SNAPSHOT
    FROM REFERENCE_DATA.DATA_GENERATION_AUDIT 
    WHERE RUN_ID = RUN_ID_INPUT AND (COMPONENT = 'ALL' OR COMPONENT = 'WAREHOUSES')
    
    UNION ALL
    
    SELECT 'Generation Parameters' as COMPONENT_NAME, GENERATION_PARAMETERS_SNAPSHOT as DATA_SNAPSHOT
    FROM REFERENCE_DATA.DATA_GENERATION_AUDIT 
    WHERE RUN_ID = RUN_ID_INPUT AND (COMPONENT = 'ALL' OR COMPONENT = 'GENERATION_PARAMETERS');
$$;

-- Procedure to generate historical data for specific date ranges
CREATE OR REPLACE PROCEDURE GENERATE_HISTORICAL_DATA_RANGE(
    START_DATE_STR STRING,
    END_DATE_STR STRING,
    EVENTS_PER_DAY INTEGER DEFAULT 100
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL GENERATE_HISTORICAL_DATA(TO_DATE(START_DATE_STR, 'YYYY-MM-DD'), TO_DATE(END_DATE_STR, 'YYYY-MM-DD'), EVENTS_PER_DAY);
    RETURN CONCAT('Historical data generation initiated for ', START_DATE_STR, ' to ', END_DATE_STR);
END;
$$;

-- Procedure to generate sample historical data (last 30 days)
CREATE OR REPLACE PROCEDURE GENERATE_SAMPLE_HISTORICAL_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL GENERATE_HISTORICAL_DATA(DATEADD('day', -30, CURRENT_DATE()), CURRENT_DATE(), 50);
    RETURN 'Sample historical data generation (last 30 days) initiated';
END;
$$;

-- Function to analyze historical data patterns
CREATE OR REPLACE FUNCTION ANALYZE_HISTORICAL_PATTERNS()
RETURNS TABLE (
    METRIC STRING, 
    TIME_PERIOD STRING, 
    VALUE FLOAT, 
    DESCRIPTION STRING
)
AS
$$
    -- Daily event volume trends
    SELECT 'Daily Events' as METRIC, 
           TO_CHAR(DATE(TIMESTAMP), 'YYYY-MM-DD') as TIME_PERIOD,
           COUNT(*)::FLOAT as VALUE,
           'Number of events per day' as DESCRIPTION
    FROM S1_RAW_DATA.RAW_RETAIL_EVENTS 
    WHERE TIMESTAMP >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY DATE(TIMESTAMP)
    
    UNION ALL
    
    -- Weekly revenue trends
    SELECT 'Weekly Revenue' as METRIC,
           TO_CHAR(DATE_TRUNC('week', DATE(TIMESTAMP)), 'YYYY-MM-DD') as TIME_PERIOD,
           SUM(RAW_DATA:data.pricing.final_total::FLOAT) as VALUE,
           'Total revenue per week' as DESCRIPTION
    FROM S1_RAW_DATA.RAW_RETAIL_EVENTS 
    WHERE EVENT_TYPE = 'purchase' 
      AND TIMESTAMP >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY DATE_TRUNC('week', DATE(TIMESTAMP))
    
    UNION ALL
    
    -- Customer acquisition trends
    SELECT 'New Customers' as METRIC,
           TO_CHAR(DATE(TIMESTAMP), 'YYYY-MM-DD') as TIME_PERIOD,
           COUNT(DISTINCT RAW_DATA:customer_id::STRING) as VALUE,
           'Unique customers per day' as DESCRIPTION
    FROM S1_RAW_DATA.RAW_RETAIL_EVENTS 
    WHERE EVENT_TYPE = 'customer_profile_update'
      AND TIMESTAMP >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY DATE(TIMESTAMP)
    
    ORDER BY METRIC, TIME_PERIOD
$$;

-- =============================================================================
-- SETUP COMPLETE!
-- =============================================================================

SELECT '🎉 SETUP COMPLETE! 🎉' as message;
SELECT 'Run the following commands to start your streaming pipeline:' as instructions;
SELECT '1. CALL START_PIPELINE();                    -- Start automated data generation' as step_1;
SELECT '2. CALL GENERATE_RETAIL_DATA(20, 50, 15, 10); -- Generate initial data batch' as step_2;
SELECT '4. SELECT * FROM TABLE(DATA_SUMMARY());      -- View data across all layers' as step_4;
SELECT '5. SELECT * FROM TABLE(VIEW_REFERENCE_CONFIG()); -- View configuration parameters' as step_5;
SELECT '' as separator_1;
SELECT '📊 HISTORICAL DATA GENERATION OPTIONS:' as historical_options;
SELECT '6. CALL GENERATE_SAMPLE_HISTORICAL_DATA();   -- Generate 30 days of sample data' as step_6;
SELECT '7. CALL GENERATE_HISTORICAL_DATA_RANGE(''2022-01-01'', ''2024-12-31'', 150); -- Generate 3 years of data' as step_7;
SELECT '8. CALL GENERATE_HISTORICAL_DATA();          -- Generate 3 years with default settings' as step_8;
SELECT '9. SELECT * FROM TABLE(ANALYZE_HISTORICAL_PATTERNS()); -- Analyze historical trends' as step_9;
SELECT '' as separator_2;
SELECT 'Sample queries to explore your data:' as sample_queries;
SELECT 'SELECT * FROM S3_GOLD.DIM_CUSTOMERS LIMIT 10;' as customers;
SELECT 'SELECT * FROM S3_GOLD.FCT_SALES LIMIT 10;' as sales;
SELECT 'SELECT customer_segment, COUNT(*), AVG(final_total) FROM S3_GOLD.FCT_SALES GROUP BY customer_segment;' as analysis;
SELECT '' as separator_3;
SELECT '🔍 CUSTOMER LIFECYCLE ANALYSIS:' as lifecycle_analysis;
SELECT 'SELECT SUBSTRING(customer_id, 6, 10) as epoch_id, COUNT(*) FROM S3_GOLD.DIM_CUSTOMERS GROUP BY 1 ORDER BY 1;' as epoch_analysis;
SELECT 'SELECT DATE(TIMESTAMP), COUNT(*) as events FROM S1_RAW_DATA.RAW_RETAIL_EVENTS GROUP BY 1 ORDER BY 1 DESC LIMIT 10;' as daily_events;

CALL GENERATE_HISTORICAL_DATA();

CALL START_PIPELINE();
SHOW TASKS IN DATABASE RETAIL_STREAMING_DEMO;

-- =============================================================================
-- MANUAL TASK EXECUTION (for testing and immediate data generation)
-- =============================================================================

-- Execute the root task (data generation)
EXECUTE TASK DATA_GENERATION_TASK;

-- View the results
SELECT * FROM TABLE(DATA_SUMMARY());  