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
SELECT 'customer_pool_size', TO_VARIANT('5000'), 'NUMBER', 'Number of consistent customers in the pool'
UNION ALL SELECT 'repeat_customer_probability', TO_VARIANT('0.7'), 'NUMBER', 'Probability of selecting an existing customer for purchases'
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
    -- Reference data checksums for change tracking
    CUSTOMER_SEGMENTS_CHECKSUM STRING,
    PRODUCT_CATEGORIES_CHECKSUM STRING,
    MARKETING_CHANNELS_CHECKSUM STRING,
    WAREHOUSES_CHECKSUM STRING,
    GENERATION_PARAMETERS_CHECKSUM STRING,
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
        """Initialize a consistent pool of customers with stable attributes."""
        pool_size = int(self.params.get('customer_pool_size', 5000))
        self.customer_pool = {}
        
        for i in range(pool_size):
            customer_id = f"CUST_{i+100000:06d}"
            
            # Assign segment based on weights
            segment_weights = [seg["weight"] for seg in self.customer_segments.values()]
            segment = np.random.choice(list(self.customer_segments.keys()), p=segment_weights)
            
            # Generate consistent demographic data
            age_mean = self.params.get('age_mean', 42)
            age_std = self.params.get('age_std', 15)
            age = int(np.random.normal(age_mean, age_std))
            age = max(18, min(80, age))
            
            # Income correlates with segment (using reference data parameters)
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
                "last_purchase_date": None
            }
    
    def _get_customer_profile(self, customer_id: str = None, segment_preference: str = None) -> dict:
        """Get a customer profile, either specific ID or random from pool."""
        if customer_id and customer_id in self.customer_pool:
            return self.customer_pool[customer_id].copy()
        
        available_customers = list(self.customer_pool.keys())
        if segment_preference:
            available_customers = [
                cid for cid in available_customers 
                if self.customer_pool[cid]["segment"] == segment_preference
            ]
        
        if not available_customers:
            available_customers = list(self.customer_pool.keys())
        
        selected_customer_id = random.choice(available_customers)
        return self.customer_pool[selected_customer_id].copy()
    
    def _update_customer_stats(self, customer_id: str, purchase_amount: float, purchase_date: datetime):
        """Update customer statistics based on new purchase."""
        if customer_id in self.customer_pool:
            customer = self.customer_pool[customer_id]
            customer["total_orders"] += 1
            customer["total_spent"] += purchase_amount
            if customer["last_purchase_date"] is None or purchase_date > customer["last_purchase_date"]:
                customer["last_purchase_date"] = purchase_date

    def generate_customer_events(self, num_events: int) -> List[Dict[str, Any]]:
        """Generate customer interaction and transaction events."""
        events = []
        current_time = datetime.utcnow()
        
        for _ in range(num_events):
            customer_profile = self._get_customer_profile()
            customer_id = customer_profile["customer_id"]
            segment = customer_profile["segment"]
            segment_data = self.customer_segments[segment]
            
            days_since_last_purchase = (
                (current_time.date() - customer_profile["last_purchase_date"]).days 
                if customer_profile["last_purchase_date"] 
                else int(np.random.exponential(30))
            )
            
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
                "event_type": "customer_profile_update",
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
        
        for _ in range(num_events):
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
            'CUSTOMER_SEGMENTS_CHECKSUM': audit_data['customer_segments_checksum'],
            'PRODUCT_CATEGORIES_CHECKSUM': audit_data['product_categories_checksum'],
            'MARKETING_CHANNELS_CHECKSUM': audit_data['marketing_channels_checksum'],
            'WAREHOUSES_CHECKSUM': audit_data['warehouses_checksum'],
            'GENERATION_PARAMETERS_CHECKSUM': audit_data['generation_parameters_checksum']
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
            StructField("CUSTOMER_SEGMENTS_CHECKSUM", StringType()),
            StructField("PRODUCT_CATEGORIES_CHECKSUM", StringType()),
            StructField("MARKETING_CHANNELS_CHECKSUM", StringType()),
            StructField("WAREHOUSES_CHECKSUM", StringType()),
            StructField("GENERATION_PARAMETERS_CHECKSUM", StringType())
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
                record['CUSTOMER_SEGMENTS_CHECKSUM'],
                record['PRODUCT_CATEGORIES_CHECKSUM'],
                record['MARKETING_CHANNELS_CHECKSUM'],
                record['WAREHOUSES_CHECKSUM'],
                record['GENERATION_PARAMETERS_CHECKSUM']
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
                json.dumps(event, default=str),
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
BEGIN
    -- Transform customers
    MERGE INTO S2_BRONZE.STG_CUSTOMERS AS target
    USING (
        SELECT 
            RAW_DATA:customer_id::STRING as customer_id,
            RAW_DATA:data.age::INTEGER as age,
            RAW_DATA:data.income::INTEGER as annual_income,
            RAW_DATA:data.customer_segment::STRING as customer_segment,
            RAW_DATA:data.location.state::STRING as state,
            RAW_DATA:data.location.city::STRING as city,
            RAW_DATA:data.location.zip_code::STRING as zip_code,
            RAW_DATA:data.behavioral_metrics.days_since_last_purchase::INTEGER as days_since_last_purchase,
            RAW_DATA:data.behavioral_metrics.total_orders::INTEGER as total_orders,
            RAW_DATA:data.behavioral_metrics.avg_order_value::FLOAT as avg_order_value,
            RAW_DATA:data.behavioral_metrics.churn_probability::FLOAT as churn_probability,
            RAW_DATA:data.behavioral_metrics.lifetime_value::FLOAT as lifetime_value,
            RAW_DATA:data.preferences.primary_category::STRING as primary_category,
            RAW_DATA:data.preferences.communication_channel::STRING as communication_channel,
            RAW_DATA:data.preferences.price_sensitivity::FLOAT as price_sensitivity,
            TIMESTAMP as last_profile_update,
            CURRENT_TIMESTAMP() as updated_at
        FROM PUBLIC.RAW_RETAIL_EVENTS_STREAM
        WHERE EVENT_TYPE = 'customer_profile_update'
            AND METADATA$ACTION = 'INSERT'
    ) AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            age = source.age,
            annual_income = source.annual_income,
            customer_segment = source.customer_segment,
            state = source.state,
            city = source.city,
            zip_code = source.zip_code,
            days_since_last_purchase = source.days_since_last_purchase,
            total_orders = source.total_orders,
            avg_order_value = source.avg_order_value,
            churn_probability = source.churn_probability,
            lifetime_value = source.lifetime_value,
            primary_category = source.primary_category,
            communication_channel = source.communication_channel,
            price_sensitivity = source.price_sensitivity,
            last_profile_update = source.last_profile_update,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (customer_id, age, annual_income, customer_segment, state, city, zip_code,
                days_since_last_purchase, total_orders, avg_order_value, churn_probability,
                lifetime_value, primary_category, communication_channel, price_sensitivity,
                last_profile_update, updated_at)
        VALUES (source.customer_id, source.age, source.annual_income, source.customer_segment,
                source.state, source.city, source.zip_code, source.days_since_last_purchase,
                source.total_orders, source.avg_order_value, source.churn_probability,
                source.lifetime_value, source.primary_category, source.communication_channel,
                source.price_sensitivity, source.last_profile_update, source.updated_at);

    -- Transform purchases
    INSERT INTO S2_BRONZE.STG_PURCHASES 
    SELECT 
        EVENT_ID,
        RAW_DATA:customer_id::STRING as customer_id,
        RAW_DATA:data.order_id::STRING as order_id,
        RAW_DATA:data.product_details.product_id::STRING as product_id,
        RAW_DATA:data.product_details.category::STRING as product_category,
        RAW_DATA:data.product_details.subcategory::STRING as product_subcategory,
        RAW_DATA:data.product_details.brand::STRING as brand,
        RAW_DATA:data.product_details.unit_price::FLOAT as unit_price,
        RAW_DATA:data.product_details.quantity::INTEGER as quantity,
        RAW_DATA:data.product_details.total_amount::FLOAT as line_total,
        RAW_DATA:data.pricing.subtotal::FLOAT as subtotal,
        RAW_DATA:data.pricing.discount_amount::FLOAT as discount_amount,
        RAW_DATA:data.pricing.tax_amount::FLOAT as tax_amount,
        RAW_DATA:data.pricing.shipping_cost::FLOAT as shipping_cost,
        RAW_DATA:data.pricing.final_total::FLOAT as final_total,
        RAW_DATA:data.channel.platform::STRING as sales_channel,
        RAW_DATA:data.channel.device_type::STRING as device_type,
        RAW_DATA:data.channel.payment_method::STRING as payment_method,
        RAW_DATA:data.fulfillment.warehouse_id::STRING as warehouse_id,
        RAW_DATA:data.fulfillment.shipping_method::STRING as shipping_method,
        RAW_DATA:data.fulfillment.estimated_delivery::TIMESTAMP as estimated_delivery_date,
        RAW_DATA:data.ml_features.customer_segment::STRING as customer_segment,
        RAW_DATA:data.ml_features.is_repeat_customer::BOOLEAN as is_repeat_customer,
        RAW_DATA:data.ml_features.cart_abandonment_risk::FLOAT as cart_abandonment_risk,
        RAW_DATA:data.ml_features.return_probability::FLOAT as return_probability,
        RAW_DATA:data.ml_features.upsell_potential::FLOAT as upsell_potential,
        TIMESTAMP as purchase_timestamp,
        CURRENT_TIMESTAMP() as created_at,
        CURRENT_TIMESTAMP() as updated_at
    FROM S1_RAW_DATA.RAW_RETAIL_EVENTS_STREAM
    WHERE EVENT_TYPE = 'purchase'
        AND METADATA$ACTION = 'INSERT';
        
    RETURN 'Bronze transformation completed successfully';
END;
$$;

-- Gold layer transformation procedure
CREATE OR REPLACE PROCEDURE TRANSFORM_TO_GOLD()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Transform to customer dimension
    MERGE INTO S3_GOLD.DIM_CUSTOMERS AS target
    USING (
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
        FROM S2_BRONZE.STG_CUSTOMERS
    ) AS source
    ON target.customer_id = source.customer_id
    WHEN MATCHED THEN
        UPDATE SET
            customer_segment = source.customer_segment,
            age = source.age,
            age_group = source.age_group,
            annual_income = source.annual_income,
            income_bracket = source.income_bracket,
            state = source.state,
            city = source.city,
            primary_category = source.primary_category,
            communication_channel = source.communication_channel,
            price_sensitivity = source.price_sensitivity,
            total_orders = source.total_orders,
            total_spent = source.total_spent,
            avg_order_value = source.avg_order_value,
            customer_tenure_days = source.customer_tenure_days,
            value_segment = source.value_segment,
            engagement_level = source.engagement_level,
            churn_probability = source.churn_probability,
            lifetime_value = source.lifetime_value,
            loyalty_tier = source.loyalty_tier,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (customer_id, customer_segment, age, age_group, annual_income, income_bracket,
                state, city, primary_category, communication_channel, price_sensitivity,
                total_orders, total_spent, avg_order_value, customer_tenure_days,
                preferred_sales_channel, preferred_payment_method, value_segment,
                engagement_level, churn_probability, lifetime_value, loyalty_tier,
                digital_adoption_level, updated_at)
        VALUES (source.customer_id, source.customer_segment, source.age, source.age_group,
                source.annual_income, source.income_bracket, source.state, source.city,
                source.primary_category, source.communication_channel, source.price_sensitivity,
                source.total_orders, source.total_spent, source.avg_order_value,
                source.customer_tenure_days, source.preferred_sales_channel,
                source.preferred_payment_method, source.value_segment, source.engagement_level,
                source.churn_probability, source.lifetime_value, source.loyalty_tier,
                source.digital_adoption_level, source.updated_at);

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
  -- Resume tasks (will succeed if suspended, or give informative error if already running)
  ALTER TASK DATA_GENERATION_TASK RESUME;
  ALTER TASK BRONZE_TRANSFORMATION_TASK RESUME;
  ALTER TASK GOLD_TRANSFORMATION_TASK RESUME;
  
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

-- Check pipeline status
CREATE OR REPLACE PROCEDURE CHECK_PIPELINE_STATUS()
RETURNS TABLE (TASK_NAME STRING, STATE STRING, LAST_RUN TIMESTAMP_LTZ)
LANGUAGE SQL
AS
$$
  SELECT 
    name as task_name,
    state,
    last_committed_on as last_run
  FROM information_schema.tasks
  WHERE task_schema = 'RAW_DATA'
    AND name IN ('DATA_GENERATION_TASK', 'BRONZE_TRANSFORMATION_TASK', 'GOLD_TRANSFORMATION_TASK')
  ORDER BY name;
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

-- =============================================================================
-- SETUP COMPLETE!
-- =============================================================================

SELECT '🎉 SETUP COMPLETE! 🎉' as message;
SELECT 'Run the following commands to start your streaming pipeline:' as instructions;
SELECT '1. CALL START_PIPELINE();                    -- Start automated data generation' as step_1;
SELECT '2. CALL GENERATE_RETAIL_DATA(20, 50, 15, 10); -- Generate initial data batch' as step_2;
SELECT '3. CALL CHECK_PIPELINE_STATUS();             -- Check if tasks are running' as step_3;
SELECT '4. SELECT * FROM TABLE(DATA_SUMMARY());      -- View data across all layers' as step_4;
SELECT '5. SELECT * FROM TABLE(VIEW_REFERENCE_CONFIG()); -- View configuration parameters' as step_5;
SELECT '' as separator;
SELECT 'Sample queries to explore your data:' as sample_queries;
SELECT 'SELECT * FROM S3_GOLD.DIM_CUSTOMERS LIMIT 10;' as customers;
SELECT 'SELECT * FROM S3_GOLD.FCT_SALES LIMIT 10;' as sales;
SELECT 'SELECT customer_segment, COUNT(*), AVG(final_total) FROM S3_GOLD.FCT_SALES GROUP BY customer_segment;' as analysis;


CALL START_PIPELINE();
CALL GENERATE_RETAIL_DATA(2000, 5000, 150, 100);
CALL CHECK_PIPELINE_STATUS();
SELECT * FROM TABLE(DATA_SUMMARY());  
SELECT * FROM TABLE(VIEW_REFERENCE_CONFIG()); 
