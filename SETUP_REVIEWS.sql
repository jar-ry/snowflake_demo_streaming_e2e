-- =============================================================================
-- CUSTOMER REVIEWS SETUP
-- =============================================================================
-- This script creates and populates a customer reviews table with anonymous
-- feedback about customer service and company experience.
-- 
-- Run this separately from the main SETUP_SNOWFLAKE.sql script.
-- =============================================================================

USE DATABASE RETAIL_STREAMING_DEMO;
USE SCHEMA S3_GOLD;

-- =============================================================================
-- STEP 1: CREATE REVIEWS TABLE
-- =============================================================================

CREATE OR REPLACE TABLE CUSTOMER_REVIEWS (
    REVIEW_ID VARCHAR(50) PRIMARY KEY,
    REVIEW_DATE DATE NOT NULL,
    RATING INTEGER NOT NULL,  -- 1-5 stars
    REVIEW_CATEGORY VARCHAR(50) NOT NULL,  -- 'customer_service', 'product_quality', 'overall_experience'
    REVIEW_TEXT VARCHAR(5000) NOT NULL,
    HELPFUL_COUNT INTEGER DEFAULT 0,
    VERIFIED_PURCHASE BOOLEAN DEFAULT FALSE
);

-- =============================================================================
-- STEP 2: POPULATE WITH 100 SAMPLE REVIEWS
-- =============================================================================

-- Insert positive customer service reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_001', DATEADD('day', -89, CURRENT_DATE()), 5, 'customer_service', 'Absolutely outstanding customer service! The support team went above and beyond to resolve my issue. They were patient, knowledgeable, and genuinely cared about my satisfaction. This is how customer service should be done!', 45, TRUE),
    ('REV_002', DATEADD('day', -87, CURRENT_DATE()), 5, 'customer_service', 'I had a problem with my order and the customer service representative was incredibly helpful. They responded within minutes and had my issue resolved in no time. Best customer service experience I''ve ever had!', 38, TRUE),
    ('REV_003', DATEADD('day', -85, CURRENT_DATE()), 4, 'customer_service', 'Very impressed with the professionalism and efficiency of the customer service team. They handled my return smoothly and kept me informed throughout the process.', 22, TRUE),
    ('REV_004', DATEADD('day', -82, CURRENT_DATE()), 5, 'customer_service', 'The customer service rep I spoke with was amazing! They not only solved my problem but also gave me helpful tips for future orders. Truly exceptional service!', 31, TRUE),
    ('REV_005', DATEADD('day', -80, CURRENT_DATE()), 4, 'customer_service', 'Quick response time and friendly staff. They made the return process painless and even offered a discount on my next purchase. Great experience overall!', 19, TRUE),
    ('REV_006', DATEADD('day', -78, CURRENT_DATE()), 5, 'overall_experience', 'This company sets the bar for excellence! From browsing to checkout to delivery, everything was seamless. The product quality exceeded my expectations and the customer service was top-notch.', 52, TRUE),
    ('REV_007', DATEADD('day', -75, CURRENT_DATE()), 5, 'overall_experience', 'I''ve been a customer for years and they never disappoint. Consistent quality, fair prices, and outstanding service. This is my go-to company for all my needs!', 41, TRUE),
    ('REV_008', DATEADD('day', -73, CURRENT_DATE()), 4, 'product_quality', 'The products are exactly as described and the quality is excellent. Very satisfied with my purchase and will definitely order again.', 27, TRUE),
    ('REV_009', DATEADD('day', -70, CURRENT_DATE()), 5, 'shipping', 'Lightning fast shipping! Ordered on Monday and received it on Wednesday. Package was well-protected and everything arrived in perfect condition.', 33, TRUE),
    ('REV_010', DATEADD('day', -68, CURRENT_DATE()), 4, 'shipping', 'Impressed with the shipping speed and packaging quality. The tracking updates were accurate and helpful. No complaints at all!', 18, TRUE),
    ('REV_011', DATEADD('day', -65, CURRENT_DATE()), 5, 'customer_service', 'Had an issue with a damaged item and the customer service team replaced it immediately with expedited shipping at no extra cost. They really care about their customers!', 44, TRUE),
    ('REV_012', DATEADD('day', -63, CURRENT_DATE()), 5, 'overall_experience', 'Everything about this company is fantastic. User-friendly website, great products, reasonable prices, and stellar customer service. Highly recommend!', 36, TRUE),
    ('REV_013', DATEADD('day', -60, CURRENT_DATE()), 4, 'customer_service', 'The chat support was very responsive and helpful. They answered all my questions and helped me choose the right product for my needs.', 21, TRUE),
    ('REV_014', DATEADD('day', -58, CURRENT_DATE()), 5, 'product_quality', 'Outstanding product quality! You can tell they take pride in what they sell. Worth every penny and then some.', 29, TRUE),
    ('REV_015', DATEADD('day', -55, CURRENT_DATE()), 4, 'overall_experience', 'Great company with excellent values. They stand behind their products and treat customers with respect. Will continue to be a loyal customer.', 25, TRUE);

-- Insert neutral reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_016', DATEADD('day', -53, CURRENT_DATE()), 3, 'customer_service', 'Customer service was okay. They resolved my issue but it took longer than expected. Not bad, but not exceptional either.', 12, TRUE),
    ('REV_017', DATEADD('day', -50, CURRENT_DATE()), 3, 'shipping', 'Shipping was average. Arrived within the estimated timeframe but nothing special. Standard experience.', 8, TRUE),
    ('REV_018', DATEADD('day', -48, CURRENT_DATE()), 3, 'product_quality', 'Product is decent. Does what it''s supposed to do but nothing extraordinary. Fair value for the price.', 14, TRUE),
    ('REV_019', DATEADD('day', -45, CURRENT_DATE()), 3, 'overall_experience', 'Average experience overall. No major complaints but nothing that wowed me either. It''s fine.', 9, TRUE),
    ('REV_020', DATEADD('day', -43, CURRENT_DATE()), 3, 'customer_service', 'The support team was polite but couldn''t fully resolve my issue. Had to call back multiple times. Could be better.', 16, TRUE),
    ('REV_021', DATEADD('day', -40, CURRENT_DATE()), 3, 'shipping', 'Delivery was on time but the packaging could have been better. Product arrived safely though, so no real issues.', 11, TRUE),
    ('REV_022', DATEADD('day', -38, CURRENT_DATE()), 3, 'product_quality', 'Product quality is acceptable. Not premium but not cheap either. Gets the job done.', 7, TRUE),
    ('REV_023', DATEADD('day', -35, CURRENT_DATE()), 3, 'overall_experience', 'It''s an okay company. Nothing special but they deliver what they promise. Would consider ordering again if the price is right.', 10, TRUE),
    ('REV_024', DATEADD('day', -33, CURRENT_DATE()), 3, 'customer_service', 'Customer service was helpful but the wait time was a bit long. Once I got through, they were friendly and professional.', 13, TRUE),
    ('REV_025', DATEADD('day', -30, CURRENT_DATE()), 3, 'overall_experience', 'Mixed feelings. Some things were great, others were just okay. Overall a neutral experience.', 6, TRUE);

-- Insert negative customer service reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_026', DATEADD('day', -28, CURRENT_DATE()), 1, 'customer_service', 'Terrible customer service! I''ve been trying to get a refund for weeks and keep getting the runaround. No one seems to know what they''re doing. Extremely frustrated!', 67, TRUE),
    ('REV_027', DATEADD('day', -25, CURRENT_DATE()), 2, 'customer_service', 'Very disappointed with the customer service. They were rude and unhelpful. Made me feel like I was bothering them by asking questions.', 42, TRUE),
    ('REV_028', DATEADD('day', -23, CURRENT_DATE()), 1, 'customer_service', 'Worst customer service experience ever! They hung up on me twice and then gave me incorrect information. Absolutely unacceptable!', 58, TRUE),
    ('REV_029', DATEADD('day', -20, CURRENT_DATE()), 2, 'customer_service', 'Customer service is severely lacking. Long wait times, unhelpful responses, and they don''t follow through on their promises. Very disappointing.', 39, TRUE),
    ('REV_030', DATEADD('day', -18, CURRENT_DATE()), 1, 'overall_experience', 'This company is a disaster! Poor quality products, terrible customer service, and they don''t honor their return policy. Save your money and shop elsewhere!', 71, TRUE),
    ('REV_031', DATEADD('day', -15, CURRENT_DATE()), 2, 'shipping', 'Shipping took forever! It was supposed to arrive in 3-5 days but took over 2 weeks. No communication about the delay either.', 34, TRUE),
    ('REV_032', DATEADD('day', -13, CURRENT_DATE()), 1, 'product_quality', 'The product quality is absolutely terrible. Broke after one use. Complete waste of money. Do not buy from this company!', 55, TRUE),
    ('REV_033', DATEADD('day', -10, CURRENT_DATE()), 2, 'overall_experience', 'Very unhappy with my experience. The product didn''t match the description and customer service was no help at all. Won''t be ordering again.', 28, TRUE),
    ('REV_034', DATEADD('day', -8, CURRENT_DATE()), 1, 'customer_service', 'Customer service is a joke. They clearly don''t care about their customers. I''ve sent multiple emails with no response. Unbelievable!', 49, TRUE),
    ('REV_035', DATEADD('day', -5, CURRENT_DATE()), 2, 'shipping', 'Package arrived damaged and customer service blamed the shipping company instead of taking responsibility. Very unprofessional.', 31, TRUE);

-- Insert more positive reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_036', DATEADD('day', -3, CURRENT_DATE()), 5, 'customer_service', 'I cannot say enough good things about this company''s customer service! They turned a potentially negative situation into a positive one. True professionals!', 47, TRUE),
    ('REV_037', DATEADD('day', -90, CURRENT_DATE()), 4, 'product_quality', 'High-quality products at reasonable prices. Very satisfied with my purchase and the attention to detail in packaging.', 23, TRUE),
    ('REV_038', DATEADD('day', -86, CURRENT_DATE()), 5, 'overall_experience', 'This is my third order and each time has been perfect. Consistent quality and service. They''ve earned a loyal customer!', 35, TRUE),
    ('REV_039', DATEADD('day', -84, CURRENT_DATE()), 4, 'shipping', 'Fast and reliable shipping. The tracking system is excellent and kept me informed every step of the way.', 20, TRUE),
    ('REV_040', DATEADD('day', -81, CURRENT_DATE()), 5, 'customer_service', 'The customer service team is incredibly patient and understanding. They helped me through a complicated issue with grace and professionalism.', 40, TRUE),
    ('REV_041', DATEADD('day', -79, CURRENT_DATE()), 5, 'overall_experience', 'Exceptional company! They care about quality, customer satisfaction, and doing business the right way. Highly recommend to everyone!', 53, TRUE),
    ('REV_042', DATEADD('day', -76, CURRENT_DATE()), 4, 'product_quality', 'The products are well-made and durable. You can tell they don''t cut corners on quality. Very impressed!', 26, TRUE),
    ('REV_043', DATEADD('day', -74, CURRENT_DATE()), 5, 'customer_service', 'Best customer service in the industry! They respond quickly, listen carefully, and always find a solution. A+ service!', 43, TRUE),
    ('REV_044', DATEADD('day', -71, CURRENT_DATE()), 4, 'shipping', 'Shipping was faster than expected and the package was well-protected. Very pleased with the delivery experience.', 17, TRUE),
    ('REV_045', DATEADD('day', -69, CURRENT_DATE()), 5, 'overall_experience', 'Everything from start to finish was excellent. This company knows how to treat customers right. Will definitely be back!', 38, TRUE);

-- Insert more negative reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_046', DATEADD('day', -66, CURRENT_DATE()), 1, 'overall_experience', 'Absolutely horrible experience from start to finish. Poor quality, worse customer service, and they don''t stand behind their products. Avoid at all costs!', 63, TRUE),
    ('REV_047', DATEADD('day', -64, CURRENT_DATE()), 2, 'customer_service', 'Customer service was unhelpful and seemed annoyed by my questions. Made me feel like I was wasting their time. Not impressed.', 37, TRUE),
    ('REV_048', DATEADD('day', -61, CURRENT_DATE()), 1, 'product_quality', 'The product fell apart within days. Clearly cheaply made. Total ripoff. Demanding a refund but good luck getting through to customer service!', 51, TRUE),
    ('REV_049', DATEADD('day', -59, CURRENT_DATE()), 2, 'shipping', 'Shipping was delayed multiple times with no explanation. When it finally arrived, the package was in terrible condition.', 29, TRUE),
    ('REV_050', DATEADD('day', -56, CURRENT_DATE()), 1, 'customer_service', 'I''ve never been treated so poorly by a customer service department. Rude, dismissive, and completely unhelpful. Disgusting behavior!', 45, TRUE);

-- Insert more neutral and mixed reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_051', DATEADD('day', -54, CURRENT_DATE()), 3, 'overall_experience', 'It''s an average company. Nothing terrible but nothing amazing either. They do what they say they''ll do, which is fine.', 11, TRUE),
    ('REV_052', DATEADD('day', -51, CURRENT_DATE()), 3, 'customer_service', 'Customer service was okay. They answered my questions but weren''t particularly friendly or enthusiastic. Just doing their job.', 9, TRUE),
    ('REV_053', DATEADD('day', -49, CURRENT_DATE()), 3, 'product_quality', 'Product quality is middle of the road. Not great, not terrible. You get what you pay for.', 8, TRUE),
    ('REV_054', DATEADD('day', -46, CURRENT_DATE()), 3, 'shipping', 'Shipping was as expected. Nothing to complain about but nothing to rave about either. Standard delivery.', 7, TRUE),
    ('REV_055', DATEADD('day', -44, CURRENT_DATE()), 3, 'overall_experience', 'Decent experience overall. Had a few minor issues but nothing major. Would consider ordering again.', 10, TRUE);

-- Insert more positive reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_056', DATEADD('day', -41, CURRENT_DATE()), 5, 'customer_service', 'The customer service representative went out of their way to help me. They even followed up to make sure everything was resolved. Outstanding!', 42, TRUE),
    ('REV_057', DATEADD('day', -39, CURRENT_DATE()), 4, 'product_quality', 'Really impressed with the quality. The product is well-designed and built to last. Worth the investment!', 24, TRUE),
    ('REV_058', DATEADD('day', -36, CURRENT_DATE()), 5, 'overall_experience', 'This company has restored my faith in online shopping! Professional, reliable, and they genuinely care about customer satisfaction.', 48, TRUE),
    ('REV_059', DATEADD('day', -34, CURRENT_DATE()), 4, 'shipping', 'Quick shipping and excellent packaging. The product arrived in perfect condition and ahead of schedule.', 19, TRUE),
    ('REV_060', DATEADD('day', -31, CURRENT_DATE()), 5, 'customer_service', 'Had a complex issue and the customer service team handled it with expertise and care. They really know their stuff!', 36, TRUE),
    ('REV_061', DATEADD('day', -29, CURRENT_DATE()), 5, 'overall_experience', 'Fantastic company! Every interaction has been positive. They set the standard for how businesses should operate.', 44, TRUE),
    ('REV_062', DATEADD('day', -26, CURRENT_DATE()), 4, 'product_quality', 'The quality exceeded my expectations. You can tell they take pride in their products. Very happy customer!', 28, TRUE),
    ('REV_063', DATEADD('day', -24, CURRENT_DATE()), 5, 'customer_service', 'Customer service is phenomenal! They''re friendly, efficient, and always willing to help. Can''t ask for more!', 39, TRUE),
    ('REV_064', DATEADD('day', -21, CURRENT_DATE()), 4, 'shipping', 'Impressed with the shipping speed and care taken in packaging. Everything arrived safely and quickly.', 21, TRUE),
    ('REV_065', DATEADD('day', -19, CURRENT_DATE()), 5, 'overall_experience', 'This is how all companies should operate! Professional, responsive, and they deliver on their promises. A+ all around!', 50, TRUE);

-- Insert more negative reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_066', DATEADD('day', -16, CURRENT_DATE()), 1, 'customer_service', 'Customer service is non-existent. I''ve been trying to reach someone for days with no response. This is unacceptable!', 54, TRUE),
    ('REV_067', DATEADD('day', -14, CURRENT_DATE()), 2, 'product_quality', 'Very disappointed with the product quality. It looked much better in the pictures. Feels cheap and poorly made.', 33, TRUE),
    ('REV_068', DATEADD('day', -11, CURRENT_DATE()), 1, 'overall_experience', 'Terrible company! They don''t care about customers at all. Save yourself the headache and shop somewhere else.', 61, TRUE),
    ('REV_069', DATEADD('day', -9, CURRENT_DATE()), 2, 'shipping', 'Shipping was extremely slow and they provided no updates. Had to chase them down for information. Very frustrating.', 27, TRUE),
    ('REV_070', DATEADD('day', -6, CURRENT_DATE()), 1, 'customer_service', 'The worst customer service I''ve ever encountered. They were rude, unhelpful, and refused to honor their return policy. Shameful!', 48, TRUE);

-- Insert more mixed reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_071', DATEADD('day', -4, CURRENT_DATE()), 3, 'customer_service', 'Customer service was hit or miss. Some representatives were helpful, others not so much. Inconsistent experience.', 12, TRUE),
    ('REV_072', DATEADD('day', -2, CURRENT_DATE()), 3, 'overall_experience', 'It''s fine. Nothing special but they get the job done. Would shop here again if the price is competitive.', 8, TRUE),
    ('REV_073', DATEADD('day', -1, CURRENT_DATE()), 3, 'product_quality', 'Product is okay for the price. Don''t expect premium quality but it''s functional. Fair value.', 9, TRUE),
    ('REV_074', CURRENT_DATE(), 3, 'shipping', 'Shipping was standard. No issues but nothing impressive. Arrived when they said it would.', 6, TRUE),
    ('REV_075', DATEADD('day', -92, CURRENT_DATE()), 3, 'overall_experience', 'Average experience. They could improve in some areas but overall it was acceptable. No major complaints.', 11, FALSE);

-- Insert final batch of positive reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_076', DATEADD('day', -91, CURRENT_DATE()), 5, 'customer_service', 'Incredible customer service! They made me feel valued as a customer. This is the kind of service that builds loyalty!', 41, TRUE),
    ('REV_077', DATEADD('day', -88, CURRENT_DATE()), 4, 'product_quality', 'Very pleased with the product quality. Well-made and exactly as advertised. No surprises, which is refreshing!', 25, TRUE),
    ('REV_078', DATEADD('day', -83, CURRENT_DATE()), 5, 'overall_experience', 'Outstanding company in every way! From the website to the product to customer service, everything is top-tier.', 46, TRUE),
    ('REV_079', DATEADD('day', -77, CURRENT_DATE()), 4, 'shipping', 'Fast and reliable shipping. The tracking was accurate and the package arrived in great condition.', 22, TRUE),
    ('REV_080', DATEADD('day', -72, CURRENT_DATE()), 5, 'customer_service', 'The customer service team is amazing! They''re knowledgeable, patient, and genuinely want to help. Best in the business!', 37, TRUE);

-- Insert final batch of negative reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_081', DATEADD('day', -67, CURRENT_DATE()), 1, 'overall_experience', 'Do not waste your time or money with this company! Poor products, terrible service, and they don''t care about making things right.', 59, TRUE),
    ('REV_082', DATEADD('day', -62, CURRENT_DATE()), 2, 'customer_service', 'Customer service was unprofessional and unhelpful. They clearly need better training on how to treat customers.', 35, TRUE),
    ('REV_083', DATEADD('day', -57, CURRENT_DATE()), 1, 'product_quality', 'Garbage quality! The product broke immediately. This company should be ashamed of selling such junk.', 52, TRUE),
    ('REV_084', DATEADD('day', -52, CURRENT_DATE()), 2, 'shipping', 'Shipping was a nightmare. Delayed, no communication, and the package was damaged. Terrible experience.', 30, TRUE),
    ('REV_085', DATEADD('day', -47, CURRENT_DATE()), 1, 'customer_service', 'Absolutely the worst customer service! They don''t respond to emails, hang up on calls, and refuse to help. Avoid!', 46, TRUE);

-- Insert final batch of neutral and positive reviews
INSERT INTO CUSTOMER_REVIEWS (REVIEW_ID, REVIEW_DATE, RATING, REVIEW_CATEGORY, REVIEW_TEXT, HELPFUL_COUNT, VERIFIED_PURCHASE)
VALUES
    ('REV_086', DATEADD('day', -42, CURRENT_DATE()), 3, 'overall_experience', 'It''s an okay company. They have room for improvement but they''re not terrible. Middle of the pack.', 10, TRUE),
    ('REV_087', DATEADD('day', -37, CURRENT_DATE()), 5, 'customer_service', 'Customer service excellence! They resolved my issue quickly and professionally. This is how it should be done!', 43, TRUE),
    ('REV_088', DATEADD('day', -32, CURRENT_DATE()), 4, 'product_quality', 'Great product quality! Well worth the money. I''m very satisfied with my purchase and would buy again.', 26, TRUE),
    ('REV_089', DATEADD('day', -27, CURRENT_DATE()), 5, 'overall_experience', 'This company is the real deal! They deliver quality products with outstanding service. Couldn''t be happier!', 49, TRUE),
    ('REV_090', DATEADD('day', -22, CURRENT_DATE()), 4, 'shipping', 'Shipping was excellent. Fast, reliable, and well-packaged. No complaints whatsoever!', 20, TRUE),
    ('REV_091', DATEADD('day', -17, CURRENT_DATE()), 5, 'customer_service', 'The customer service team is wonderful! They''re helpful, friendly, and always go the extra mile. Highly recommend!', 38, TRUE),
    ('REV_092', DATEADD('day', -12, CURRENT_DATE()), 3, 'product_quality', 'Product is decent. Nothing special but it does what it''s supposed to do. Fair for the price.', 8, TRUE),
    ('REV_093', DATEADD('day', -7, CURRENT_DATE()), 5, 'overall_experience', 'Exceptional company! They care about quality and customer satisfaction. This is my new favorite place to shop!', 51, TRUE),
    ('REV_094', DATEADD('day', -95, CURRENT_DATE()), 4, 'customer_service', 'Very good customer service. They were patient with my questions and helped me make the right choice.', 23, TRUE),
    ('REV_095', DATEADD('day', -93, CURRENT_DATE()), 2, 'shipping', 'Shipping took way too long and there was no communication about the delay. Not happy with this aspect.', 28, FALSE),
    ('REV_096', DATEADD('day', -94, CURRENT_DATE()), 5, 'overall_experience', 'Love this company! They consistently deliver great products and service. They''ve earned a customer for life!', 45, TRUE),
    ('REV_097', DATEADD('day', -96, CURRENT_DATE()), 1, 'customer_service', 'Customer service was terrible. They were dismissive and unhelpful. I expected much better treatment.', 40, TRUE),
    ('REV_098', DATEADD('day', -97, CURRENT_DATE()), 4, 'product_quality', 'High-quality product that exceeded expectations. Very satisfied with the craftsmanship and durability.', 27, TRUE),
    ('REV_099', DATEADD('day', -98, CURRENT_DATE()), 3, 'overall_experience', 'Average experience. Some things were good, others could be better. It''s an okay company overall.', 9, FALSE),
    ('REV_100', DATEADD('day', -99, CURRENT_DATE()), 5, 'customer_service', 'Fantastic customer service! They made my shopping experience enjoyable and stress-free. Can''t recommend them enough!', 42, TRUE);

-- =============================================================================
-- COMPLETION MESSAGE
-- =============================================================================

SELECT '✅ Customer Reviews Setup Complete!' AS STATUS,
       (SELECT COUNT(*) FROM CUSTOMER_REVIEWS) AS TOTAL_REVIEWS,
       (SELECT ROUND(AVG(RATING), 2) FROM CUSTOMER_REVIEWS) AS AVERAGE_RATING;
