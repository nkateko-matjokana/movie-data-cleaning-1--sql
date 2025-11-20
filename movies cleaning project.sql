
-- MY MOVIE DATA CLEANING JOURNEY
-- From messy data to insights-ready dataset
-- This project shows how I took raw movie data with lots of issues
-- and transformed it into a clean, reliable dataset ready for analysis.


-- === FIRST, LET'S GET TO KNOW OUR DATA ===
-- Before cleaning, I wanted to understand what we're working with

-- How many movies are we dealing with?
SELECT COUNT(*) as total_movies FROM movies_raw;

-- What does the data look like?
SELECT * FROM movies_raw LIMIT 5;
DESCRIBE movies_raw;

-- Let's check for data quality issues
SELECT
    COUNT(*) as total_movies,
    -- How many are missing crucial information?
    SUM(CASE WHEN movie_title IS NULL OR movie_title = '' THEN 1 ELSE 0 END) as missing_titles,
    SUM(CASE WHEN title_year IS NULL OR title_year = '' THEN 1 ELSE 0 END) as missing_years,
    SUM(CASE WHEN imdb_score IS NULL OR imdb_score = '' THEN 1 ELSE 0 END) as missing_ratings
FROM movies_raw;

-- What's the range of our data?
SELECT 
    MIN(title_year) as oldest_movie,
    MAX(title_year) as newest_movie,
    MIN(imdb_score) as worst_rating,
    MAX(imdb_score) as best_rating,
    AVG(imdb_score) as average_rating
FROM movies_raw;

-- Looking for weird data patterns
SELECT movie_title FROM movies_raw WHERE movie_title LIKE '%?%' OR movie_title LIKE '%"%';

-- === TIME TO CLEAN THINGS UP ===
-- I'll work with a copy so we don't mess up the original data

-- Create a safe workspace (staging table)
CREATE TABLE movies_staging LIKE movies_raw;
INSERT INTO movies_staging SELECT * FROM movies_raw;

-- Let's find duplicate movies
WITH find_duplicates AS (
    SELECT *,
    ROW_NUMBER() OVER(
        PARTITION BY 
            movie_title, title_year, director_facebook_likes, 
            actor_1_facebook_likes, gross, budget, imdb_score
    ) AS duplicate_flag
    FROM movies_staging
)
SELECT * FROM find_duplicates WHERE duplicate_flag > 1;

-- Create a clean table without duplicates
CREATE TABLE movies_clean LIKE movies_staging;

INSERT INTO movies_clean
SELECT
    movie_title, duration, director_facebook_likes,
    actor_1_facebook_likes, gross, budget, title_year, imdb_score,
    num_critic_for_reviews, actor_3_facebook_likes, num_voted_users,
    cast_total_facebook_likes, facenumber_in_poster, num_user_for_reviews,
    actor_2_facebook_likes
FROM (
    SELECT *,
    ROW_NUMBER() OVER(
        PARTITION BY 
            movie_title, title_year, director_facebook_likes,
            actor_1_facebook_likes, gross, budget, imdb_score
    ) AS duplicate_flag
    FROM movies_staging
) AS numbered_movies
WHERE duplicate_flag = 1;  -- Keep only the first instance of each duplicate

-- Check our progress: how many duplicates did we remove?
SELECT 
    (SELECT COUNT(*) FROM movies_staging) as original_count,
    (SELECT COUNT(*) FROM movies_clean) as cleaned_count;

-- === FIXING DATA ISSUES ===

-- Clean up movie titles (remove those ?? marks)
UPDATE movies_clean 
SET movie_title = REPLACE(REPLACE(movie_title, '??', ''), '?', '')
WHERE movie_title LIKE '%?%';

-- Fix numbers that were stored as text (like "475")
UPDATE movies_clean 
SET director_facebook_likes = REPLACE(REPLACE(director_facebook_likes, '"', ''), '"""', '')
WHERE director_facebook_likes LIKE '%"%';

-- Convert empty values to proper NULLs
UPDATE movies_clean 
SET 
    duration = NULLIF(TRIM(duration), ''),
    director_facebook_likes = NULLIF(TRIM(director_facebook_likes), ''),
    gross = NULLIF(TRIM(gross), ''),
    budget = NULLIF(TRIM(budget), ''),
    title_year = NULLIF(TRIM(title_year), ''),
    imdb_score = NULLIF(TRIM(imdb_score), '');

-- === MAKING THE DATA MORE USEFUL ===
-- Let's add some categories that will help with analysis

-- Add columns for our new categories
ALTER TABLE movies_clean 
ADD COLUMN budget_category VARCHAR(20),
ADD COLUMN gross_category VARCHAR(20),
ADD COLUMN duration_category VARCHAR(20),
ADD COLUMN rating_category VARCHAR(20);

-- Organize movies into meaningful groups
UPDATE movies_clean 
SET 
    budget_category = CASE 
        WHEN budget IS NULL THEN 'Unknown'
        WHEN budget < 10000000 THEN 'Low Budget'
        WHEN budget BETWEEN 10000000 AND 50000000 THEN 'Medium Budget'
        WHEN budget BETWEEN 50000001 AND 100000000 THEN 'High Budget'
        ELSE 'Blockbuster Budget'
    END,
    gross_category = CASE 
        WHEN gross IS NULL THEN 'Unknown'
        WHEN gross < 50000000 THEN 'Underperformer'
        WHEN gross BETWEEN 50000000 AND 200000000 THEN 'Solid Performer'
        WHEN gross BETWEEN 200000001 AND 500000000 THEN 'Hit'
        ELSE 'Blockbuster'
    END,
    duration_category = CASE 
        WHEN duration IS NULL THEN 'Unknown'
        WHEN duration < 90 THEN 'Short Film'
        WHEN duration BETWEEN 90 AND 120 THEN 'Standard Length'
        WHEN duration BETWEEN 121 AND 150 THEN 'Long Film'
        ELSE 'Epic Length'
    END,
    rating_category = CASE 
        WHEN imdb_score IS NULL THEN 'Not Rated'
        WHEN imdb_score < 5.0 THEN 'Poor'
        WHEN imdb_score BETWEEN 5.0 AND 6.9 THEN 'Average'
        WHEN imdb_score BETWEEN 7.0 AND 7.9 THEN 'Good'
        ELSE 'Excellent'
    END;

-- === FINAL TOUCHES ===

-- Fix some data issues I noticed
UPDATE movies_clean 
SET budget = budget / 10 
WHERE budget > 1000000000;  -- Some budgets were 10x too large

-- Fill in missing runtime data
UPDATE movies_clean 
SET duration = 169, duration_category = 'Epic Length'
WHERE movie_title = 'Pirates of the Caribbean: At World''s End' 
AND duration IS NULL;

UPDATE movies_clean 
SET duration = 165, duration_category = 'Epic Length'
WHERE movie_title = 'The Dark Knight Rises' 
AND duration IS NULL;

-- === LET'S SEE OUR FINAL RESULTS ===

-- Create a nice view for analysis
CREATE OR REPLACE VIEW ready_for_analysis AS
SELECT 
    movie_title, 
    duration, 
    duration_category,
    budget, 
    budget_category,
    gross, 
    gross_category, 
    imdb_score, 
    rating_category,
    director_facebook_likes,
    title_year
FROM movies_clean;

-- How did we do?
SELECT 
    COUNT(*) as total_movies_cleaned,
    SUM(CASE WHEN duration IS NULL THEN 1 ELSE 0 END) as still_missing_runtime,
    SUM(CASE WHEN gross IS NULL THEN 1 ELSE 0 END) as still_missing_gross
FROM movies_clean;

-- Let's see our beautiful clean data!
SELECT * FROM ready_for_analysis LIMIT 10;


-- WHAT I ACCOMPLISHED:

-- Took messy movie data with duplicates, missing values, and inconsistencies
-- Cleaned and standardized everything using SQL
-- Added helpful categories for analysis
-- Ended up with a reliable dataset ready for insights

-- The data is now ready to answer questions like:
-- Do big budgets lead to better ratings?
-- What's the ideal movie length for success?
-- How do director popularity and movie success relate?






















































































