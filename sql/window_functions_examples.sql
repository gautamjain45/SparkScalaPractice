-- ============================================================================
-- WINDOW FUNCTIONS IN SQL - Examples and Explanations
-- ============================================================================

-- Sample Data: Employee Salary Table
-- CREATE TABLE employees (
--   emp_id INT,
--   emp_name VARCHAR(50),
--   department VARCHAR(50),
--   salary INT,
--   hire_date DATE
-- );

-- INSERT INTO employees VALUES
-- (1, 'Gautam Jain', 'Trade Surveillance', 10000, '2020-01-15'),
-- (2, 'Maheep Gupta', 'Startup', 12000, '2019-06-20'),
-- (3, 'Ankita Sharma', 'IOS', 8000, '2021-03-10'),
-- (4, 'Parth Jangir', 'IOS', 9000, '2020-11-05'),
-- (5, 'Rajesh Kumar', 'Trade Surveillance', 11000, '2018-02-14'),
-- (6, 'Priya Singh', 'Startup', 13000, '2021-07-01'),
-- (7, 'Amit Patel', 'IOS', 8500, '2022-01-20');


-- ============================================================================
-- 1. LAG() - Access previous row's data in ordered window
-- ============================================================================
-- Explanation:
-- LAG(column, offset, default_value) OVER (PARTITION BY ... ORDER BY ...)
-- - Returns value from a previous row (default offset = 1)
-- - Useful for comparing current row with previous row (e.g., YoY growth, trend analysis)
-- - Default value is used if no previous row exists

-- Example: Get each employee's salary and their previous employee's salary
-- (ordered by salary within their department)
SELECT 
    emp_id,
    emp_name,
    department,
    salary,
    LAG(salary, 1, 0) OVER (PARTITION BY department ORDER BY salary) AS prev_salary,
    salary - LAG(salary, 1, 0) OVER (PARTITION BY department ORDER BY salary) AS salary_diff
FROM employees
ORDER BY department, salary;

-- Output Explanation:
-- For each department, it shows the current salary, the previous employee's salary 
-- (ordered by salary), and the difference. The first employee in each department 
-- has a prev_salary of 0.

-- More Example: Month-over-Month Sales Change
-- SELECT 
--     month,
--     sales,
--     LAG(sales) OVER (ORDER BY month) AS prev_month_sales,
--     sales - LAG(sales) OVER (ORDER BY month) AS mom_change
-- FROM monthly_sales;


-- ============================================================================
-- 2. LEAD() - Access next row's data in ordered window
-- ============================================================================
-- Explanation:
-- LEAD(column, offset, default_value) OVER (PARTITION BY ... ORDER BY ...)
-- - Returns value from a next/future row (default offset = 1)
-- - Opposite of LAG()
-- - Useful for looking ahead (e.g., upcoming salary increase, next month forecast)

-- Example: Get each employee's salary and their next employee's salary
-- (ordered by salary within their department)
SELECT 
    emp_id,
    emp_name,
    department,
    salary,
    LEAD(salary, 1, 0) OVER (PARTITION BY department ORDER BY salary) AS next_salary,
    LEAD(salary, 1, 0) OVER (PARTITION BY department ORDER BY salary) - salary AS next_salary_diff
FROM employees
ORDER BY department, salary;

-- Output Explanation:
-- Shows current salary and the next employee's salary (ordered by salary). 
-- The last employee in each department has next_salary of 0.

-- Real-world Example: Predicting if employee will get a raise
-- SELECT 
--     emp_name,
--     salary,
--     LEAD(salary) OVER (PARTITION BY emp_id ORDER BY year) AS next_year_salary,
--     CASE 
--         WHEN LEAD(salary) OVER (PARTITION BY emp_id ORDER BY year) > salary 
--         THEN 'Will get raise'
--         ELSE 'No raise'
--     END AS prediction
-- FROM employee_salary_history;


-- ============================================================================
-- 3. RANK() - Rank with gaps for ties
-- ============================================================================
-- Explanation:
-- RANK() OVER (PARTITION BY ... ORDER BY ...)
-- - Assigns rank to rows, with ties getting the same rank
-- - SKIPS ranks after ties (e.g., 1, 2, 2, 4 instead of 1, 2, 2, 3)
-- - Useful when you need to identify competitors tied at same level

-- Example: Rank employees by salary within each department
SELECT 
    emp_id,
    emp_name,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_rank,
    RANK() OVER (ORDER BY salary DESC) AS company_wide_rank
FROM employees
ORDER BY department, salary_rank;

-- Output Explanation:
-- salary_rank = ranking within department (highest salary = rank 1)
-- company_wide_rank = ranking across entire company
-- If two employees have same salary, they get same rank, and next rank is skipped.
-- Example: Rank 1, 2, 2, 4 (not 1, 2, 2, 3)

-- Real-world Example: Sports leaderboard
-- SELECT 
--     player_name,
--     game,
--     score,
--     RANK() OVER (PARTITION BY game ORDER BY score DESC) AS leaderboard_rank
-- FROM game_scores;


-- ============================================================================
-- 4. DENSE_RANK() - Rank without gaps for ties
-- ============================================================================
-- Explanation:
-- DENSE_RANK() OVER (PARTITION BY ... ORDER BY ...)
-- - Similar to RANK() but WITHOUT gaps
-- - Ties get same rank, next rank is consecutive (e.g., 1, 2, 2, 3 not 1, 2, 2, 4)
-- - Useful for continuous ranking (like movie ratings, review counts)

-- Example: Dense rank employees by salary within each department
SELECT 
    emp_id,
    emp_name,
    department,
    salary,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS salary_dense_rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS company_wide_dense_rank
FROM employees
ORDER BY department, salary_dense_rank;

-- Output Explanation:
-- Similar to RANK() but consecutive numbering.
-- If ranks are 1, 2, 2, then next is 3 (not 4 like in RANK())

-- Comparison: RANK() vs DENSE_RANK()
SELECT 
    emp_id,
    emp_name,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS rank_with_gaps,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS rank_no_gaps
FROM employees
ORDER BY salary DESC;

-- If two employees have 10000 salary:
-- RANK()       -> 1, 1, 3, 4, 5
-- DENSE_RANK() -> 1, 1, 2, 3, 4


-- ============================================================================
-- 5. SUM() OVER(PARTITION BY ...) - Running total / Aggregate by group
-- ============================================================================
-- Explanation:
-- SUM(column) OVER (PARTITION BY ... [ORDER BY ...])
-- - Aggregates values by partition, but keeps all rows (doesn't collapse)
-- - Without ORDER BY: gets total for entire partition for each row
-- - With ORDER BY: can create running totals (rows unbounded preceding to current)
-- - Part of aggregate window functions (also includes AVG, COUNT, MIN, MAX)

-- Example 1: Total salary expense per department
SELECT 
    emp_id,
    emp_name,
    department,
    salary,
    SUM(salary) OVER (PARTITION BY department) AS dept_total_salary,
    ROUND(100.0 * salary / SUM(salary) OVER (PARTITION BY department), 2) AS pct_of_dept
FROM employees
ORDER BY department, salary DESC;

-- Output Explanation:
-- dept_total_salary = sum of all salaries in that department (same for all in dept)
-- pct_of_dept = this employee's salary as % of their department total

-- Example 2: Running total of salaries by hire date
SELECT 
    emp_id,
    emp_name,
    hire_date,
    salary,
    SUM(salary) OVER (ORDER BY hire_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_total
FROM employees
ORDER BY hire_date;

-- Output Explanation:
-- running_salary_total = cumulative sum from first row to current row
-- First row: 10000
-- Second row: 10000 + 12000 = 22000
-- Third row: 22000 + 8000 = 30000, etc.

-- Example 3: Month-to-date sales (running total within month)
-- SELECT 
--     order_date,
--     amount,
--     SUM(amount) OVER (
--         PARTITION BY YEAR(order_date), MONTH(order_date) 
--         ORDER BY order_date 
--         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
--     ) AS mtd_sales
-- FROM orders;

-- Example 4: Department salary percentage of company total
SELECT 
    emp_id,
    emp_name,
    department,
    salary,
    SUM(salary) OVER (PARTITION BY department) AS dept_salary_total,
    SUM(salary) OVER () AS company_salary_total,
    ROUND(100.0 * SUM(salary) OVER (PARTITION BY department) / SUM(salary) OVER (), 2) AS dept_pct_of_company
FROM employees
ORDER BY department;


-- ============================================================================
-- COMBINED EXAMPLE: All window functions together
-- ============================================================================

SELECT 
    emp_id,
    emp_name,
    department,
    salary,
    hire_date,
    
    -- LAG: Previous employee's salary in department
    LAG(salary) OVER (PARTITION BY department ORDER BY salary DESC) AS prev_higher_salary,
    
    -- LEAD: Next employee's salary in department
    LEAD(salary) OVER (PARTITION BY department ORDER BY salary DESC) AS next_lower_salary,
    
    -- RANK: Salary rank with gaps
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dept_salary_rank,
    
    -- DENSE_RANK: Salary rank without gaps
    DENSE_RANK() OVER (ORDER BY salary DESC) AS company_salary_dense_rank,
    
    -- SUM: Department salary total and percentage
    SUM(salary) OVER (PARTITION BY department) AS dept_total_salary,
    ROUND(100.0 * salary / SUM(salary) OVER (PARTITION BY department), 2) AS pct_of_dept_salary
    
FROM employees
ORDER BY department, salary DESC;


-- ============================================================================
-- PRACTICAL USE CASES
-- ============================================================================

-- 1. Find top N records from each group
-- Example: Top 2 highest paid employees per department
SELECT * FROM (
    SELECT 
        emp_name,
        department,
        salary,
        DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
    FROM employees
) ranked_employees
WHERE rank <= 2
ORDER BY department, rank;


-- 2. Identify salary outliers (big jump from previous hire)
SELECT 
    emp_name,
    department,
    salary,
    LAG(salary) OVER (PARTITION BY department ORDER BY hire_date) AS prev_hire_salary,
    salary - LAG(salary) OVER (PARTITION BY department ORDER BY hire_date) AS salary_increase,
    CASE 
        WHEN salary - LAG(salary) OVER (PARTITION BY department ORDER BY hire_date) > 2000 
        THEN 'Significant increase'
        ELSE 'Normal'
    END AS change_type
FROM employees
ORDER BY department;


-- 3. Month-over-Month comparison
-- SELECT 
--     MONTH(sale_date) AS month,
--     SUM(amount) AS monthly_sales,
--     LAG(SUM(amount)) OVER (ORDER BY MONTH(sale_date)) AS prev_month_sales,
--     SUM(amount) - LAG(SUM(amount)) OVER (ORDER BY MONTH(sale_date)) AS growth
-- FROM sales
-- GROUP BY MONTH(sale_date);


-- 4. Identify consecutive low performers
-- SELECT 
--     emp_name,
--     review_date,
--     rating,
--     LAG(rating) OVER (PARTITION BY emp_id ORDER BY review_date) AS prev_rating,
--     LEAD(rating) OVER (PARTITION BY emp_id ORDER BY review_date) AS next_rating
-- FROM employee_reviews
-- WHERE rating < 3 OR LAG(rating) < 3 OR LEAD(rating) < 3;
