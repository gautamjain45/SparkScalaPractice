-- SQL Query to show daily timeline for advertiser budgets in April 2026

-- Assuming the table budget_changes has columns: advertiser_id (string or int), change_date (date), new_budget (int or decimal)

-- This query generates a daily timeline for each advertiser, showing the active budget on every day in April 2026,
-- even if no change occurred on that day. The budget remains the same until the next change.

WITH dates AS (
  -- Generate dates from 2026-04-01 to 2026-04-30
  SELECT DATEADD(DAY, n-1, '2026-04-01') AS date
  FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),
               (11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
               (21),(22),(23),(24),(25),(26),(27),(28),(29),(30)) AS nums(n)
),
advertisers AS (
  SELECT DISTINCT advertiser_id FROM budget_changes
),
timeline AS (
  SELECT a.advertiser_id, d.date, MAX(b.change_date) AS last_change_date
  FROM advertisers a
  CROSS JOIN dates d
  LEFT JOIN budget_changes b ON a.advertiser_id = b.advertiser_id AND b.change_date <= d.date
  GROUP BY a.advertiser_id, d.date
)
SELECT t.advertiser_id, t.date, COALESCE(b.new_budget, 0) AS active_budget
FROM timeline t
LEFT JOIN budget_changes b ON t.advertiser_id = b.advertiser_id AND t.last_change_date = b.change_date
ORDER BY t.advertiser_id, t.date;

-- Note: If there are no changes before April, the budget is assumed to be 0.
-- Adjust the COALESCE if needed. Also, this assumes SQL Server syntax for DATEADD; adjust for your database.