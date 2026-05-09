/*Amazon Ads needs to monitor advertisers in real-time. We want to identify the exact moment an advertiser crosses a certain spend threshold so we can trigger a notification.

Task:
Write a SQL query to find the earliest timestamp on '2026-05-01' when each advertiser's cumulative spend first crossed $1,000.

Your result should include:

advertiser_id

The timestamp of the transaction that pushed them over the limit.

The cumulative_spend at that specific moment.*/

/*Sample table : ad_spend_events
advertiser_id,timestamp,spend_amount
101,2026-05-01 08:00:01,450.00
102,2026-05-01 08:05:00,900.00
101,2026-05-01 09:15:20,500.00
103,2026-05-01 10:00:00,1200.00
101,2026-05-01 10:30:00,100.00
102,2026-05-01 11:00:00,200.00
104,2026-05-01 12:00:00,500.00 */

WITH cumulative_data AS (
    select advertiser_id, timestamp, spend_amount,
           SUM(spend_amount) OVER (PARTITION BY advertiser_id ORDER BY timestamp) AS running_total
    from ad_spend_events
    where cast(timestamp as date) = '2026-05-01'
),
threshold_crossed AS (
    select *, 
    ROW_NUMBER() OVER (
        PARTITION BY advertiser_id 
        ORDER BY timestamp
    ) as occurrence_rank
    from cumulative_data
    where running_total > 1000
)
SELECT advertiser_id, timestamp, running_total AS cumulative_spend
FROM threshold_crossed
WHERE occurrence_rank = 1;