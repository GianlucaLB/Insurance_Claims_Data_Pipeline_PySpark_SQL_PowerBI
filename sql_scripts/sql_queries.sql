-- 1
-- Claim approval rate (approved / total)

WITH count_rate AS (
SELECT
	COUNT(*) total_claim,
	SUM(CASE WHEN claim_status = 'Approved' THEN 1 ELSE 0 END) as approved_claim
FROM fact_claims
)

SELECT
	approved_claim,
	total_claim,
	ROUND((CAST(approved_claim AS FLOAT) / total_claim) * 100 ,2)as approval_rate
FROM count_rate

-- 2
-- Average payout

SELECT
claim_status ,
ROUND(AVG(claim_amount),0) AS avg_claim_amount
FROM fact_claims
WHERE claim_amount IS NOT NULL AND claim_status != 'Pending'
group by claim_status 

-- 3
-- High-risk claims count

SELECT
	risk_category,
	COUNT(*) as total_claims
FROM fact_claims
GROUP BY risk_category
ORDER BY total_claims DESC;

-- 4
-- Average Decision Time by Claim Type & Status
SELECT
	c.claim_type,
	f.claim_status,
	avg(f.processing_time_days) avg_processing_time_days
FROM fact_claims f
LEFT JOIN dim_claim_type c
ON f.claim_type_id = c.claim_type_id
WHERE f.claim_status != 'Pending'
GROUP BY c.claim_type,
		 f.claim_status
ORDER BY claim_type , avg_processing_time_days DESC;

-- 5
-- Monthly claims trends

SELECT
	COUNT(*) as total_claims,
	FORMAT(claim_date , 'MMM-yyyy') as 'Month-Year'
FROM fact_claims
GROUP BY YEAR(claim_date) ,MONTH(claim_date) , FORMAT(claim_date , 'MMM-yyyy')
ORDER BY YEAR(claim_date) ,MONTH(claim_date)
