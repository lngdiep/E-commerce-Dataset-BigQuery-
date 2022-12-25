
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

SELECT 
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
  sum(totals.totalTransactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix between '0101' and '0331'
GROUP BY 1
ORDER BY 1
;

-- Query 02: Bounce rate per traffic source in July 2017

SELECT 
  trafficSource.source as source,
  sum(totals.visits) as total_visits,
  sum(totals.bounces) as total_no_of_bounces,
  sum(totals.bounces)/sum(totals.visits)*100.0 as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
GROUP BY trafficSource.source
ORDER BY total_visits desc
;

-- Query 3: Revenue by traffic source by week, by month in June 2017

with month_data as
(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)
,
week_data as
(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

SELECT * FROM month_data
UNION ALL
SELECT * FROM week_data
;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser

with purchaser as
(SELECT 
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  sum(totals.pageviews) as p_pageviews,
  count(distinct fullVisitorId) as p_num
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix between '0601' and '0731'
  and totals.transactions>=1
GROUP BY 1
)
  ,
non_purchaser as
(SELECT 
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  sum(totals.pageviews) as np_pageviews,
  count(distinct fullVisitorId) as np_num
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
WHERE _table_suffix between '0601' and '0731'
  and totals.transactions is null
GROUP BY 1
)

SELECT 
  month, 
  (p_pageviews/p_num) as avg_pageviews_purchase,
  (np_pageviews/np_num) as avg_pageviews_non_purchase
FROM purchaser
INNER JOIN non_purchaser
  using (month)
;

-- Query 05: Average number of transactions per user that made a purchase in July 2017

SELECT 
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
WHERE totals.transactions>=1
GROUP BY 1
;

-- Query 06: Average amount of money spent per session

SELECT 
    format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
    sum(totals.totalTransactionRevenue)/count(fullvisitorId) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
WHERE totals.transactions is not null
GROUP BY 1
;

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.

SELECT
  other_purchased_products,
  sum(productQuantity) as quantity
FROM
  (
  SELECT
    fullVisitorId,
    v2ProductName as other_purchased_products,
    productQuantity
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
    unnest(hits) as hits,
    unnest(hits.product) as product
  WHERE 
    fullVisitorId in
                    (SELECT 
                      fullVisitorId
                    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
                      unnest(hits) as hits,
                      unnest(hits.product) as product
                    WHERE v2ProductName="YouTube Men's Vintage Henley"
                          and productRevenue is not null)
  and productRevenue is not null
  and v2ProductName<>"YouTube Men's Vintage Henley"
  ) as temp
GROUP BY other_purchased_products
ORDER BY quantity desc
;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.

# Solution 1:
with productview as
(SELECT
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  count(v2ProductName) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest(hits) as hits,
  unnest(hits.product) as product
WHERE _table_suffix between '0101' and '0331'
  and eCommerceAction.action_type='2'
GROUP BY month)
,
productcart as
(SELECT
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  count(v2ProductName) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest(hits) as hits,
  unnest(hits.product) as product
WHERE _table_suffix between '0101' and '0331'
  and eCommerceAction.action_type='3'
GROUP BY month)
,
productpurchase as
(SELECT
  format_date('%Y%m', parse_date('%Y%m%d', date)) as month,
  count(v2ProductName) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest(hits) as hits,
  unnest(hits.product) as product
WHERE _table_suffix between '0101' and '0331'
  and eCommerceAction.action_type='6'
GROUP BY month)

SELECT
  productview.month,
  num_product_view,
  num_addtocart,
  num_purchase,
  round(num_addtocart/num_product_view*100.0,2) as add_to_cart_rate,
  round(num_purchase/num_product_view*100.0,2) as purchase_rate
FROM productview
INNER JOIN productcart 
  on productview.month = productcart.month
INNER JOIN productpurchase 
  on productcart.month = productpurchase.month
ORDER BY month
;

# Solution 2: count(case when) or sum(case when)

WITH product_data AS
(
SELECT
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    COUNT(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    COUNT(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    COUNT(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
WHERE _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
GROUP BY month
ORDER BY month
)

SELECT
    *,
    ROUND(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    ROUND(num_purchase/num_product_view * 100, 2) as purchase_rate
FROM product_data
;

