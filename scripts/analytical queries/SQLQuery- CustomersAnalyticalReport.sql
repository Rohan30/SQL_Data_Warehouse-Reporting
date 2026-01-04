/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================

--  1. Gathers essential fields such as names, ages, and transaction details.
-- creating a base cte
with base_cte as
(
select s.order_number,s.product_key,
s.order_date,s.sales_amount,s.quantity,
c.customer_key,c.customer_number,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
DATEDIFF(year,c.birth_date,GETDATE()) as customer_age
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key=c.customer_key
where s.order_date IS NOT NULL),
-- 3.developing data for customer aggregations and aggregation metrics
customer_aggregation as
(
select 
customer_key,customer_number,customer_name,customer_age,
COUNT( DISTINCT(order_number)) as total_orders,
SUM(sales_amount) as total_sales,
SUM(quantity) as total_quantity,
COUNT(DISTINCT(product_key)) as total_products,
MAX(order_date) as last_order,
DATEDIFF(month,MIN(order_date),MAX(order_date)) as lifespan
from base_cte
group by customer_key,customer_number,customer_name,customer_age)
select 
customer_key,customer_number,customer_name,customer_age,
-- 2.creating customer age groups
CASE WHEN customer_age<20 then 'Under 20'
WHEN customer_age between 20 and 29 then '20-29'
WHEN customer_age between 30 and 39 then '30-39'
WHEN customer_age between 40 and 49 then '40-49'
ELSE '50 and above' end as age_group,
--2.creating customer segments
CASE WHEN lifespan>=12 and total_sales>5000 then 'VIP'
WHEN lifespan>=12 and total_sales<=5000 then 'Regular'
ELSE 'New' end as cust_segment,
total_orders,total_sales,total_quantity,total_products,
last_order,lifespan,
---4. creating the rem KPIs
-- calculating recency KPI
DATEDIFF(month,last_order,GETDATE()) as recency,
-- calculating average order value (sales/orders)
CASE WHEN total_sales=0 then 0
ELSE total_sales/total_orders END AS avg_order_value,
-- calculating average monthly spend (sales/months or sales/lifespan)
CASE WHEN lifespan=0 then 0
ELSE total_sales/lifespan END AS avg_monthly_spend
from customer_aggregation;


--- CREATING A VIEW FROM THIS DATA
--SO THAT IN CAN BE USED FOR REPORTING/FURTHER ANALYSIS
CREATE VIEW gold.customers_report AS
with base_cte as
(
select s.order_number,s.product_key,
s.order_date,s.sales_amount,s.quantity,
c.customer_key,c.customer_number,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
DATEDIFF(year,c.birth_date,GETDATE()) as customer_age
from gold.fact_sales s
left join gold.dim_customers c
on s.customer_key=c.customer_key
where s.order_date IS NOT NULL),
-- 3.developing data for customer aggregations and aggregation metrics
customer_aggregation as
(
select 
customer_key,customer_number,customer_name,customer_age,
COUNT( DISTINCT(order_number)) as total_orders,
SUM(sales_amount) as total_sales,
SUM(quantity) as total_quantity,
COUNT(DISTINCT(product_key)) as total_products,
MAX(order_date) as last_order,
DATEDIFF(month,MIN(order_date),MAX(order_date)) as lifespan
from base_cte
group by customer_key,customer_number,customer_name,customer_age)
select 
customer_key,customer_number,customer_name,customer_age,
-- 2.creating customer age groups
CASE WHEN customer_age<20 then 'Under 20'
WHEN customer_age between 20 and 29 then '20-29'
WHEN customer_age between 30 and 39 then '30-39'
WHEN customer_age between 40 and 49 then '40-49'
ELSE '50 and above' end as age_group,
--2.creating customer segments
CASE WHEN lifespan>=12 and total_sales>5000 then 'VIP'
WHEN lifespan>=12 and total_sales<=5000 then 'Regular'
ELSE 'New' end as cust_segment,
total_orders,total_sales,total_quantity,total_products,
last_order,lifespan,
---4. creating the rem KPIs
-- calculating recency KPI
DATEDIFF(month,last_order,GETDATE()) as recency,
-- calculating average order value (sales/orders)
CASE WHEN total_sales=0 then 0
ELSE total_sales/total_orders END AS avg_order_value,
-- calculating average monthly spend (sales/months or sales/lifespan)
CASE WHEN lifespan=0 then 0
ELSE total_sales/lifespan END AS avg_monthly_spend
from customer_aggregation;

-- DOING FURTHER ANALYSIS ON REPORT
select age_group, count(customer_number) as tot_customers,
sum(total_sales) as total_sales
from gold.customers_report
group by age_group;

