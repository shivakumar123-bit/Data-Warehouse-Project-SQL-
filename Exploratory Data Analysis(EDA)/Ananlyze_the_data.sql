/*
=========================
Exploratory Data Analysis
=========================
*/
select * from INFORMATION_SCHEMA.TABLES
---------------------------------------
  
select * from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = 'dim_customers'
-----------------------------------------------
  
select distinct country from gold.dim_customers;
-----------------------------------------------

select distinct category, sub_category, product_name 
from gold.dim_proudcts
order by 1,2,3;
---------------------------------------------------

select min(sales_order) as min_order_date,
max(sales_order) as max_order_date,
datediff(YEAR,min(sales_order),max(sales_order)) as Difference
from gold.fact_sales;
--------------------------------------------------------------

select 
min(birth_date) as old_customer,
datediff(year,min(birth_date),getdate()) as age_of_old,
max(birth_date) as young_customer,
datediff(year,max(birth_date),getdate()) as age_of_young
from gold.dim_customers
-------------------------------------------------
  
select 'total_sales' as measure_name,
sum(sales_amount) as measure_value 
from gold.fact_sales
union all
select 'total_quantity' as measure_name,
sum(quantity) as measure_value 
from gold.fact_sales
union all
select 'avg_price' as measure_name,
avg(price) as measure_value 
from gold.fact_sales
union all
select 'total_orders' as measure_name,
count(distinct order_number) as measure_value 
from gold.fact_sales
union all
select 'total_products' as measure_name,
count(product_key) as measure_value 
from gold.dim_proudcts
union all
select 'total_customers' as measure_name,
count(distinct customer_key) as measure_value
from gold.fact_sales
---------------------------------------------------
  
select country,
count(customer_key) as total_customers
from gold.dim_customers
group by country
order by total_customers desc
--------------------------------------------------
  
select gender,
count(customer_key) as total_customers
from gold.dim_customers
group by gender
order by total_customers desc

-----------------------------------------
select category,
count(product_name) as total_products
from gold.dim_proudcts
group by category
order by total_products desc
-----------------------------------------
  
select category,
avg(product_cost) as avg_cost
from gold.dim_proudcts
group by category
order by avg_cost desc
------------------------------------------
  
select
c.customer_id,
c.first_name,
c.last_name,
sum(f.sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by customer_id,
c.first_name,
c.last_name
order by total_sales desc
---------------------------------------
  
select
c.country,
sum(f.quantity) as number_of_sales
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.country
order by number_of_sales desc
------------------------------------------
select top 5
p.product_name,
sum(f.sales_amount) as revenue
from gold.fact_sales f
left join gold.dim_proudcts p
on p.product_key = f.product_key
group by p.product_name
order by revenue desc
----------------------------------------
  
select top 10
c.customer_key,
c.first_name,
c.last_name,
sum(f.sales_amount) as revenue
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key,
c.first_name,
c.last_name
order by revenue desc
------------------------------------
  
select top 3
c.customer_key,
c.first_name,
c.last_name,
count(distinct f.order_number) as orders
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by c.customer_key,
c.first_name,
c.last_name
order by orders
