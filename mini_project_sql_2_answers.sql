create database minisql2;
use minisql2;

# checking tables
select * from cust_dimen;
select * from orders_dimen;
select * from market_fact;
select * from prod_dimen;
select * from shipping_dimen;

# 1.	Join all the tables and create a new table called 
# combined_table. (market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

create  table Combined_table as
select mf.ord_id,mf.prod_id,mf.ship_id,mf.cust_id,sales,discount,order_quantity,profit,
shipping_cost,product_base_margin,od.order_id,order_date,order_priority,product_category,product_sub_category
,ship_mode,ship_date,customer_name,province,region,customer_segment
from market_fact mf join cust_dimen using (cust_id)
join prod_dimen using (prod_id) join shipping_dimen sd using(ship_id)
join orders_dimen od using(order_id);

select * from combined_table;



# 2.	Find the top 3 customers who have the maximum number of orders

select * from (
select customer_name,cust_id,total_orders,dense_rank() over(order by total_orders desc) top_orders from
(select customer_name ,cust_id,count(*) total_orders from combined_table
group by cust_id,customer_name
order by  count(*) desc) t1) t2
where top_orders<4;

# 3.Create a new column DaysTakenForDelivery that 
# contains the date difference of Order_Date and Ship_Date.

select ord_id,customer_name,order_date,ship_date, datediff(ship_date_new,order_date_new) DaysTakenforDelivery from 
(select ord_id,prod_id,ship_id,cust_id,customer_name,order_date,concat_ws("-",substr(order_date,7,4),substr(order_date,4,2),substr(order_date,1,2)) order_date_new,ship_date,concat_ws("-",substr(ship_date,7,4),substr(ship_date,4,2),substr(ship_date,1,2)) ship_date_new 
from combined_table) t1 ;


# 4.Find the customer whose order took the maximum time to get delivered.

select * from 
(select ord_id,ship_id,cust_id,customer_name,order_date,ship_date, datediff(ship_date_new,order_date_new) DaysTakenforDelivery from 
(select ord_id,prod_id,ship_id,cust_id,customer_name,order_date,concat_ws("-",substr(order_date,7,4),substr(order_date,4,2),substr(order_date,1,2)) order_date_new,ship_date,concat_ws("-",substr(ship_date,7,4),substr(ship_date,4,2),substr(ship_date,1,2)) ship_date_new 
from combined_table) t1) t2
order by daystakenfordelivery desc limit 1;


# 5.Retrieve total sales made by each product 
#from the data (use Windows function)

select distinct prod_id,
round(sum(sales) over(partition by prod_id ),2) total_sales 
from combined_table;


#6.Retrieve total profit made from each product 
# from the data (use windows function)

select distinct prod_id,
round(sum(profit) over(partition by prod_id ),2) total_profit 
from combined_table;

#7.	Count the total number of unique customers in January and 
# how many of them came back every month over the entire year in 2011
# changing date format
create or replace view combined_table_n as
select *, concat_ws("-",substr(order_date,7,4),substr(order_date,4,2),
substr(order_date,1,2)) order_date_n from combined_table;

select * from combined_table_n;

# Answer
select distinct year(order_date_n) year ,month(order_date_n) month, count(cust_id)
over(partition by month(order_date_n) order by month(order_date_n)) as unique_customers
from combined_table_n
where year(order_date_n)=2011 and month(order_date_n)>1 and cust_id
in
(select cust_id from combined_table_n
where month(order_date_n)=1
and year(order_date_n)=2011);

#8.	Retrieve month-by-month customer retention rate since the start of the business.
# (using views)

#step 1
# creating a view
 
create or replace view visit_log as
select cust_id,timestampdiff(month,(select min(order_date_n) 
from combined_table_n),order_date_n) as visit_month
from combined_table_n
order by cust_id,visit_month;

select * from visit_log;


# identifying time lapse

create or replace view time_lapse as
select distinct cust_id, visit_month,lead(visit_month,1)
over(partition by cust_id order by cust_id,visit_month) leading_by
from visit_log;

select * from time_lapse;

#step 3 calculatinff difference between visits
create or replace view time_lapsed as
select cust_id,visit_month,leading_by,
leading_by-visit_month as time_difference
from time_lapse;

select * from time_lapsed;


#step 4
create or replace view cust_category as
select cust_id,visit_month,
case 
when time_difference <=1 then "retained"
when time_difference >1 then "ïrregular"
when time_difference is null then "churned"
end as customer_category
from time_lapsed;

select * from cust_category;


# step 5 month-by-month retention rate
select visit_month,((count(if(customer_category="retained",1,null)))/count(cust_id))*100 as retention_rate_percentage
from cust_category group by visit_month order by visit_month asc;