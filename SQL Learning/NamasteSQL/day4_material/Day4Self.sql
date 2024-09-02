update orders set city = NULL where order_id in ('CA-2020-161389' , 'US-2021-156909')

select * from orders where city is null

3- write a query to get total profit, first order date and latest order date for each category

select sub_category, sum(profit) as total_profit, min(order_date) as first_date, max(order_date) as latest_date
from orders
group by sub_category;

4- write a query to find sub-categories where average profit is more than the half of the max profit in that sub-category

select sub_category
from orders
group by sub_category
having AVG(profit) > max(profit)/2

5- create the exams table with below script;

create table exams (student_id int, subject varchar(20), marks int);
insert into exams values (1,'Chemistry',91),(1,'Physics',91),(1,'Maths',92)
,(2,'Chemistry',80),(2,'Physics',90)
,(3,'Chemistry',80),(3,'Maths',80)
,(4,'Chemistry',71),(4,'Physics',54)
,(5,'Chemistry',79);

write a query to find students who have got same marks in Physics and Chemistry.

select student_id, marks from exams
where subject in ('Physics', 'Chemistry')
group by student_id, marks
having count(1) = count(2)

select student_id , marks
from exams
where subject in ('Physics','Chemistry')
group by student_id , marks
having count(1)=2;

select category, count(distinct product_id) as no_of_products
from orders
group by category

select * from orders

SELECT category, count(distinct product_id) as total_products from orders
group by category

select top 5 sub_category, sum(quantity) as total_quantity
from orders
where region = 'West'
group by sub_category
order by total_quantity desc;

SELECT region, ship_mode, sum(sales) as total_sales
FROM orders
where order_date BETWEEN '2020-01-01' and '2020-12-31'
group by region, ship_mode

UPDATE orders
SET order_date = CONVERT(varchar, order_date, 23);

SELECT student_id, marks,count(1) as total_count, count(distinct marks) as distinct_marks
from exams
where subject in ('Physics', 'Chemistry')
group by student_id, marks
order by student_id

SELECT student_id, marks,count(1) as total_count, count(distinct marks) as distinct_marks
from exams
where subject in ('Physics', 'Chemistry')
group by student_id, marks
having count(*) = 2
