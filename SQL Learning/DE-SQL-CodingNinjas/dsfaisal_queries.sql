select * from students;
select * from countries;
select * from weather;


-- Select all data from Person table
SELECT *, left(phone_number, 3) as country_code
FROM Person;

-- Select all data from Country table
SELECT * FROM Country;

-- Select all data from Calls table
SELECT * FROM Calls;

with combined as
(
    select *
    from (
            select * from
                    (
                    select caller_id, duration
                    from calls
                    ) caller

            union all

            select * from
                    (
                    select callee_id, duration
                    from calls
                    ) callee
            ) combined
),
country_average as
(
select co.name as country_name, avg(cb.duration) as country_average
from combined cb
left join (select *, left(phone_number, 3) as country_code from person) p on cb.caller_id = p.id
left join country co on co.country_code = p.country_code
group by co.name
)
select country_name as country from country_average
where country_average > (select avg(duration) from combined);


SELECT o.customer_id, name
from orders o
join product p
ON o.product_id = p.product_id
JOIN Customers c
ON o.customer_id = c.customer_id
GROUP BY 1, 2
HAVING SUM(CASE WHEN date_format(order_date, '%Y-%m')='2020-06'
THEN price*quantity END) >= 100
AND
SUM(CASE WHEN date_format(order_date, '%Y-%m')='2020-07'
THEN price*quantity END) >= 100;


SELECT * FROM Orders o
join Customers c on o.customer_id = c.customer_id;

select name as customer_name, o.customer_id, order_id, order_date
from
(SELECT *, row_number() over (partition by customer_id order by order_date desc) rn
FROM Orders) o
join Customers c on o.customer_id = c.customer_id
where rn <= 3
ORDER BY customer_name, customer_id, order_date DESC;



select * from products;











select distinct c.customer_id, customer_name
from Customers c
where not exists
	(select 1 from orders o
		where c.customer_id = o.customer_id
		and product_name = 'C'
        )
and c.customer_id in 
	(select customer_id from orders 
		where product_name in ('A', 'B') 
        group by customer_id 
        having count(distinct product_name) = 2
        );


SELECT DISTINCT c.customer_id, c.customer_name
FROM Customers c
JOIN Orders o1 ON c.customer_id = o1.customer_id AND o1.product_name = 'A'
JOIN Orders o2 ON c.customer_id = o2.customer_id AND o2.product_name = 'B'
WHERE NOT EXISTS 
    (SELECT 1 FROM Orders o3
     WHERE o3.customer_id = c.customer_id
       AND o3.product_name = 'C');






SELECT stock_name,
       SUM(CASE WHEN operation = 'Buy' THEN -price ELSE price END) AS capital_gain_loss
FROM Stocks
GROUP BY stock_name;

















SELECT * FROM NPV;
select * from accounts;
select * from logins;


select id
from
(
select *, lead(login_date, 4) over (partition by id order by login_date) as day5
from (select distinct * from logins) a
) b
where date_add(login_date, interval 4 day) = day5;

with max_salary as
(
SELECT company_id, max(salary) as tax_salary FROM salaries group by company_id
)
select s.company_id, employee_id, employee_name,
	round(case when tax_salary between 1000 and 10000 then salary - (24/100 * salary)
		when tax_salary > 10000 then salary - (49/100 * salary)
        else salary
	end) as salary
from salaries s
join max_salary ms on s.company_id = ms.company_id;




select * FROM TVProgram;
select * FROM Content;


select distinct title
from Content c
join TVProgram p using content_id
where c.Kids_content = 'Y'
and c.content_type = 'Movies'
and date_format(program_Date, '%Y-%m') = '2020-06';



select * FROM variables;
select * from Expressions;

-- Drop the Sales table if it exists
select * from sales;


select s1.sale_date, first(s1.sold_num - s2.sold_num)
from sales s1
join sales s2
on s1.sale_date = s2.sale_date and s1.fruit <> s2.fruit
group by s1.sale_date;


select sale_date, sum(case when fruit = 'oranges' then -sold_num else sold_num end) as diff
from sales
group by sale_date;


select * from sales;


SELECT TRIM(LOWER(product_name)) AS product_name,
       DATE_FORMAT(sale_date, '%Y-%m') AS sale_date,
       COUNT(*) AS total
FROM Sales
GROUP BY 1, DATE_FORMAT(sale_date, '%Y-%m')
ORDER BY 1, 2;





























SELECT * FROM Transactions;
SELECT * FROM visits;


SELECT customer_id, count(v.visit_id) count_no_trans
FROM visits V
left join transactions t on v.visit_id = t.visit_id
where transaction_id is null
group by 1
order by 2 desc;







SELECT date_format(order_date, '%Y-%m') as month, 
		count(distinct order_id) as order_count,
		count(distinct customer_id) as customer_count
from orders
where invoice > 20
group by 1;






select name as warehouse_name, sum(units * width * length * height) volume
from warehouse w
left join products p on w.product_id = p.product_id
group by 1;






select name, balance
from users u
left join 
	(select account, sum(amount) as balance from transactions group by account) t
on u.account = t.account
where balance > 10000;


select o.customer_id, o.product_id, p.product_name
from
	(
    SELECT customer_id, product_id, count(product_id),
		rank() over (partition by customer_id order by count(product_id) desc) rk
	FROM ORDERS O group by 1, 2
    ) o
join products p on o.product_id = p.product_id
where rk = 1
order by 1, 2;








INSERT INTO Customer (customer_id, customer_name)
VALUES
    (1, 'Alice'),
    (4, 'Bob'),
    (5, 'Charlie');

SELECT * FROM Customer;

with recursive cte as
(
	select 1 as ids, max(customer_id) as 'Max_Id'
    from Customer
    union all
    select ids + 1, Max_Id
    from cte
    where ids < Max_Id
)
select ids 
from cte c
where ids not in (select customer_id from customer);
    




























-- Drop existing tables if they exist
SELECT * FROM  SchoolA;
SELECT * FROM  SchoolB;
SELECT * FROM  SchoolC;




SELECT a.student_name AS 'member_A',
b.student_name AS 'member_B',
c.student_name AS 'member_C'
FROM  SchoolA a, SchoolB b, SchoolC c
WHERE a.student_name <> b.student_name 
and b.student_name <> c.student_name
and a.student_name <> c.student_name
and a.student_id <> b.student_id
and a.student_id <> c.student_id
and b.student_id <> c.student_id;


select a1.machine_id, round(avg(a1.timestamp - a2.timestamp),3) as processing_time
from activity a1
join activity a2
on a1.machine_id = a2.machine_id
and a1.process_id = a2.process_id
where a1.activity_type = 'end' and a2.activity_type = 'start'
group by a1.machine_id;







select concat(upper(left(name, 1)), lower(substring(name, 2))) as name 
from users;
















select * from product;


select p.name, sum(rest) as rest, sum(paid) as paid, sum(canceled) as canceled,
		sum(refunded) as refunded
from invoice i
join product p on i.product_id = p.product_id
group by 1
order by 1;




select from_id as person1, to_id as person2, sum(duration)
from calls c1
where from_id < to_id
group by 1,2

union

select to_id as person1, from_id as person2, sum(duration)
from calls c1
where from_id < to_id
group by 1,2;


select person1, person2, count(duration) as call_count, sum(duration) as total_duration
from
(select to_id as person1, from_id as person2, duration
from calls
union all
select from_id as person1, to_id as person2, duration
from calls) c
where person1 < person2
group by 1, 2
order by 1, 2;

select user_id, max(diff) as biggest_window
from
(select user_id, datediff(coalesce(lead(visit_date) over (partition by user_id order by visit_date), '2021-01-1') , visit_date) as diff
from UserVisits) t
group by 1
order by 1;




select case when grade in ('A', 'A+') then 4 else 1 end as grade
	,sum(credits) 
from semestergrades
group by 1;


select * from semestergrades;

SELECT 
    SUM(credits * 
        CASE 
            WHEN grade IN ('A+', 'A') THEN 4.0
            WHEN grade = 'A-' THEN 3.7
            WHEN grade = 'B+' THEN 3.3
            WHEN grade = 'B' THEN 3.0
            WHEN grade = 'B-' THEN 2.7
            WHEN grade = 'C+' THEN 2.3
            WHEN grade = 'C' THEN 2.0
            WHEN grade = 'C-' THEN 1.7
            WHEN grade = 'D' THEN 1.0
            ELSE 0.0
        END
    ) / SUM(credits) AS GPA
FROM 
    SemesterGrades
WHERE 
    credits > 0; -- Exclude subjects with no credits
    
    
    SELECT 
    SUM(credits * 
        CASE 
            WHEN grade IN ('A+', 'A', 'A-') THEN 4.0
            WHEN grade IN ('B+', 'B', 'B-') THEN 3.0
            WHEN grade IN ('C+', 'C', 'C-') THEN 2.0
            WHEN grade IN ('D', 'F') THEN 0.0
            ELSE 0.0
        END
    ) / SUM(credits) AS GPA
FROM 
    SemesterGrades
WHERE 
    credits > 0; -- Exclude subjects with no credits
    
    
    
select * from SemesterGrades;








select sum( b.apple_count + ifnull(c.apple_count, 0) ) as apple_count, sum( b.orange_count + ifnull(c.orange_count, 0) ) as orange_count
from Boxes b 
left join chests c on b.chest_id = c.chest_id;





-- Drop tables if they already exist
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS orders;

-- Create the products table
CREATE TABLE products (
    product_id INT,
    price_date DATE,
    price INT
);

-- Insert data into the products table
INSERT INTO products (product_id, price_date, price) VALUES
(100, '2024-01-01', 150),
(100, '2024-01-21', 170),
(100, '2024-02-01', 190),
(101, '2024-01-01', 1000),
(101, '2024-01-27', 1200),
(101, '2024-02-05', 1250);

-- Create the orders table
CREATE TABLE orders (
    order_id INT,
    order_date DATE,
    product_id INT
);

-- Insert data into the orders table
INSERT INTO orders (order_id, order_date, product_id) VALUES
(1, '2024-01-05', 100),
(2, '2024-01-21', 100),
(3, '2024-02-20', 100),
(4, '2024-01-07', 101),
(5, '2024-02-04', 101),
(6, '2024-02-05', 101);








select * from products;
select * from orders;


select p.*, o.*
from orders o
join products p
on o.product_id = p.product_id 
and	o.order_date <= p.price_date
order by p.product_id;


SELECT 
        o.order_id,
        o.product_id,
        o.order_date,
        p.price
    FROM 
        orders o
    JOIN 
        products p
    ON 
        o.product_id = p.product_id
    AND 
        p.price_date = (
            SELECT MAX(price_date)
            FROM products p2
            WHERE p2.product_id = o.product_id
            AND p2.price_date <= o.order_date
        );


select p.product_id, p.price
from orders o
join products p
on  o.product_id = p.product_id
and p.price_date = (
  select max(price_date)
  from products p1
  where p1.product_id = o.product_id
  and p1.price_date <= o.order_date);
  
  
select * from products;

select price from products
order by price desc
limit 1
offset 2;

select max(price)
from products
where price < (select max(price) from products);

select distinct price from products
order by price desc
limit 1
offset n;




create function getNthPrice(N INT) returns INT
begin
set N = N-1;
	RETURN(
		select distinct price from products order by price desc limit 1 offset N
	);
END







