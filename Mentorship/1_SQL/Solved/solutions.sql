--Leetcode 262

select request_at as Day,
		round(sum(if(Status<>"completed", 1, 0)) / count(status) , 2) as "Cancellation Rate"
from trips
where request_at between "2013-10-01" and "2013-10-03"
and client_id not in (select users_id from users where banned = "Yes")
and driver_id not in (select users_id from users where banned = "Yes")
group by request_at;



--Leetcode 550

--sol1
select round(count(distinct b.player_id) / count(distinct a.player_id), 2) as fraction
from
(select player_id, min(event_date) as event_date
from activity
group by player_id ) a
left join activity b
on a.player_id = b.player_id and a.event_date + 1 = b.event_date;


--sol2
select round(sum(case when t1.event_date = t2.event_date + 1 then 1 else 0 end) / count(distinct t1.player_id) , 2) as fraction
from activity t1
join ( 
	select player_id, min(event_date) as event_date
    from activity group by player_id) t2
on t1.player_id = t2.player_id;



-- Leetcode 570

--sol1
select m.name as Manager
from employee e
join employee m
on e.managerId = m.id
group by m.name
having count(distinct e.id) >= 5;


--sol2
SELECT Name
FROM Employee
WHERE id IN
   (SELECT ManagerId
    FROM Employee
    GROUP BY ManagerId
    HAVING COUNT(DISTINCT Id) >= 5)


-- Leetcode 571  (HARD)    --- Understand from Nishchay

with expanded_freq as
( select *, 
	sum(frequency) over (order by num) as c_freq,
    sum(frequency) over () as total_freq
  from numbers
),
positions as
( 
	select num,
    (total_freq + 1) / 2 as lower,
	(total_freq + 2) / 2 as upper
    from expanded_freq
)
select round(avg(e.num), 2)
from expanded_freq e
join positions p on e.num = p.num
where c_freq between lower and upper;



-- Leetcode 578 (medium)

select question_id
from survey_log
group by question_id
order by round(count(answer_id) / sum(if(action = 'show', 1, 0)), 2) desc
limit 1;


------ Leetcode 579 {Hard}

WITH cte AS (
    SELECT 
        id, month,
        SUM(salary) OVER (
            PARTITION BY id 
            ORDER BY month 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS salary,
        MAX(month) OVER (PARTITION BY id) AS recent_month
    FROM Employee
)
SELECT 
    id, month, salary
FROM 
    cte 
WHERE 
    month <> recent_month
ORDER BY 
    id, month DESC;


  --Solution 2:

SELECT  a.id, a.month,  SUM(b.salary) AS salary
FROM  Employee a 
JOIN  Employee b 
ON  a.id = b.id 
    AND a.month - b.month >= 0 
    AND a.month - b.month < 3
GROUP BY
    a.id, a.month
HAVING
    (a.id, a.month) NOT IN (
                SELECT id, MAX(month) 
                FROM Employee 
                GROUP BY id  )
ORDER BY
    a.id, a.month DESC;


-- Leetcode 580

SELECT 
    dept_name, 
    SUM(CASE WHEN student_id IS NOT NULL THEN 1 ELSE 0 END) AS student_number
FROM 
    department d
LEFT JOIN 
    student s 
ON 
    d.dept_id = s.dept_id
GROUP BY 
    dept_name
ORDER BY student_number DESC, dept_name;


-- Leetcode 585 [Medium] [Premium]

select round(sum(tiv_2016), 2) as tiv_2016 from insurance
where tiv_2015 in (select tiv_2015 from insurance group by tiv_2015 having count(*) > 1)
and (lat, lon) in (select lat, lon from insurance group by lat, lon having count(*) = 1);

--or

WITH FilteredInsurance AS (
    SELECT 
        tiv_2016,
        COUNT(1) OVER (PARTITION BY tiv_2015) AS cnt_same_tiv,
        COUNT(1) OVER (PARTITION BY lat, lon) AS cnt_unique_location
    FROM 
        Insurance
)
SELECT 
    ROUND(SUM(tiv_2016), 2) AS tiv_2016
FROM 
    FilteredInsurance
WHERE 
    cnt_same_tiv > 1 AND cnt_unique_location = 1;



--  597

select round( (a.accepted / b.requests) , 2) as acceptance_rate
from
(select count(distinct requester_id, accepter_id) as accepted from RequestAccepted) a,
(select count(distinct sender_id, send_to_id ) as requests from friend_request) b;


-- 597 follow up : Can you write a query to return the accept rate but for every month? How about the cumulative accept rate for every day?

with monthly_requests as
(
	select month(request_date) as request_month, count(distinct sender_id, send_to_id) as requests
    from friend_request
    group by month(request_date)
),
monthly_acceptance as
(
	select month(accept_date) as accept_month, count(distinct requester_id, accepter_id) as accepted
    from request_accepted
    group by month(accept_date)
)
select request_month as month, round( (ifnull(accepted, 0) / ifnull(requests, 1) ), 2) as accept_rate
from monthly_requests mr 
left join monthly_acceptance ma on mr.request_month = ma.accept_month;




--  601. Human Traffic of Stadium


WITH cte AS (
 SELECT id, visit_date, people,
 LAG(people, 1) OVER (ORDER BY id) AS prev1,
 LAG(people, 2) OVER (ORDER BY id) AS prev2,
 LEAD(people, 1) OVER (ORDER BY id) AS next1,
 LEAD(people, 2) OVER (ORDER BY id) AS next2
 FROM stadium
)
SELECT * FROM cte
WHERE (people >= 100 AND next1 >= 100 AND next2 >= 100)
 OR (people >= 100 AND prev1 >= 100 AND prev2 >= 100)
 OR (people >= 100 AND prev1 >= 100 AND next1 >= 100);

--OR

 WITH grp AS (
 SELECT *,
 id - ROW_NUMBER() OVER (ORDER BY id) AS diff
 FROM stadium
 WHERE people >= 100
)
SELECT id, visit_date, people
FROM grp
WHERE diff IN (
 SELECT diff
 FROM grp
 GROUP BY diff
 HAVING COUNT(*) >= 3
);

-- 602

WITH bidirectional AS (
 SELECT requester_id AS id, COUNT(*) AS num
 FROM RequestAccepted
 GROUP BY requester_id
 UNION ALL
 SELECT accepter_id AS id, COUNT(*) AS num
 FROM RequestAccepted
 GROUP BY accepter_id
)
SELECT id, SUM(num) AS num
FROM bidirectional
GROUP BY id
ORDER BY SUM(num) DESC
LIMIT 1;


--- 607

select name from SalesPerson
where sales_id not in (
	select sales_id from orders
    where com_id in (
		select com_id from company
        where name = 'RED'
        )
	);
    

-- select s.name from salesPerson s
-- left join orders o on s.sales_id = o.sales_id
-- left join company c on o.com_id = c.com_id and c.name = 'RED'
-- where c.name is null;



-- Leetcode 608

SELECT id, 
	case 
		when p_id is null then 'Root'
        when id in (select distinct p_id from Tree where p_id is not null) then 'Inner'
        else 'Leaf'
        end
	as type
from tree;


SELECT t1.id,
    CASE
        WHEN ISNULL(t1.p_id) THEN 'Root'          -- Case 1: Root nodes
        WHEN ISNULL(MAX(t2.id)) THEN 'Leaf'       -- Case 2: Leaf nodes
        ELSE 'Inner'                              -- Case 3: Inner nodes
    END AS Type
FROM tree AS t1 
LEFT JOIN tree AS t2 ON t1.id = t2.p_id          -- Self-join to find children
GROUP BY t1.id, t1.p_id;

----- Leetcode 614 ---





--------- LEETCODE 615 ------

select t1.department_id, t1.month,
	(CASE WHEN t1.avg_department = t2.avg_company THEN 'same'
          WHEN t1.avg_department > t2.avg_company THEN 'higher'
          WHEN t1.avg_department < t2.avg_company THEN 'lower' END) AS comparison
 from (
select department_id, avg(amount) avg_department, month(pay_date) as month
from salary s
join employee e on s.employee_id = e.employee_id
group by 1, 3
order by 3 desc , 1) t1
join
(select avg(amount) avg_company, month(pay_date) as month
from salary s
join employee e on s.employee_id = e.employee_id
group by 2) t2
on t1.month = t2.month;



----- Leetcode 1069 ---

SELECT s.product_id, product_name, SUM(quantity) AS total_quantity
FROM sales s
JOIN product p ON s.product_id = p.product_id
GROUP BY 1, 2;


--- Leetcode 1070 -----

select product_id, price, year
from (select *, row_number() over (partition by product_id order by year) as rn from sales) sale
where rn = 1;

--OR

SELECT s.product_id, s.price, s.year
FROM sales s
JOIN (
    SELECT product_id, MIN(year) AS min_year
    FROM sales
    GROUP BY product_id
) first_sale ON s.product_id = first_sale.product_id AND s.year = first_sale.min_year;

--OR

SELECT
    product_id,
    year first_year,
    quantity,
    price
FROM Sales
WHERE (product_id, year) IN (SELECT product_id, MIN(year)
                             FROM Sales
                             GROUP BY product_id)

                    


--Leetcode 1076---

SELECT project_id
FROM Project
GROUP BY project_id
HAVING COUNT(employee_id) = (SELECT COUNT(employee_id)
                            FROM Project
                            GROUP BY project_id
                            ORDER BY COUNT(employee_id) DESC
                            LIMIT 1);

--- Leetcode 1077 -----

select project_id, e.employee_id
from project p
left join employee e on p.employee_id = e.employee_id
where experience_years = 
		(select max(experience_years) from employee);


------ Leetcode 1082 ------

SELECT seller_id
from sales
group by seller_id
having sum(price) = (SELECT sum(price)
from sales
group by seller_id
order by sum(price) desc limit 1);


----- Leetcode  1083 ---------

SELECT DISTINCT s.buyer_id
FROM Sales s
JOIN Product p ON s.product_id = p.product_id
WHERE p.product_name = 'S8'
AND s.buyer_id NOT IN (
    SELECT s2.buyer_id
    FROM Sales s2
    INNER JOIN Product p2 ON s2.product_id = p2.product_id
    WHERE p2.product_name = 'iPhone'
);


----- Leetcode  1084 ---------

select p.product_id, p.product_name
from sales s
join product p on s.product_id = p.product_id
where sale_date between '2019-01-01' and '2019-03-31';

--or

SELECT DISTINCT s.product_id, p.product_name
FROM Sales s LEFT JOIN Product p ON
    s.product_id = p.product_id
WHERE s.sale_date >= '2019-01-01' AND
      s.sale_date <= '2019-03-31';



----- Leetcode  1097 ---------

WITH t AS (
    SELECT player_id, MIN(event_date) AS first_login
    FROM Activity
    GROUP BY player_id
),
d1 AS (
    SELECT t.first_login, COUNT(t.player_id) AS day1_ret
    FROM Activity a
    LEFT JOIN t ON a.player_id = t.player_id
    WHERE DATEDIFF(a.event_date, t.first_login) = 1
    GROUP BY t.first_login
)
SELECT 
    t.first_login AS install_date, 
    COUNT(t.player_id) AS installs, 
    IFNULL(ROUND(MAX(d1.day1_ret) / COUNT(t.player_id), 2), 0.00) AS day1_retention
FROM t
LEFT JOIN d1 ON t.first_login = d1.first_login
GROUP BY t.first_login;

--Or


select install_dt, count(install_dt) as installs, 
ifnull(round(count(b.event_date) / count(install_dt),2) , 0.00) Day1_retention
 from
(SELECT player_id, MIN(event_date) install_dt
FROM Activity
GROUP BY player_id) a
LEFT JOIN Activity b ON
a.player_id = b.player_id 
AND DATE_ADD(a.install_dt, INTERVAL 1 DAY) = b.event_date
group by install_dt;



----- Leetcode  1098 ---------

select b.book_id, b.name
from books b
left join
    ( select book_id, sum(quantity) nsold
      from orders o
      where dispatch_date between date_sub('2019-06-23', interval 1 year) and '2019-06-23'
      group by book_id 
      ) o
 on b.book_id = o.book_id
  where (o.nsold < 10 or o.nsold is null)
 and DATEDIFF('2019-06-23', b.available_from) > 30;
 ;

----- Leetcode  1107 ---------

select first_login as login_date, count(user_id) as user_count
from (
    select user_id, min(activity_date) as first_login
    from traffic
    where activity = 'login'
    group by user_id ) f
where datediff('2019-06-30', first_login) <= 90
group by first_login;




----- Leetcode  1112 ---------

select student_id, course_id, grade
from (
	select student_id, course_id, grade, ROW_NUMBER() over (partition by student_id order by grade desc, student_id) rn
	from enrollments
    ) r
where rn = 1;




----- Leetcode  1113 ---------

select extra as report_reason, count(distinct post_id) as report_count
from actions
where action_date = '2019-07-04' and action  = 'report'
group by report_reason
order by report_count;




----- Leetcode  1126 ---------

select business_id
from events e
left join (select event_type, avg(occurences) as average
		from events
	group by event_type) av 
on e.event_type = av.event_type
where occurences > average
group by business_id
having count(e.event_type) > 1;


----- Leetcode  1132 ---------

with percentages as
(select (count(r.post_id) / count(DISTINCT a.post_id)) * 100 as percentages
	from actions a
	left join removals r
	on a.post_id = r.post_id
	where extra = 'spam' and action = 'report'
	group by a.action_date)
select round(avg(percentages), 2) as average_daily_percent from percentages;



----- Leetcode  1141 ---------

select activity_date, count(distinct user_id) as user_count
from activity
where activity_type is not null
and activity_date between date_sub('2019-07-27', interval 30 day) and '2019-07-27'
group by activity_date;


----- Leetcode  1142 ---------

select IFNULL(ROUND(AVG(a.num),2),0) as average_sessions_per_user
from
( select count(DISTINCT session_id) as num
from activity
where activity_date between date_sub('2019-07-27' , interval 30 day) and '2019-07-27'
group by user_id) a;


----- Leetcode  1148 ---------

select distinct author_id as id
from views
where author_id = viewer_id;


----- Leetcode  1149 ---------

select DISTINCT viewer_id as id
from views
group by viewer_id, view_date
having count(distinct article_id) > 1
order by 1;

----- Leetcode  1158 ---------

select user_id, join_date, coalesce(orders_in_2019, 0) orders_in_2019
from users u
left join 
(select u1.user_id as buyer_id, count(buyer_id) as orders_in_2019
from users u1
join orders o on u1.user_id = o.buyer_id
where year(order_date) = 2019
group by u1.user_id ) o1
on u.user_id = o1.buyer_id;

----- Leetcode  1164 ---------

select t1.product_id, coalesce(t2.new_price, 10)
from (SELECT distinct product_id
  FROM Products) AS t1 
LEFT JOIN ( select product_id, new_price
			from products
            where (product_id, change_date) in 
						(select product_id, max(change_date)
							from products
							where change_date <= '2019-08-16'
							group by product_id
						)
		) t2
on t1.product_id = t2.product_id;


----- Leetcode  1173 ---------

select round(sum(case when order_date = customer_pref_delivery_date then 1 else 0 end) / count(*) * 100 ,2) as immediate_percentage
from delivery;



----- Leetcode  1174 ---------

select round((sum(case when order_date = customer_pref_delivery_date then 1 else 0 end) / count(delivery_id) ) * 100 , 2) as immediate_percentage
from
(select *, row_number() over (partition by customer_id order by order_date asc) rn
from delivery) first_orders
where rn = 1;

----- Leetcode  1179 ---------

select id,
    sum(if(month = 'Jan', revenue, null)) as Jan_Revenue,
    sum(if(month = 'Feb', revenue, null)) as Feb_Revenue,
    sum(if(month = 'Mar', revenue, null)) as Mar_Revenue,
    sum(if(month = 'Apr', revenue, null)) as Apr_Revenue,
    sum(if(month = 'May', revenue, null)) as May_Revenue,
    sum(if(month = 'Jun', revenue, null)) as Jun_Revenue,
    sum(if(month = 'Jul', revenue, null)) as Jul_Revenue,
    sum(if(month = 'Aug', revenue, null)) as Aug_Revenue,
    sum(if(month = 'Sep', revenue, null)) as Sep_Revenue,
    sum(if(month = 'Oct', revenue, null)) as Oct_Revenue,
    sum(if(month = 'Nov', revenue, null)) as Nov_Revenue,
    sum(if(month = 'Dec', revenue, null)) as Dec_Revenue
from department
group by id;

----- Leetcode  1193 ---------

select DATE_FORMAT(trans_date, '%Y-%m') as month, country, count(id) as trans_count, 
		sum(case when state = 'approved' then 1 else 0 end) as approved_count,
        sum(amount) as trans_total_amount,
		sum(case when state = 'approved' then amount else 0 end) as approved_total_amount
from transactions
group by 1,2;

----- Leetcode  1194 ---------

with cte as
(
select p.player_id, sum(coalesce(score, 0)) as total
from players p
left join
(select first_player as player_id,
		first_score as score
from matches
union all
select second_player as player_id,
		second_score as score
from matches) as m
on p.player_id = m.player_id
group by 1
),
cte2 as (
select cte.player_id, group_id, total, rank() over (partition by group_id order by total desc, cte.player_id) as rk
from cte
join players p on cte.player_id = p.player_id)
select group_id, player_id
from cte2
where rk = 1;

----- Leetcode  1204 ---------

select person_name
from
(select *, sum(weight) over (order by turn) as current_weight
from queue) as cum_weight
where current_weight <= 1000
order by current_weight desc
limit 1;

----- Leetcode  1205 --------- V.Imp

select month,
		country,
        sum(if(state = 'approved', 1, 0)) as approved_count,
        sum(if(state = 'approved', amount, 0)) as approved_amount,
        sum(if(state is null, 1, 0)) as chargeback_count,
        sum(if(state is null, amount, 0)) as chargeback_amount
		
from	(select date_format(trans_date, '%Y-%m') as month,
			country,
            state,
			id,
			amount
		from transactions
        
        union
        
        select date_format(charge_date, '%Y-%m') as month,
				country,
                null as state,
				trans_id,
                amount
		from transactions t join chargebacks c on t.id = c.trans_id ) as all_transactions
group by 1, 2
HAVING 
    approved_count > 0 OR approved_amount > 0 OR chargeback_count > 0 OR chargeback_amount > 0;

--or 

SELECT month, country,
    SUM(CASE WHEN type='approved' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN type='approved' THEN amount ELSE 0 END) AS approved_amount,
    SUM(CASE WHEN type='chargeback' THEN 1 ELSE 0 END) AS chargeback_count,
    SUM(CASE WHEN type='chargeback' THEN amount ELSE 0 END) AS chargeback_amount
FROM (
    (
    SELECT left(t.trans_date, 7) AS month, t.country, amount,'approved' AS type
    FROM Transactions AS t
    WHERE state='approved'
    )
    UNION ALL (
    SELECT left(c.charge_date, 7) AS month, t.country, amount,'chargeback' AS type
    FROM Transactions AS t JOIN Chargebacks AS c
    ON t.id = c.trans_id
    )
) AS tt
GROUP BY tt.month, tt.country;


----- Leetcode  1212 ---------

with cte as
(
select team_id, sum(num_points) as num_points
from
	(
		select host_team as team_id, 
			   sum( case when host_goals > guest_goals then 3
						when host_goals = guest_goals then 1
                         else 0
				end) as num_points
		from matches
        group by host_team, match_id
        
        union all
        
        select guest_team as team_id, 
			   sum( case when guest_goals > host_goals then 3
						when guest_goals = host_goals then 1
                         else 0
				end) as num_points
		from matches
        group by guest_team, match_id
        
	) as combined
group by team_id
)
select t.team_id, t.team_name, coalesce(num_points, 0) as num_points
from teams t
left join cte on t.team_id = cte.team_id
order by 3 desc, 1;



----------LEETCODE 1225 ----------------

with Combined as 
(
select success_date as task_date, 'success' as period_state from Succeeded where success_date BETWEEN '2019-01-01' and '2019-12-31'
union
select fail_date, 'failed' from failed where fail_date BETWEEN '2019-01-01' and '2019-12-31'
),
ranked as
(
select *,
	-- row_number() over (order by task_date),
    -- row_number() over (partition by period_state order by task_date),
    row_number() over (order by task_date) - row_number() over (partition by period_state order by task_date) as grp
from combined
)
select period_state,
	min(task_date) as start_date,
    max(task_date) as end_date
from ranked
group by period_state, grp
order by start_date;



----------LEETCODE 1241 ----------------


with posts as
(		select sub_id as post_id from submissions where parent_id is null
		union
        select parent_id from submissions where parent_id is not null
)
select post_id, count(distinct sub_id) as number_of_comments
from posts p
left join submissions s on p.post_id = s.parent_id
group by post_id;

--or 

select post_id, count(distinct sub_id) as number_of_comments
from (select sub_id as post_id from submissions where parent_id is null) as p
left join submissions s on p.post_id = s.parent_id
group by post_id;

--or

SELECT 
    s1.sub_id AS post_id,
    COUNT(DISTINCT s2.sub_id) AS number_of_comments
FROM 
    Submissions s1
LEFT JOIN 
    Submissions s2
ON 
    s1.sub_id = s2.parent_id
WHERE 
    s1.parent_id IS NULL
GROUP BY 
    s1.sub_id
ORDER BY 
    s1.sub_id;

----------LEETCODE 1251 ----------------
                    
select p.product_id, round(sum(p.price * u.units)/sum(units),2) as average_price
from prices p
join UnitsSold u on p.product_id = u.product_id and u.purchase_date between p.start_date and p.end_date
group by p.product_id;


----------LEETCODE 1264 ----------------

with friends as
(
select user2_id as id from friendship where user1_id = 1
union
select user1_id from friendship where user2_id = 1
)
select distinct(page_id) as recommended_page
from likes
where user_id in (select id from friends) and page_id not in (select page_id from likes where user_id = 1);


----------LEETCODE 1270 ----------------

select e3.employee_id
	from employees e1
	join employees e2 on e2.manager_id = e1.employee_id
    join employees e3 on e3.manager_id = e2.employee_id
where e1.manager_id = 1 and e3.employee_id != 1;

--or 

with recursive cte as
(
select employee_id from employees
where manager_id = 1
union
select e.employee_id
from employees e
join cte on e.manager_id = cte.employee_id
)
select * from cte
where employee_id != 1
order by employee_id;

----------LEETCODE 1280 ----------------

select s.student_id, s.student_name, sub.subject_name, count(e.student_id) attended_exams
FROM 
    Students s
CROSS JOIN 
    Subjects sub
LEFT JOIN
	Examinations e ON s.student_id = e.student_id AND sub.subject_name = e.subject_name
GROUP BY s.student_id, s.student_name, sub.subject_name
ORDER BY s.student_id;

----------LEETCODE 1285 ----------------

select min(log_id) as start_id, max(log_id) as end_id
from(
select log_id, 
	log_id - row_number() over (order by log_id) as grp
from logs
) as grouped
group by grp;


--or

--Solution 2: Add temporary columns of rank and prev
SELECT MIN(log_id) AS START_ID, MAX(log_id) AS END_ID
FROM (SELECT log_id,
        @rank := CASE WHEN @prev = log_id-1 THEN @rank ELSE @rank+1 END AS rank,
        @prev := log_id AS prev
    FROM Logs,
       (SELECT @rank:=0, @prev:=-1) AS rows) AS tt
GROUP BY rank
ORDER BY START_ID

----------LEETCODE 1294 ----------------

select country_name, 
		case when average_state <= 15 then 'Cold'
			when average_state >= 25 then 'Hot'
            else 'warm'
		end as weather
from
(select country_id, avg(weather_state) as average_state
from weather
where date_format(day, '%Y-%m') = '2019-11'
group by country_id) a
left join countries c on a.country_id = c.country_id;


----------LEETCODE 1303 ----------------

select employee_id, s.team_size
from  Employee e
join
(select team_id, count(employee_id) as team_size
from employee
group by team_id
) s on e.team_id = s.team_id;


----------LEETCODE 1308 ----------------

SELECT gender, day,
SUM(score_points) OVER (PARTITION BY gender ORDER BY day) AS total
FROM Scores
ORDER BY gender, day;


----------LEETCODE 1321 ----------------

select visited_on, 
	sum(amount) over (order by visited_on rows between 6 preceding and current row) as amount,
    round(avg(amount) over (order by visited_on rows between 6 preceding and current row),2) as average
from 
(
	SELECT visited_on, SUM(amount) AS amount
	FROM customer
	GROUP BY visited_on
	ORDER BY visited_on
) a
order by visited_on
offset 6 ROWS;

--or

SELECT visited_on, amount, average
FROM (
    SELECT visited_on, 
        SUM(amount) OVER (ORDER BY visited_on ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS amount,
        ROUND(AVG(amount) OVER (ORDER BY visited_on ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS average
    FROM 
    (
        SELECT visited_on, SUM(amount) AS amount
        FROM Customer
        GROUP BY visited_on
        ORDER BY visited_on
    ) a
) b
WHERE DATEDIFF(visited_on, (SELECT MIN(visited_on) FROM Customer)) >= 6
ORDER BY visited_on;


----------LEETCODE 1322 ----------------

select ad_id, 
    round((case when total_clicks = 0 and total_views = 0 then 0
        else (total_clicks / (total_clicks + total_views) ) * 100
        end), 2) as ctr
from
(
    select ad_id,
            sum(if(action = 'Clicked', 1, 0)) as total_clicks,
            sum(if(action = 'Viewed', 1, 0)) as total_views
    from Ads
    group by ad_id
) total
group by 1
order by 1;

----------LEETCODE 1327 ----------------

select p.product_name, sum(o.unit) as unit
from products p
join
( select product_id, unit
from orders where date_format(order_date, '%Y-%m') = '2020-02') o
on p.product_id = o.product_id
group by product_name
having unit >= 100;

----------LEETCODE 1341 ----------------

select * from 
(
select name as results
from Movie_Rating m
join users u on m.user_id = u.user_id
group by name
order by count(distinct movie_id) desc, name
limit 1) as top_user

union all

select * from
(
select title
from Movies mo
join Movie_Rating m on mo.movie_id = m.movie_id
where date_format(created_at, '%Y-%m') = '2020-02'
group by title
order by avg(rating) desc, title
limit 1
) as top_movie;


----------LEETCODE 1350 ----------------

select s.id, s.name
from students s
left join Departments d on s.department_id = d.id
where d.name is null;


----------LEETCODE 1355 ----------------

with performed as
(
SELECT activity, count(id) as cnt
FROM friends f
group by activity
),
max_min as
(
select max(cnt) cnt from performed
union all
select min(cnt) cnt from performed
)
select activity from performed
where cnt not in (select cnt from max_min);

----------LEETCODE 1364 ----------------

with customers_count as
(
select c.customer_id, c.customer_name, count(user_id) as contacts_cnt, count(c1.customer_name) as trusted_contacts_cnt
from Customers c
left join contacts co on c.customer_id = co.user_id
left join Customers c1 on co.contact_name = c1.customer_name
group by c.customer_id, c.customer_name
)
select invoice_id, customer_name, price, contacts_cnt, trusted_contacts_cnt
from invoices i
left join customers_count cou on i.user_id = cou.customer_id;

----------LEETCODE 1378 ----------------

select unique_id, name
from employees e
left join EmployeeUNI eu on e.id = eu.id;

----------LEETCODE 1393 ----------------

SELECT stock_name,
       SUM(CASE WHEN operation = 'Buy' THEN -price ELSE price END) AS capital_gain_loss
FROM Stocks
GROUP BY stock_name;

--or

select buys.stock_name, sell_amount - buy_amount as capital_gain_loss
from
(
select stock_name, sum(price) as buy_amount from stocks
where operation = 'Buy'
group by stock_name) buys
left join
(
select stock_name, sum(price) as sell_amount from stocks
where operation = 'Sell'
group by stock_name
) sells 
on buys.stock_name = sells.stock_name;

----------LEETCODE 1398 ----------------

select distinct c.customer_id, customer_name
from Customers c
join Orders o
on c.customer_id = o.customer_id
where c.customer_id not in 
	(select distinct customer_id from orders
		where product_name = 'C'
        )
and c.customer_id in 
	(select customer_id from orders 
		where product_name in ('A', 'B') 
        group by customer_id 
        having count(distinct product_name) = 2
        );

--or

SELECT DISTINCT c.customer_id, c.customer_name
FROM Customers c
JOIN Orders o1 ON c.customer_id = o1.customer_id AND o1.product_name = 'A'
JOIN Orders o2 ON c.customer_id = o2.customer_id AND o2.product_name = 'B'
WHERE NOT EXISTS 
    (SELECT 1 FROM Orders o3
     WHERE o3.customer_id = c.customer_id
       AND o3.product_name = 'C');


----------LEETCODE 1407 ----------------

select name, coalesce(sum(distance), 0) as travelled_distance
from Users u
left join Rides r on u.id = r.user_id
group by name
order by 2 desc, 1;


----------LEETCODE 1421 ----------------

select q.id, q.year, coalesce(n.npv, 0) npv
from queries q
left join npv n on q.id = n.id and q.year = n.year
;

----------LEETCODE 1435 ----------------

(select '[0-5>' as bin, sum(case when duration/60 < 5 THEN 1 ELSE 0 END) as total from sessions)
union
(select '[5-10>' as bin, sum(case when duration/60 BETWEEN 5 AND 10 THEN 1 ELSE 0 END) as total from sessions)
union
(select '[10-15>' as bin, sum(case when duration/60 BETWEEN 10 AND 15 THEN 1 ELSE 0 END) as total from sessions)
union
(select '15 or more' as bin, sum(case when duration/60 > 15 THEN 1 ELSE 0 END) as total from sessions);

----------LEETCODE 1440 ----------------




----------LEETCODE 1445 ----------------

select sale_date, sum(case when fruit = 'oranges' then -sold_num else sold_num end) as diff
from sales
group by sale_date;


----------LEETCODE 1454 ----------------

with cte as
(
	select id, login_date, lead(login_date, 4) over(partition by id order by login_date ) date5
    from (select distinct * from logins) a
)
select a.id, a.name 
from cte
join logins l on cte.id = l.id
join accounts a on cte.id = a.id
where date_sub(cte.date5, interval 4 day) = l.login_date;

----------LEETCODE 1459 ----------------

select p1, p2, area
from
(
select p1.id as p1, p2.id as p2, abs(p1.x_value - p2.x_value) * abs(p1.y_value - p2.y_value) as area
from points p1
join points p2 on p1.id < p2.id
) p
where area <> 0
order by area desc, p1, p2;


----------LEETCODE 1468 ----------------

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


----------LEETCODE 1484 ----------------

select sell_date, count(distinct product) as num_sold, group_concat(product)
from activities
group by sell_date;

----------LEETCODE 1495 ----------------

select distinct title
from Content c
join TVProgram p
on c.content_id = p.content_id
where c.Kids_content = 'Y'
and date_format(program_Date, '%Y-%m') = '2020-06';

----------LEETCODE 1322 ----------------

SELCT DISTINCT title
FROM
(SELCT content_id, title
FROM content
WHERE kids_content = 'Y' AND content_type = 'Movies') a
JOIN
tvprogram USING (content_id)
WHERE month(program_date) = 6


----------LEETCODE 1501 ----------------

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


----------LEETCODE 1511 ----------------

with monthly_spending as
(
select o.customer_id, c.name, month(order_date) as month, sum(quantity * price) as amount
from orders o
left join product p on o.product_id = p.product_id
join customers c on o.customer_id = c.customer_id
where year(order_date) = 2020 and month(order_date) in (6,7)
group by month(order_date), o.customer_id, c.name
having sum(quantity * price) >= 100
)
select customer_id, name
from monthly_spending
group by customer_id, name
having count(distinct month) = 2 and min(amount) >= 100;


--or

SELECT o.customer_id, name
from orders o
JOIN Product p
ON o.product_id = p.product_id
JOIN Customers c
ON o.customer_id = c.customer_id
GROUP BY 1, 2
HAVING SUM(CASE WHEN date_format(order_date, '%Y-%m')='2020-06'
THEN price*quantity END) >= 100
AND
SUM(CASE WHEN date_format(order_date, '%Y-%m')='2020-07'
THEN price*quantity END) >= 100;

----------LEETCODE 1517 ----------------

select *
from users
where mail regexp  "^[a-zA-Z]+[a-zA-Z0-9_\\./\\-]{0,}@leetcode\\.com$";

----------LEETCODE 1527 ----------------

SELECT * FROM Patients
where conditions like '% DIAB1%' or conditions like 'DIAB1%';

----------LEETCODE 1532 ----------------

select name as customer_name, o.customer_id, order_id, order_date
from
(SELECT *, row_number() over (partition by customer_id order by order_date desc) rn
FROM Orders) o
join Customers c on o.customer_id = c.customer_id
where rn <= 3
ORDER BY customer_name, customer_id, order_date DESC;

----------LEETCODE 1543 ----------------

SELECT TRIM(LOWER(product_name)) AS product_name,
       DATE_FORMAT(sale_date, '%Y-%m') AS sale_date,
       COUNT(*) AS total
FROM Sales
GROUP BY 1, DATE_FORMAT(sale_date, '%Y-%m')
ORDER BY 1, 2;

----------LEETCODE 1549 ----------------

with cte as
(
SELECT p.product_name, p.product_id,  o.order_id, o.order_date,
		rank() over (partition by p.product_name order by order_date desc) rn
FROM orders o
join products p on o.product_id = p.product_id
)
select product_name, product_id, order_id, order_date from cte
where rn = 1;

----------LEETCODE 1555 ----------------

with cte as
(
	select user_id, sum(trans) as credit_used from
	(
		select paid_by as user_id, -amount as trans from transaction
        union all
        select paid_to, amount as trans from transaction
	) t
	group by user_id
),
cte2 as
(	select u.user_id, user_name, (u.credit + ifnull(cte.credit_used,0)) as credit
	from users u
	left join cte on u.user_id = cte.user_id
)
select *, case when credit < 0 then 'Yes' else 'No' end as credit_limit_breached
from cte2;

----------LEETCODE 1565 ----------------

SELECT date_format(order_date, '%Y-%m') as month, 
		count(distinct order_id) as order_count,
		count(distinct customer_id) as customer_count
from orders
where invoice > 20
group by 1;

----------LEETCODE 1571 ----------------

select name as warehouse_name, sum(units * width * length * height) volume
from warehouse w
left join products p on w.product_id = p.product_id
group by 1;


----------LEETCODE 1581 ----------------

SELECT customer_id, count(v.visit_id) count_no_trans
FROM visits V
left join transactions t on v.visit_id = t.visit_id
where transaction_id is null
group by 1
order by 2 desc;

----------LEETCODE 1587 ----------------

select name, balance
from users u
left join 
	(select account, sum(amount) as balance from transactions group by account) t
on u.account = t.account
where balance > 10000;

----------LEETCODE 1596 ----------------

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


----------LEETCODE 1607 ----------------

SELECT seller_name FROM Seller
WHERE seller_id NOT IN (
SELECT DISTINCT seller_id FROM Orders
WHERE YEAR(sale_date)='2020'
)
ORDER BY seller_name;

----------LEETCODE 1613 ----------------

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

----------LEETCODE 1623 ----------------

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

----------LEETCODE 1661 ----------------

select a1.machine_id, round(avg(a1.timestamp - a2.timestamp),3) as processing_time
from activity a1
join activity a2
on a1.machine_id = a2.machine_id
and a1.process_id = a2.process_id
where a1.activity_type = 'end' and a2.activity_type = 'start'
group by a1.machine_id;

----------LEETCODE 1669 ----------------

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

----------LEETCODE 1709 ----------------

select user_id, max(diff) as biggest_window
from
(select user_id, datediff(coalesce(lead(visit_date) over (partition by user_id order by visit_date), '2021-01-1') , visit_date) as diff
from UserVisits) t
group by 1
order by 1;

----------LEETCODE 1715 ----------------

SELECT sum(IFNULL(box.apple_count, 0) + IFNULL(chest.apple_count, 0)) AS apple_count,
    sum(IFNULL(box.orange_count, 0) + IFNULL(chest.orange_count, 0)) AS orange_count
    FROM Boxes AS box
    LEFT JOIN Chests AS chest
    ON box.chest_id = chest.chest_id;

----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------



----------LEETCODE 1613 ----------------