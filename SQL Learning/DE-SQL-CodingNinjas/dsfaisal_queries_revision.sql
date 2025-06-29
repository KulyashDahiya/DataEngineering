----------------------------------------------------------------------------

DELIMITER $$

CREATE FUNCTION getNthPrice(N INT) RETURNS INT
DETERMINISTIC
BEGIN
    DECLARE result INT;

    SET N = N - 1;

    SELECT DISTINCT price
    INTO result
    FROM products
    ORDER BY price DESC
    LIMIT 1 OFFSET N;

    RETURN result;
END$$

DELIMITER ;


SELECT getNthPrice(2) AS nth_price;

----------------------------------------------------------------------------

-- Drop the table if it already exists
DROP TABLE IF EXISTS scores;

-- Create the table
CREATE TABLE scores (
    Id INT PRIMARY KEY,
    Score DECIMAL(4,2) NOT NULL
);

-- Insert data into the table
INSERT INTO scores (Id, Score) VALUES
(1, 3.50),
(2, 3.65),
(3, 4.00),
(4, 3.85),
(5, 4.00),
(6, 3.65);


select score, DENSE_RANK() over(order by score desc) as "Rank"
from scores;

----------------------------------------------------------------------------
-- Drop the table if it already exists
DROP TABLE IF EXISTS Logs;

-- Create the table
CREATE TABLE Logs (
    Id INT PRIMARY KEY,
    Num INT NOT NULL
);

-- Insert data into the table
INSERT INTO Logs (Id, Num) VALUES
(1, 1),
(2, 1),
(3, 1),
(4, 2),
(5, 1),
(6, 2),
(7, 2);


select * from logs;

select l2.num as consecutiveNums
from logs l1
join logs l2
on l1.id = l2.id - 1 and l1.num = l2.num
join logs l3
on l2.id = l3.id -1 and l2.num = l3.num;



----------------------------------------------------------------------------

-- Drop the table if it already exists
DROP TABLE IF EXISTS Employee;

-- Create the table
CREATE TABLE Employee (
    Id INT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL,
    Salary INT NOT NULL,
    ManagerId INT
);

-- Insert data into the table
INSERT INTO Employee (Id, Name, Salary, ManagerId) VALUES
(1, 'Joe', 70000, 3),
(2, 'Henry', 80000, 4),
(3, 'Sam', 60000, NULL),
(4, 'Max', 90000, NULL);

select e.name as Employee
from employee e
join employee m
on e.managerid = m.id
where m.salary < e.salary;


----------------------------------------------------------------------------

-- Drop the table if it already exists
DROP TABLE IF EXISTS Person;

-- Create the table
CREATE TABLE Person (
    Id INT PRIMARY KEY,
    Email VARCHAR(255) NOT NULL
);

-- Insert data into the table
INSERT INTO Person (Id, Email) VALUES
(1, 'a@b.com'),
(2, 'c@d.com'),
(3, 'a@b.com');

select email from person
group by email
having count(*) > 1;


----------------------------------------------------------------------------

-- Drop the tables if they already exist
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Orders;

-- Create the Customers table
CREATE TABLE Customers (
    Id INT PRIMARY KEY,
    Name VARCHAR(50) NOT NULL
);

-- Create the Orders table
CREATE TABLE Orders (
    Id INT PRIMARY KEY,
    CustomerId INT NOT NULL
);

-- Insert data into the Customers table
INSERT INTO Customers (Id, Name) VALUES
(1, 'John'),
(2, 'Alice'),
(3, 'Bob'),
(4, 'Charlie');

-- Insert data into the Orders table
INSERT INTO Orders (Id, CustomerId) VALUES
(1, 3),
(2, 1);



select * from customers c
left join orders o
on c.id = o.customerId
where o.customerid is null;

----------------------------------------------------------------------------
-- Drop tables if they already exist
DROP TABLE IF EXISTS Employee;
DROP TABLE IF EXISTS Department;

-- Create Employee table
CREATE TABLE Employee (
    Id INT PRIMARY KEY,
    Name VARCHAR(50),
    Salary INT,
    DepartmentId INT
);

-- Create Department table
CREATE TABLE Department (
    Id INT PRIMARY KEY,
    Name VARCHAR(50)
);

-- Insert data into Employee table
INSERT INTO Employee (Id, Name, Salary, DepartmentId) VALUES
(1, 'Joe', 70000, 1),
(2, 'Jim', 90000, 1),
(3, 'Henry', 80000, 2),
(4, 'Sam', 60000, 2),
(5, 'Max', 90000, 1);

-- Insert data into Department table
INSERT INTO Department (Id, Name) VALUES
(1, 'IT'),
(2, 'Sales');

with cte as
(
select d.name as Department, e.name as Employee, e.salary, 
rank() over (partition by d.name order by e.salary desc) as rnk
from employee e
join department d
on e.departmentId = d.id
)
select Department, Employee, Salary
from cte
where rnk = 1;

-- Solution 2
select d.name as Department, e.name as Employee, Salary
from employee e
join department d
on e.departmentID = d.id
where (departmentID, Salary) in (
			select departmentID, max(salary) as salary
            from employee 
            group by departmentID
            );

----------------------------------------------------------------------------

-- Drop tables if they already exist
DROP TABLE IF EXISTS Employee;
DROP TABLE IF EXISTS Department;

-- Create Employee table
CREATE TABLE Employee (
    Id INT PRIMARY KEY,
    Name VARCHAR(50),
    Salary INT,
    DepartmentId INT
);

-- Create Department table
CREATE TABLE Department (
    Id INT PRIMARY KEY,
    Name VARCHAR(50)
);

-- Insert data into Employee table
INSERT INTO Employee (Id, Name, Salary, DepartmentId) VALUES
(1, 'Joe', 85000, 1),
(2, 'Henry', 80000, 2),
(3, 'Sam', 60000, 2),
(4, 'Max', 90000, 1),
(5, 'Janet', 69000, 1),
(6, 'Randy', 85000, 1),
(7, 'Will', 70000, 1);

-- Insert data into Department table
INSERT INTO Department (Id, Name) VALUES
(1, 'IT'),
(2, 'Sales');


-- Solution

with cte as
(
select d.name as Department, e.name as Employee, e.salary, 
DENSE_RANK() over (partition by d.name order by e.salary desc) as rnk
from employee e
join department d
on e.departmentId = d.id
)
select Department, Employee, Salary
from cte
where rnk <= 3;

----------------------------------------------------------------------------
-- Drop table if it already exists
DROP TABLE IF EXISTS Person;

-- Create Person table
CREATE TABLE Person (
    Id INT PRIMARY KEY,
    Email VARCHAR(100)
);

-- Insert data into Person table
INSERT INTO Person (Id, Email) VALUES
(1, 'john@example.com'),
(2, 'bob@example.com'),
(3, 'john@example.com');

select * from person;

DELETE p2
from person p1
join person p2
on p1.email = p2.email
and p1.id < p2.id;


----------------------------------------------------------------------------

-- Drop table if it already exists
DROP TABLE IF EXISTS Weather;

-- Create Weather table
CREATE TABLE Weather (
    id INT PRIMARY KEY,
    recordDate DATE,
    temperature INT
);

-- Insert sample data into Weather table
INSERT INTO Weather (id, recordDate, temperature) VALUES
(1, '2023-01-01', 30),
(2, '2023-01-02', 35),
(3, '2023-01-03', 25),
(4, '2023-01-04', 28);


select * from weather;

select w1.id
from weather w1, weather w2
where datediff(w1.recorddate, w2.recordDate) = 1
and w1.temperature > w2.temperature;

-- sol 2
select id from
(
select id, recordDate, temperature,
lag(temperature) over (order by recordDate) as prev_day_temp
from weather
) a
where temperature > prev_day_temp;


----------------------------------------------------------------------------

-- Drop tables if they already exist
DROP TABLE IF EXISTS Trips;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Register;

-- Create Users table
CREATE TABLE Users (
    Users_Id INT PRIMARY KEY,
    Banned ENUM('Yes', 'No'),
    Role ENUM('client', 'driver', 'partner')
);

-- Create Trips table
CREATE TABLE Trips (
    Id INT PRIMARY KEY,
    Client_Id INT,
    Driver_Id INT,
    City_Id INT,
    Status ENUM('completed', 'cancelled_by_driver', 'cancelled_by_client'),
    Request_at DATE,
    FOREIGN KEY (Client_Id) REFERENCES Users(Users_Id),
    FOREIGN KEY (Driver_Id) REFERENCES Users(Users_Id)
);

-- Insert data into Users table
INSERT INTO Users (Users_Id, Banned, Role) VALUES
(1, 'No', 'client'),
(2, 'Yes', 'client'),
(3, 'No', 'client'),
(4, 'No', 'client'),
(10, 'No', 'driver'),
(11, 'No', 'driver'),
(12, 'No', 'driver'),
(13, 'No', 'driver');

-- Insert data into Trips table
INSERT INTO Trips (Id, Client_Id, Driver_Id, City_Id, Status, Request_at) VALUES
(1, 1, 10, 1, 'completed', '2013-10-01'),
(2, 2, 11, 1, 'cancelled_by_driver', '2013-10-01'),
(3, 3, 12, 6, 'completed', '2013-10-01'),
(4, 4, 13, 6, 'cancelled_by_client', '2013-10-01'),
(5, 1, 10, 1, 'completed', '2013-10-02'),
(6, 2, 11, 6, 'completed', '2013-10-02'),
(7, 3, 12, 6, 'completed', '2013-10-02'),
(8, 2, 12, 12, 'completed', '2013-10-03'),
(9, 3, 10, 12, 'completed', '2013-10-03'),
(10, 4, 13, 12, 'cancelled_by_driver', '2013-10-03');

select request_at as Day, 
round(sum( if(status <> "completed", 1, 0) ) / count(status),2) as "Cancellation Rate"
from trips
where request_at between "2013-10-01" and "2013-10-03"
and client_id not in (select users_id from users where banned = 'Yes')
and driver_id not in (select users_id from users where banned = 'Yes')
group by request_at;


----------------------------------------------------------------------------

-- Drop table if it already exists
DROP TABLE IF EXISTS Activity;

-- Create Activity table
CREATE TABLE Activity (
    player_id INT,
    device_id INT,
    event_date DATE,
    games_played INT,
    PRIMARY KEY (player_id, event_date)
);

-- Insert data into Activity table
INSERT INTO Activity (player_id, device_id, event_date, games_played) VALUES
(1, 2, '2016-03-01', 5),
(1, 2, '2016-05-02', 6),
(2, 3, '2017-06-25', 1),
(3, 1, '2016-03-02', 0),
(3, 4, '2018-07-03', 5);

select player_id, min(event_date) as first_login
from activity
group by player_id;

----------------------------------------------------------------------------

-- Drop table if it already exists
DROP TABLE IF EXISTS Activity;

-- Create Activity table
CREATE TABLE Activity (
    player_id INT,
    device_id INT,
    event_date DATE,
    games_played INT,
    PRIMARY KEY (player_id, event_date)
);

-- Insert data into Activity table
INSERT INTO Activity (player_id, device_id, event_date, games_played) VALUES
(1, 2, '2016-03-01', 5),
(1, 2, '2016-05-02', 6),
(2, 3, '2017-06-25', 1),
(3, 1, '2016-03-02', 0),
(3, 4, '2018-07-03', 5);

select player_id, device_id
from (
select player_id, device_id, event_date,
row_number() over (partition by player_id order by event_date) as rn
from activity
) a
where rn = 1;

-- solution 2

select player_id, device_id
from activity
where (player_id, event_date) in 
			(select player_id, min(event_date)
				from activity
                group by player_id
			);

----------------------------------------------------------------------------

-- Drop table if it already exists
DROP TABLE IF EXISTS Activity;

-- Create Activity table
CREATE TABLE Activity (
    player_id INT,
    device_id INT,
    event_date DATE,
    games_played INT,
    PRIMARY KEY (player_id, event_date)
);

-- Insert data into Activity table
INSERT INTO Activity (player_id, device_id, event_date, games_played) VALUES
(1, 2, '2016-03-01', 5),
(1, 2, '2016-05-02', 6),
(1, 3, '2017-06-25', 1),
(3, 1, '2016-03-02', 0),
(3, 4, '2018-07-03', 5);


select player_id,event_date,
sum(games_played) over (partition by player_id order by event_date) as games_played_so_far
from activity;

----------------------------------------------------------------------------

DROP TABLE IF EXISTS Activity;

-- Create Activity table
CREATE TABLE Activity (
    player_id INT,
    device_id INT,
    event_date DATE,
    games_played INT,
    PRIMARY KEY (player_id, event_date)
);

INSERT INTO Activity (player_id, device_id, event_date, games_played) VALUES
(1, 2, '2016-03-01', 5),
(1, 2, '2016-03-02', 6),
(2, 3, '2017-06-25', 1),
(3, 1, '2016-03-02', 0),
(3, 4, '2018-07-03', 5);

select  
round(sum(case when t1.event_date = t2.first_event + 1 then 1 else 0 end) / count(distinct t1.player_id), 2) as fraction
from activity t1
join
(select player_id , min(event_date) as first_event
from activity 
group by player_id) t2
on t1.player_id = t2.player_id;


-- Solution 2 

select round(count(distinct t2.player_id) / count(distinct t1.player_id), 2)  as fraction
from (select player_id , min(event_date) as first_event
from activity 
group by player_id) t1
left join activity t2
on t1.player_id = t2.player_id and date_add(first_event, interval 1 Day) = event_date;

----------------------------------------------------------------------------

-- Drop the table if it already exists
DROP TABLE IF EXISTS Employee;

-- Create the table
CREATE TABLE Employee (
    Id INT PRIMARY KEY,
    Company VARCHAR(10),
    Salary INT
);

-- Insert sample data
INSERT INTO Employee (Id, Company, Salary) VALUES
(1, 'A', 2341),
(2, 'A', 341),
(3, 'A', 15),
(4, 'A', 15314),
(5, 'A', 451),
(6, 'A', 513),
(7, 'B', 15),
(8, 'B', 13),
(9, 'B', 1154),
(10, 'B', 1345),
(11, 'B', 1221),
(12, 'B', 234),
(13, 'C', 2345),
(14, 'C', 2645),
(15, 'C', 2645),
(16, 'C', 2652),
(17, 'C', 65);


with ranked_salary as
(
select id, company, salary, 
dense_rank() over (partition by company order by salary) rnk,
count(*) over (partition by company) as total_count
from Employee
) 
select company,
		round(avg(salary), 1) as MedianSalary
from ranked_salary
where rnk in (floor((total_count+1)/2), ceil((total_count + 1)/2) )
group by company
;

select company, count(id) as cnt from employee group by company;

select (451 + 513) / 2 as num;
----------------------------------------------------------------------------

DROP TABLE IF EXISTS Employee;

CREATE TABLE Employee (
    Id INT PRIMARY KEY,
    Name VARCHAR(50),
    Department VARCHAR(50),
    ManagerId INT
);

INSERT INTO Employee (Id, Name, Department, ManagerId) VALUES
(101, 'John', 'A', NULL),
(102, 'Dan', 'A', 101),
(103, 'James', 'A', 101),
(104, 'Amy', 'A', 101),
(105, 'Anne', 'A', 101),
(106, 'Ron', 'B', 101);

select * from employee;

select m.name
from employee e
join employee m
on e.managerID = m.id
group by 1
having count(e.id) >= 5;

----------------------------------------------------------------------------

-- 571. Find Median Given Frequency of Numbers 

DROP TABLE IF EXISTS Numbers;

CREATE TABLE Numbers (
    Number INT PRIMARY KEY,
    Frequency INT
);

INSERT INTO Numbers (Number, Frequency) VALUES
(0, 7),
(1, 1),
(2, 3),
(3, 1);


----------------------------------------------------------------------------


----------------------------------------------------------------------------




----------------------------------------------------------------------------




----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------




----------------------------------------------------------------------------




----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------



----------------------------------------------------------------------------




----------------------------------------------------------------------------



----------------------------------------------------------------------------





----------------------------------------------------------------------------





----------------------------------------------------------------------------





----------------------------------------------------------------------------