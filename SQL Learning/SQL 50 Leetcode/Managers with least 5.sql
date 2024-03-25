SELECT * from Employee;

INSERT INTO Employee (id, name, department, managerId) VALUES (101, null, 'A', null);
INSERT INTO Employee (id, name, department, managerId) VALUES (102, null, 'A', 101);
INSERT INTO Employee (id, name, department, managerId) VALUES (103, null, 'A', 101);
INSERT INTO Employee (id, name, department, managerId) VALUES (104, null, 'A', 101);
INSERT INTO Employee (id, name, department, managerId) VALUES (105, null, 'A', 101);
INSERT INTO Employee (id, name, department, managerId) VALUES (106, null, 'B', 101);


SELECT name, count(managerID) from Employee group by name having count(managerID) > 4

SELECT a.name from Employee a
left join Employee b on a.id = b.managerID
group by a.name
having count(b.name) >= 5

SELECT a.name from Employee a
join Employee b on a.id = b.managerID
group by a.name
having count(b.name) >= 5

EXEC sp_rename 'Employees', 'Employee';

with result as (
SELECT a.id from Employee a
join Employee b on a.id = b.managerID
group by a.id
having count(b.name) >= 5)
select name from result

SELECT a.id from Employee a
join Employee b on a.id = b.managerID
group by a.id
having count(b.id) >= 5

with result as (
SELECT a.id, a.name from Employee a
left join Employee b on a.id = b.managerID
group by a.name, a.id
having count(b.id) >= 5)
select name from result


select name from Employee where id in (select managerid
FROM Employee group by managerid having count(managerid) >=5 );