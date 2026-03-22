-- 11. Simple UPDATE Statement
-- Use Case: Updating employee salary based on their department.
UPDATE Employees
SET salary = salary * 1.10
WHERE department_id = 3;

-- 12. DELETE with a WHERE Clause
-- Use Case: Removing orders older than a specific date.
DELETE FROM Orders
WHERE order_date < '2023-01-01';

-- 13. IN Operator
-- Use Case: Selecting products with specific IDs.
SELECT *
FROM Products
WHERE product_id IN (1, 3, 5, 7);

-- 14. Basic String Functions
-- Use Case: Extracting domain from an email address.
SELECT 
    email,
    SUBSTRING(email, LOCATE('@', email) + 1) AS domain
FROM Customers;

-- 15. Simple Arithmetic Operations
-- Use Case: Calculating total price with tax.
SELECT product_id, price, price * 1.08 AS price_with_tax
FROM Products;

-- 16. Conditional Expressions with NULLIF
-- Use Case: Avoiding division by zero.
SELECT 
    product_id, 
    price,
    price / NULLIF(stock, 0) AS price_per_unit
FROM Products;

-- 17. Basic String Concatenation
-- Use Case: Creating full names from first and last names.
SELECT 
    first_name || ' ' || last_name AS full_name
FROM Employees;

-- 18. Using TRIM() to Remove Whitespace
-- Use Case: Cleaning up data entry errors.
SELECT TRIM(leading ' ' FROM employee_name) AS cleaned_name
FROM Employees;

-- 19. Simple DATE_FORMAT()
-- Use Case: Formatting date of birth.
SELECT 
    employee_id, 
    DATE_FORMAT(date_of_birth, '%M %d, %Y') AS formatted_dob
FROM Employees;

-- 20. CAST() for Data Type Conversion
-- Use Case: Converting salary to string.
SELECT 
    employee_id, 
    CAST(salary AS CHAR) AS salary_string
FROM Employees;

-- 1. Grouping with HAVING clause
-- Use Case: Finding departments with more than 5 employees.
SELECT department_id, COUNT(*) AS employee_count
FROM Employees
GROUP BY department_id
HAVING COUNT(*) > 5;

-- 2. CASE Statement for Conditional Logic
-- Use Case: Categorizing employees based on salary.
SELECT employee_id,
       CASE
           WHEN salary > 100000 THEN 'High'
           WHEN salary > 50000 THEN 'Medium'
           ELSE 'Low'
       END AS salary_category
FROM Employees;

-- 3. Subqueries in SELECT
-- Use Case: Finding total sales for each customer.
SELECT customer_id,
       (SELECT SUM(amount) FROM Sales WHERE Sales.customer_id = Customers.customer_id) AS total_sales
FROM Customers;

-- 4. Window Function with ROW_NUMBER()
-- Use Case: Finding the top-ranked employee by salary in each department.
SELECT employee_id, department_id, salary,
       ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) AS rank
FROM Employees;

-- 5. Self-Join
-- Use Case: Finding employees who report to the same manager.
SELECT e1.employee_id, e2.employee_id AS colleague_id
FROM Employees e1
JOIN Employees e2 ON e1.manager_id = e2.manager_id
WHERE e1.employee_id <> e2.employee_id;

-- 6. UNION vs UNION ALL
-- Use Case: Combining data from two tables (with and without duplicates).
SELECT product_id FROM Products_A
UNION
SELECT product_id FROM Products_B;

-- 7. Date Arithmetic
-- Use Case: Calculating employee tenure in years.
SELECT employee_id, 
       DATEDIFF(CURDATE(), hire_date) / 365 AS tenure_years
FROM Employees;

-- 8. Conditional Aggregation
-- Use Case: Counting employees based on gender in each department.
SELECT department_id,
       SUM(CASE WHEN gender = 'Male' THEN 1 ELSE 0 END) AS male_count,
       SUM(CASE WHEN gender = 'Female' THEN 1 ELSE 0 END) AS female_count
FROM Employees
GROUP BY department_id;

-- 9. EXISTS Subquery
-- Use Case: Finding customers who have placed at least one order.
SELECT customer_id
FROM Customers c
WHERE EXISTS (SELECT 1 FROM Orders o WHERE o.customer_id = c.customer_id);

-- 10. Using COALESCE for NULL Handling
-- Use Case: Replacing NULL values with a default value.
SELECT employee_id, COALESCE(bonus, 0) AS adjusted_bonus
FROM Employees;

-- 11. Conditional Logic with IIF() (Alternative to CASE)
-- Use Case: Categorizing salaries quickly.
SELECT 
    employee_id,
    IIF(salary > 100000, 'High', 'Low') AS salary_category
FROM Employees;

-- 12. Aggregate Functions with DISTINCT
-- Use Case: Counting unique products sold.
SELECT COUNT(DISTINCT product_id) AS unique_products_sold
FROM Sales;

-- 13. GROUP_CONCAT() for Aggregating Strings
-- Use Case: Creating a comma-separated list of employees per department.
SELECT 
    department_id,
    GROUP_CONCAT(employee_name ORDER BY employee_name SEPARATOR ', ') AS employee_list
FROM Employees
GROUP BY department_id;

-- 14. HAVING Clause with Aggregates
-- Use Case: Finding customers who spent more than a specified amount.
SELECT 
    customer_id, 
    SUM(amount) AS total_spent
FROM Sales
GROUP BY customer_id
HAVING total_spent > 5000;

-- 15. PIVOT with CASE and Aggregation
-- Use Case: Pivoting product sales per month.
SELECT 
    product_id,
    SUM(CASE WHEN MONTH(sale_date) = 1 THEN amount ELSE 0 END) AS January,
    SUM(CASE WHEN MONTH(sale_date) = 2 THEN amount ELSE 0 END) AS February
FROM Sales
GROUP BY product_id;

-- 16. Complex Joins with Multiple Conditions
-- Use Case: Joining orders with customers based on multiple criteria.
SELECT o.order_id, c.customer_name
FROM Orders o
JOIN Customers c ON o.customer_id = c.customer_id AND o.order_date > '2023-01-01';

-- 17. SUBSTRING_INDEX() for Splitting Strings
-- Use Case: Extracting the first name from a full name.
SELECT 
    full_name,
    SUBSTRING_INDEX(full_name, ' ', 1) AS first_name
FROM Employees;

-- 18. CROSS JOIN for Cartesian Products
-- Use Case: Generating all combinations of product and region.
SELECT p.product_id, r.region_name
FROM Products p
CROSS JOIN Regions r;

-- 19. ROLLUP for Subtotals
-- Use Case: Calculating total sales per category and overall total.
SELECT category_id, SUM(amount) AS total_sales
FROM Sales
GROUP BY category_id WITH ROLLUP;

-- 20. Conditional COUNT() with CASE
-- Use Case: Counting orders based on their status.
SELECT 
    COUNT(CASE WHEN status = 'shipped' THEN 1 END) AS shipped_orders,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) AS pending_orders
FROM Orders;


-- 1. Recursive CTE for Hierarchical Data
-- Use Case: Navigating an organizational hierarchy to find all subordinates of a manager.
WITH RECURSIVE hierarchy AS (
    SELECT employee_id, manager_id, 1 AS level
    FROM Employees
    WHERE manager_id = 1
    UNION ALL
    SELECT e.employee_id, e.manager_id, h.level + 1
    FROM Employees e
    JOIN hierarchy h ON e.manager_id = h.employee_id
)
SELECT employee_id, level
FROM hierarchy;

-- 2. Windowed Aggregation
-- Use Case: Calculating cumulative sales over time.
SELECT 
    order_date,
    SUM(amount) OVER (ORDER BY order_date) AS running_total
FROM Orders;

-- 3. Identifying Gaps in Data
-- Use Case: Finding gaps in order dates.
SELECT 
    order_date,
    order_date - ROW_NUMBER() OVER (ORDER BY order_date) AS group_id
FROM Orders;

-- 4. Dynamic Pivot Table
-- Use Case: Pivoting sales data by month.
SELECT 
    customer_id,
    SUM(CASE WHEN MONTH(order_date) = 1 THEN amount ELSE 0 END) AS Jan,
    SUM(CASE WHEN MONTH(order_date) = 2 THEN amount ELSE 0 END) AS Feb
FROM Orders
GROUP BY customer_id;

-- 5. Using LAG() for Previous Row Comparison
-- Use Case: Calculating daily sales changes.
SELECT 
    sales_date,
    sales,
    sales - LAG(sales) OVER (ORDER BY sales_date) AS change
FROM Sales;

-- 6. Conditional Joins
-- Use Case: Joining tables based on conditional logic.
SELECT e.employee_id, d.department_name
FROM Employees e
LEFT JOIN Departments d ON (e.salary > 50000 AND e.department_id = d.department_id);

-- 7. Ranking with Ties Using RANK()
-- Use Case: Finding top-performing employees with ties.
SELECT employee_id, salary,
       RANK() OVER (ORDER BY salary DESC) AS rank
FROM Employees;

-- 8. Advanced Date Manipulation
-- Use Case: Calculating time intervals and categorizing them.
SELECT employee_id, 
       CASE 
           WHEN TIMESTAMPDIFF(YEAR, hire_date, CURDATE()) > 10 THEN 'Veteran'
           ELSE 'Newcomer'
       END AS category
FROM Employees;

-- 9. Correlated Subquery
-- Use Case: Finding employees whose salary is above average in their department.
SELECT employee_id, department_id, salary
FROM Employees e
WHERE salary > (SELECT AVG(salary) FROM Employees WHERE department_id = e.department_id);

-- 10. Complex Grouping with ROLLUP
-- Use Case: Creating subtotals and grand totals.
SELECT department_id, SUM(salary) AS total_salary
FROM Employees
GROUP BY department_id WITH ROLLUP;

-- 11. Multi-Level Recursive Queries
-- Use Case: Finding all employees reporting up to a specific manager.
WITH RECURSIVE subordinates AS (
    SELECT employee_id, manager_id
    FROM Employees
    WHERE manager_id = 1
    UNION ALL
    SELECT e.employee_id, e.manager_id
    FROM Employees e
    JOIN subordinates s ON e.manager_id = s.employee_id
)
SELECT employee_id
FROM subordinates;

-- 12. Advanced Window Functions with NTILE()
-- Use Case: Distributing employees into 4 quartiles by salary.
SELECT 
    employee_id, 
    salary,
    NTILE(4) OVER (ORDER BY salary DESC) AS quartile
FROM Employees;

-- 13. INTERSECT Operation
-- Use Case: Finding customers present in both tables (if supported by your DBMS).
SELECT customer_id FROM Sales
INTERSECT
SELECT customer_id FROM Orders;

-- 14. Recursive Calculation for Sequences
-- Use Case: Generating a Fibonacci sequence up to 10 iterations.
WITH RECURSIVE fibo AS (
    SELECT 0 AS num
    UNION ALL
    SELECT 1
    UNION ALL
    SELECT (SELECT MAX(num) FROM fibo) + (SELECT MAX(num) FROM (SELECT num FROM fibo LIMIT 1 OFFSET (SELECT COUNT(*) - 2 FROM fibo)) fib)
    LIMIT 10
)
SELECT num FROM fibo;

-- 15. Dynamic SQL Execution (if supported by your DBMS)
-- Use Case: Executing a dynamic query based on input parameters (example in pseudo-code).
-- DECLARE @query VARCHAR(MAX) = 'SELECT * FROM ' + @tableName;
-- EXEC(@query);

-- 16. LAG() with Complex Conditions
-- Use Case: Calculating changes in metrics with conditional flags.
SELECT 
    sales_date,
    sales,
    sales - LAG(sales) OVER (ORDER BY sales_date) AS sales_change,
    CASE WHEN LAG(sales) OVER (ORDER BY sales_date) IS NULL THEN 'Initial' ELSE 'Repeat' END AS change_type
FROM Sales;

-- 17. Advanced GROUPING SETS
-- Use Case: Custom groupings with subtotals and combinations.
SELECT department_id, manager_id, SUM(salary)
FROM Employees
GROUP BY GROUPING SETS ((department_id), (manager_id), ());

-- 18. Complex Aggregation with Multiple Window Functions
-- Use Case: Calculating moving averages and cumulative sums in one query.
SELECT 
    sales_date,
    SUM(sales) OVER (ORDER BY sales_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_sum,
    SUM(sales) OVER (ORDER BY sales_date) AS cumulative_sum
FROM Sales;

-- 19. Advanced Conditional Joins
-- Use Case: Joining with conditions based on ranges.
SELECT e.employee_id, d.department_name
FROM Employees e
LEFT JOIN Departments d ON e.salary BETWEEN d.min_salary AND d.max_salary;

-- 20. Unpivoting Data (Manual Method)
-- Use Case: Transforming columns into rows.
SELECT customer_id, 'January' AS month, january_sales AS sales
FROM MonthlySales
UNION ALL
SELECT customer_id, 'February', february_sales
FROM MonthlySales;


