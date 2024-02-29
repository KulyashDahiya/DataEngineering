SELECT region, count(1) as Total_Returns
from orders o
join returns r on o.order_id = r.order_id
GROUP by region;

