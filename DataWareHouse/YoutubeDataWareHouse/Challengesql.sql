select ci.city, sum(p.amount) as revenue from payment p
join customer c on p.customer_id = c.customer_id
join address a on c.address_id = a.address_id
join city ci on a.city_id = ci.city_id
group by ci.city
order by sum(p.amount) desc