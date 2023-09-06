DROP TABLE a_orders;
create table a_orders
(
    order_id integer NOT NULL,
    order_date date,
    product_name varchar(100),
    total_price decimal(6,2) UNIQUE,
    discount integer check (discount<=20) UNIQUE,
    payment_method varchar(20) check (payment_method in ('UPI', 'CREDIT CARD')) DEFAULT 'UPI'
    PRIMARY KEY(order_id, product_name)
);

insert into a_orders values(1, '2022-10-01', 'Shoes', 132.5, 5, 'UPI');
insert into a_orders values(2, '2022-10-01', 'Shoes', 2.5, 14, 'UPI');
insert into a_orders values(3, '2022-10-01', 'Shoes', 13.5, 10, 'UPI');
insert into a_orders values(1, '2022-10-01', 'Shoess', 44.5, 3, 'UPI');

select * from a_orders;

delete from a_orders where product_name = 'Shoess';


