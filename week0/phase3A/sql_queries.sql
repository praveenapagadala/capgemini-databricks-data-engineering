
CREATE TABLE customers_raw (
    customer_id INT,
    name VARCHAR(50),
    city VARCHAR(50),
    age INT
);

INSERT INTO customers_raw (customer_id, name, city, age) VALUES
(1, 'Ravi', 'Hyderabad', 25),
(2, NULL, 'Chennai', 32),
(NULL, 'Arun', 'Hyderabad', 28),
(4, 'Meena', NULL, 30),
(4, 'Meena', NULL, 30),
(5, 'John', 'Bangalore', -5);


create table customers_clean as
select DISTINCT
    customer_id,
    IFNULL(name, 'Unknown') as name,
    IFNULL(city, 'Unknown') as city,
    age
from customers_raw
where customer_id IS NOT NULL
AND age >= 0;

select * from customers_clean;

select COUNT(*) AS row_count_after_cleaning
from customers_clean;
