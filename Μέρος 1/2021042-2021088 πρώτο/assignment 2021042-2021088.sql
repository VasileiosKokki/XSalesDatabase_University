----------- 2ο ερώτημα – Δημιουργία και γέμισμα σχήματος -----------

ALTER SESSION SET NLS_DATE_FORMAT='DD/MM/YY HH:MI';

DROP TABLE Customers;
DROP TABLE Products;
DROP TABLE Orders;
DROP TABLE delayed_orders;
DROP TABLE end_profits;
DROP TABLE deficit;
DROP TABLE profit;

------------------  Creation of table Customers  -------------------

CREATE TABLE Customers AS
SELECT
    id AS customer_id,
    gender,
    get_age_group(birth_date, SYSDATE) AS age_group,
    fix_status(marital_status) AS marital_status,
    get_income_level(income_level) AS income_level
FROM XSALES.customers;

DESC Customers;
SELECT * FROM Customers;

-------------------  Creation of table Products  -------------------

CREATE TABLE Products AS
SELECT
    p.identifier AS product_id,
    p.name AS productname,
    c.name AS categoryname,
    TO_NUMBER(REPLACE(p.list_price, ',', '.'), '9999.99') AS
    list_price
FROM XSALES.products p JOIN XSALES.categories c
ON p.subcategory_reference = c.id;

DESC Products;
SELECT * FROM Products;

--------------------  Creation of table Orders  --------------------

CREATE TABLE Orders AS
SELECT
    o.id AS order_id,
    i.product_id,
    o.customer_id,
    TO_NUMBER(TRUNC(SYSDATE) - TRUNC(i.order_date)) -
    TO_NUMBER(TRUNC(SYSDATE) - TRUNC(o.order_finished)) AS
    days_to_process,
    i.amount AS price,
    i.cost,
    o.channel
FROM XSALES.orders o
JOIN XSALES.order_items i ON o.id = i.order_id;

DESC Orders;
SELECT * FROM Orders;

---------------------------  Functions  ---------------------------

-- get_age_group function
CREATE OR REPLACE FUNCTION get_age_group(birth_date DATE, current_date DATE)
RETURN VARCHAR2
IS
    age NUMBER;
    age_group VARCHAR2(50);
BEGIN
    age := FLOOR(MONTHS_BETWEEN(current_date, birth_date) / 12);
    
    IF age < 40 THEN
      age_group := 'under 40';
    ELSIF age >= 40 AND age < 50 THEN
      age_group := '40-50';
    ELSIF age >= 50 AND age < 60 THEN
      age_group := '50-60';
    ELSIF age >= 60 AND age <= 70 THEN
      age_group := '60-70';
    ELSE
      age_group := 'above 70';
    END IF;

    RETURN age_group;
END;
/

-- get_income_level function
CREATE OR REPLACE FUNCTION get_income_level(income_level VARCHAR2)
RETURN VARCHAR2
IS
    cleaned_income_level VARCHAR2(50);
    income_value NUMBER;
    income_level2 VARCHAR2(50);
BEGIN
    -- Remove non-numeric characters and spaces
    cleaned_income_level := TRIM(TRANSLATE(income_level,
    'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz: ,',' '));
    
    -- Check if the cleaned input contains a hyphen
    -- indicating a range
    IF INSTR(cleaned_income_level, '-') > 0 THEN
        -- Split the cleaned input into two values
        income_value := TO_NUMBER(SUBSTR(cleaned_income_level,
        INSTR(cleaned_income_level, '-') + 1));
    ELSE
        -- If it's not a range, return the cleaned value as is
        income_value := TO_NUMBER(cleaned_income_level);
    END IF;
    
    IF income_value <= 129999 THEN
        income_level2 := 'low';
    ELSIF income_value <= 249999 THEN
        income_level2 := 'medium';
    ELSIF income_value >= 250000 THEN
        income_level2 := 'high';
    ELSE
        income_level2 := 'unknown';
    END IF;
    
    RETURN income_level2;
END;
/

-- fix_status function
CREATE OR REPLACE FUNCTION fix_status(marital_status VARCHAR2)
RETURN VARCHAR2
IS
    fixed_status VARCHAR2(50);
BEGIN
    CASE marital_status
        WHEN 'Widowed' THEN
            fixed_status := 'single';
        WHEN 'Separ.' THEN
            fixed_status := 'single';
        WHEN 'divorced' THEN
            fixed_status := 'single';
        WHEN 'NeverM' THEN
            fixed_status := 'single';
        WHEN 'Single' THEN
            fixed_status := 'single';
        WHEN 'single' THEN
            fixed_status := 'single';
        WHEN 'Divorc.' THEN
            fixed_status := 'single';
        ELSE
            fixed_status := 'married';
    END CASE;
    
    IF marital_status IS NULL THEN
        fixed_status := 'unknown';
    END IF;

    RETURN fixed_status;
END;
/

---------- Ερώτημα 2ο – Εντοπισμός ζημιογόνων παραγγελιών ----------
-- 2.1
CREATE TABLE delayed_orders AS
SELECT order_id, MAX(calculate_delay(days_to_process)) AS delay FROM Orders
GROUP BY order_id ORDER BY order_id;

SELECT * FROM delayed_orders;

CREATE OR REPLACE FUNCTION calculate_delay(days_to_process NUMBER)
RETURN NUMBER
IS
    delay NUMBER;
BEGIN
    -- calculate the delay
    delay := GREATEST(days_to_process - 20, 0);
    RETURN delay;
END;
/

-- 2.2
CREATE TABLE end_profits AS
SELECT e.order_id, SUM(o.price - o.cost - (p.list_price * 0.001 * e.delay)) AS profit FROM
(
SELECT 
    order_id,
    CASE 
         WHEN MAX(days_to_process) - 20 < 0 THEN 0
         ELSE MAX(days_to_process) - 20 
    END as delay
FROM Orders
GROUP BY order_id
) e JOIN Orders o ON e.order_id = o.order_id JOIN Products p ON o.product_id = p.product_id
GROUP BY e.order_id ORDER BY e.order_id;

SELECT * FROM end_profits;

-- 2.3
CREATE TABLE deficit(
    orderid NUMBER,
    customerid NUMBER,
    channel VARCHAR(20),
    deficit NUMBER
);

DESC deficit;
SELECT * FROM deficit;

CREATE TABLE profit(
    orderid NUMBER,
    customerid NUMBER,
    channel VARCHAR(20),
    profit NUMBER
);

DESC profit;
SELECT * FROM profit;
    
DECLARE
    -- Declare a cursor
    CURSOR c_cursor IS
    SELECT o.order_id, o.customer_id, o.channel,
    SUM(o.price - o.cost - (p.list_price * 0.001 * e.delay)) FROM
    (
    SELECT 
        order_id,
        CASE 
             WHEN MAX(days_to_process) - 20 < 0 THEN 0
             ELSE MAX(days_to_process) - 20 
        END AS delay
    FROM Orders
    GROUP BY order_id
    ) e JOIN Orders o ON e.order_id = o.order_id
    JOIN Products p ON o.product_id = p.product_id
    GROUP BY o.order_id, o.customer_id, o.channel;
    
    -- Declare temporary variables
    temp_order_id Orders.order_id%TYPE;
    temp_customer_id Orders.customer_id%TYPE;
    temp_channel Orders.channel%TYPE;
    temp_end_profit NUMBER;
BEGIN
    OPEN c_cursor;
    
    LOOP
    FETCH c_cursor INTO temp_order_id, temp_customer_id,
    temp_channel, temp_end_profit;

    -- Exit the loop if no more rows to fetch
    EXIT WHEN c_cursor%NOTFOUND;

    IF temp_end_profit < 0 THEN
        -- If negative, insert into deficit table
        INSERT INTO deficit (orderid, customerid, channel, deficit)
        VALUES (temp_order_id, temp_customer_id, temp_channel,
        ABS(temp_end_profit));
    ELSIF temp_end_profit > 0 THEN
        -- If positive, insert into profit table
        INSERT INTO profit (orderid, customerid, channel, profit)
        VALUES (temp_order_id, temp_customer_id, temp_channel,
        temp_end_profit);
    END IF;

    END LOOP;
    CLOSE c_cursor;
END;
/

-- 2.4
SELECT pr.gender, pr.total_profit, df.total_deficit FROM
(SELECT c.gender, SUM(profit) AS total_profit FROM profit
JOIN Customers c ON customerid = c.customer_id GROUP BY c.gender) pr
JOIN
(SELECT c.gender, SUM(deficit) AS total_deficit FROM deficit
JOIN Customers c ON customerid = c.customer_id GROUP BY c.gender) df
ON pr.gender = df.gender;

-- 2.5
SELECT pr.channel, pr.total_profit, df.total_deficit FROM
(SELECT channel, SUM(profit) AS total_profit FROM profit GROUP BY channel) pr
JOIN
(SELECT channel, SUM(deficit) AS total_deficit FROM deficit GROUP BY channel) df
ON pr.channel = df.channel;

--------- 3ο Ερώτημα – Βελτιστοποίηση ερωτήματος ισότητας ---------

--select count(*) as real_rows from
--(select order_id, price-cost,days_to_process
--from products p join orders o on o.product_id=p.product_id
--join customers c on o.customer_id=c.customer_id
--where p.categoryname='Accessories' and o.channel='Internet'
--and c.gender='Male' and c.income_level='high' and
--days_to_process=0);

explain plan for
select order_id, price-cost,days_to_process
from products p join orders o on o.product_id=p.product_id
join customers c on o.customer_id=c.customer_id
where p.categoryname='Accessories' and o.channel='Internet'
and c.gender='Male' and c.income_level='high' and
days_to_process=0;

delete from plan_table;
select * from table(DBMS_XPLAN.DISPLAY);

SELECT id, parent_id, depth, operation, options, object_name, access_predicates,
filter_predicates, projection, cost, cpu_cost, io_cost, cardinality FROM plan_table
CONNECT BY PRIOR id = parent_id START WITH id = 0 ORDER BY id;

drop index orders_idx;
drop index customers_idx;
drop index products_idx;

-- most important single-column index
create index orders_idx on orders(days_to_process);

create index orders_idx on orders(days_to_process, channel, customer_id, product_id); -- most important composite index
create index customers_idx on customers(income_level, gender, customer_id);
create index products_idx on products(categoryname, product_id);

-------- 4ο Ερώτημα – Βελτιστοποίηση ερωτήματος ανισότητας --------

explain plan for
select order_id, price-cost,days_to_process
from products p join orders o on o.product_id=p.product_id
join customers c on o.customer_id=c.customer_id
where p.categoryname='Accessories' and o.channel='Internet'
and c.gender='Male' and c.income_level='high' and
days_to_process>100;

delete from plan_table;
select * from table(DBMS_XPLAN.DISPLAY);

SELECT id, parent_id, depth, operation, options, object_name, access_predicates,
filter_predicates, projection, cost, cpu_cost, io_cost, cardinality FROM plan_table
CONNECT BY PRIOR id = parent_id START WITH id = 0 ORDER BY id;

drop index customers_idx;
drop index products_idx;

create unique index customers_idx on customers(income_level, gender, customer_id);
create unique index products_idx on products(categoryname, product_id);