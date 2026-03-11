-- ============================================================
-- Developer 6 - Advanced SQL Queries & Solutions
-- ============================================================

-- ----------------------------------------------------------
-- Q1: Customers who ordered ALL products
-- ----------------------------------------------------------
SELECT c.customer_id, c.first_name, c.last_name
FROM CUSTOMERS c
WHERE NOT EXISTS (
    SELECT p.product_id
    FROM PRODUCTS p
    WHERE NOT EXISTS (
        SELECT 1
        FROM ORDER_ITEMS oi
        JOIN ORDERS o ON o.order_id = oi.order_id
        WHERE o.customer_id = c.customer_id
          AND oi.product_id = p.product_id
    )
);

-- ----------------------------------------------------------
-- Q2: Products never ordered
-- ----------------------------------------------------------
SELECT p.product_id, p.product_name
FROM PRODUCTS p
LEFT JOIN ORDER_ITEMS oi ON p.product_id = oi.product_id
WHERE oi.item_id IS NULL;

-- ----------------------------------------------------------
-- Q3: Orders containing both Mouse (id=2) and Keyboard (id=3)
-- ----------------------------------------------------------
SELECT o.order_id, o.order_date, o.total_amount
FROM ORDERS o
WHERE o.order_id IN (
    SELECT oi.order_id FROM ORDER_ITEMS oi WHERE oi.product_id = 2
)
AND o.order_id IN (
    SELECT oi.order_id FROM ORDER_ITEMS oi WHERE oi.product_id = 3
);

-- ----------------------------------------------------------
-- Q4: Customers with unpaid (Pending) orders
-- ----------------------------------------------------------
SELECT DISTINCT c.customer_id, c.first_name, c.last_name, o.order_id, o.total_amount
FROM CUSTOMERS c
JOIN ORDERS o    ON c.customer_id = o.customer_id
JOIN PAYMENTS py ON o.order_id    = py.order_id
WHERE py.status = 'Pending';

-- ----------------------------------------------------------
-- Q5: Top 5 spending customers
-- ----------------------------------------------------------
SELECT c.customer_id, c.first_name, c.last_name,
       SUM(o.total_amount) AS total_spent
FROM CUSTOMERS c
JOIN ORDERS o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC
LIMIT 5;

-- ----------------------------------------------------------
-- Q6: Running payment totals per customer (window function)
-- ----------------------------------------------------------
SELECT c.customer_id, c.first_name,
       py.payment_date,
       py.amount,
       SUM(py.amount) OVER (
           PARTITION BY c.customer_id
           ORDER BY py.payment_date
           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS running_total
FROM CUSTOMERS c
JOIN ORDERS o    ON c.customer_id = o.customer_id
JOIN PAYMENTS py ON o.order_id    = py.order_id
WHERE py.status = 'Completed'
ORDER BY c.customer_id, py.payment_date;

-- ----------------------------------------------------------
-- Q7: Customer ranking by total spending (RANK / DENSE_RANK)
-- ----------------------------------------------------------
SELECT customer_id, first_name, last_name, total_spent,
       RANK()       OVER (ORDER BY total_spent DESC) AS rank_pos,
       DENSE_RANK() OVER (ORDER BY total_spent DESC) AS dense_rank_pos
FROM (
    SELECT c.customer_id, c.first_name, c.last_name,
           SUM(o.total_amount) AS total_spent
    FROM CUSTOMERS c
    JOIN ORDERS o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
) AS spending;

-- ----------------------------------------------------------
-- Q8: Window function – order sequence per customer
-- ----------------------------------------------------------
SELECT c.customer_id, c.first_name,
       o.order_id, o.order_date, o.total_amount,
       ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_date)     AS order_seq,
       LAG(o.total_amount) OVER (PARTITION BY c.customer_id ORDER BY o.order_date)  AS prev_order_amt,
       LEAD(o.total_amount) OVER (PARTITION BY c.customer_id ORDER BY o.order_date) AS next_order_amt
FROM CUSTOMERS c
JOIN ORDERS o ON c.customer_id = o.customer_id
ORDER BY c.customer_id, o.order_date;

-- ----------------------------------------------------------
-- Q9: CTE – Monthly revenue summary
-- ----------------------------------------------------------
WITH monthly_revenue AS (
    SELECT DATE_FORMAT(o.order_date, '%Y-%m') AS order_month,
           SUM(o.total_amount) AS revenue
    FROM ORDERS o
    GROUP BY order_month
)
SELECT order_month, revenue,
       SUM(revenue) OVER (ORDER BY order_month) AS cumulative_revenue
FROM monthly_revenue
ORDER BY order_month;

-- ----------------------------------------------------------
-- Q10: CTE – Customers with above-average spending
-- ----------------------------------------------------------
WITH customer_spending AS (
    SELECT c.customer_id, c.first_name, c.last_name,
           SUM(o.total_amount) AS total_spent
    FROM CUSTOMERS c
    JOIN ORDERS o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
),
avg_spending AS (
    SELECT AVG(total_spent) AS avg_spent FROM customer_spending
)
SELECT cs.customer_id, cs.first_name, cs.last_name,
       cs.total_spent, a.avg_spent
FROM customer_spending cs
CROSS JOIN avg_spending a
WHERE cs.total_spent > a.avg_spent
ORDER BY cs.total_spent DESC;
