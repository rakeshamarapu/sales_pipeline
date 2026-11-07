-- Query 1: Display orders with their customer and product names.
SELECT f.order_id, d.full_date AS order_date, c.customer_name,
       p.product_name, f.quantity, f.unit_price, f.total_amount,
       f.order_status, f.source_system
FROM fact_sales AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
JOIN dim_product AS p ON p.product_key = f.product_key
JOIN dim_date AS d ON d.date_key = f.date_key
ORDER BY d.full_date, f.order_id;

-- Query 2: Calculate sales for each customer, excluding cancelled/returned orders.
SELECT c.customer_id, c.customer_name, SUM(f.total_amount) AS total_sales
FROM fact_sales AS f
JOIN dim_customer AS customer_version ON customer_version.customer_key = f.customer_key
JOIN dim_customer AS c
    ON c.customer_id = customer_version.customer_id AND c.is_current = TRUE
WHERE f.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY c.customer_id, c.customer_name
ORDER BY total_sales DESC;

-- Query 3: Find the five top-selling products by units sold.
SELECT p.product_id, p.product_name, SUM(f.quantity) AS units_sold
FROM fact_sales AS f
JOIN dim_product AS p ON p.product_key = f.product_key
WHERE f.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY p.product_id, p.product_name
ORDER BY units_sold DESC, p.product_id
LIMIT 5;

-- Query 4: Calculate monthly revenue.
SELECT d.year, d.month, SUM(f.total_amount) AS monthly_sales
FROM fact_sales AS f
JOIN dim_date AS d ON d.date_key = f.date_key
WHERE f.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Query 5: Find the highest revenue-generating product.
SELECT p.product_id, p.product_name, SUM(f.total_amount) AS total_revenue
FROM fact_sales AS f
JOIN dim_product AS p ON p.product_key = f.product_key
WHERE f.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC, p.product_id
LIMIT 1;

-- Query 6: Count orders by status.
SELECT order_status, COUNT(*) AS order_count
FROM fact_sales
GROUP BY order_status
ORDER BY order_count DESC, order_status;

-- Query 7: Find current customers with no matching sales orders.
SELECT c.customer_id, c.customer_name, c.email
FROM dim_customer AS c
WHERE c.is_current = TRUE
  AND NOT EXISTS (
      SELECT 1
      FROM fact_sales AS f
            JOIN dim_customer AS customer_version
                ON customer_version.customer_key = f.customer_key
            WHERE customer_version.customer_id = c.customer_id
  )
ORDER BY c.customer_id;

-- Query 8: Calculate sales by customer city.
SELECT c.city, SUM(f.total_amount) AS total_revenue
FROM fact_sales AS f
JOIN dim_customer AS c ON c.customer_key = f.customer_key
WHERE f.order_status NOT IN ('Cancelled', 'Returned')
GROUP BY c.city
ORDER BY total_revenue DESC, c.city;

-- Query 9: Use a CTE to calculate monthly sales.
WITH monthly_sales AS (
    SELECT d.year, d.month, SUM(f.total_amount) AS total_sales
    FROM fact_sales AS f
    JOIN dim_date AS d ON d.date_key = f.date_key
    WHERE f.order_status NOT IN ('Cancelled', 'Returned')
    GROUP BY d.year, d.month
)
SELECT year, month, total_sales
FROM monthly_sales
ORDER BY year, month;

-- Query 10: Rank products by revenue with a window function.
WITH product_revenue AS (
    SELECT p.product_id, p.product_name, SUM(f.total_amount) AS revenue
    FROM fact_sales AS f
    JOIN dim_product AS p ON p.product_key = f.product_key
    WHERE f.order_status NOT IN ('Cancelled', 'Returned')
    GROUP BY p.product_id, p.product_name
)
SELECT product_id, product_name, revenue,
       RANK() OVER (ORDER BY revenue DESC) AS revenue_rank
FROM product_revenue
ORDER BY revenue_rank, product_id;