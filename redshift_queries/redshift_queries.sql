--Units Sold and Sales Revenue Trends (Hourly, Daily, Weekly)
-- Hourly
SELECT DATE_TRUNC('hour', timestamp) AS hour_timestamp,
       SUM(quantity) AS units_sold,
       SUM(quantity * unit_price) AS sales_revenue
FROM sales_transactions
GROUP BY hour_timestamp;

-- Daily
SELECT DATE_TRUNC('day', timestamp) AS day_timestamp,
       SUM(quantity) AS units_sold,
       SUM(quantity * unit_price) AS sales_revenue
FROM sales_transactions
GROUP BY day_timestamp;

-- Weekly
SELECT DATE_TRUNC('week', timestamp) AS week_timestamp,
       SUM(quantity) AS units_sold,
       SUM(quantity * unit_price) AS sales_revenue
FROM sales_transactions
GROUP BY week_timestamp;
----------------------------------------------------------

-- Product Wise Units Sold and Sales Revenue:
SELECT p.name AS product_name,
       SUM(st.quantity) AS units_sold,
       SUM(st.quantity * st.unit_price) AS sales_revenue
FROM sales_transactions st
INNER JOIN products p ON st.productid = p.productid
GROUP BY p.name;
----------------------------------------------------------

-- Real-Time Inventory Levels and Products at Risk of Stockouts:
WITH ProductSalesAndStock AS (
  SELECT iu.productid,
         SUM(iu.quantitychange) AS current_stock,
         AVG(SUM(CASE WHEN iu.quantitychange < 0 THEN iu.quantitychange * (-1) END)) AS avg_units_sold_in_day
  FROM inventory_updates iu
  GROUP BY iu.productid
)
SELECT ps.productid,
       ps.current_stock,
       ps.avg_units_sold_in_day
FROM ProductSalesAndStock ps;

----------------------------------------------------------

-- Sales Through Rate:
SELECT p.productid,
       SUM(CASE WHEN iu.quantitychange < 0 THEN iu.quantitychange * (-1) END) / SUM(CASE WHEN iu.quantitychange > 0 THEN iu.quantitychange END) AS sell_through_rate
FROM inventory_updates iu
INNER JOIN products p ON iu.productid = p.productid
GROUP BY p.productid;
----------------------------------------------------------

-- Category Wise Total Revenue and Units Sold:
SELECT p.category AS product_category,
       SUM(st.quantity) AS units_sold,
       SUM(st.quantity * st.unit_price) AS sales_revenue
FROM sales_transactions st
INNER JOIN products p ON st.productid = p.productid
GROUP BY product_category, st.storeid;

-- Stores ordered based on their revenue
SELECT st.storeId AS storeId,
       SUM(st.unitPrice * st.quantity) as store_revenue
FROM sales_transactions st
GROUP BY storeId
ORDER BY store_revenue DESC;
