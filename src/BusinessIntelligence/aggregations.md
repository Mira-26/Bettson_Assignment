Usefull aggregations one might use for reporting purposes

1. Identify best-sellers
find the ProductID (top 10) which had the biggest amount of items sold

SELECT
  p.ProductID,
  p.Description,
  SUM(f.Quantity) AS TotalUnitsSold
FROM Fact_Invoice f
JOIN Dim_Product p ON f.ProductKey = p.ProductKey
GROUP BY p.ProductID, p.Description
ORDER BY TotalUnitsSold DESC
LIMIT 10;


2. Identify most profitable months
find the 3 most profitable months (and its respective year)

SELECT
  d.Year,
  d.Month,
  SUM(f.Quantity * f.Price) AS TotalRevenue
FROM Fact_Invoice f
JOIN Dim_Date d
  ON f.DateKey = d.DateKey
GROUP BY
  d.Year,
  d.Month
ORDER BY
  TotalRevenue DESC
LIMIT 3;
;

3. Calculate the profit of a specific month
Calculate the profit of December 2009

SELECT
  d.Year,
  SUM(f.Quantity * f.Price) AS DecemberRevenue
FROM Fact_Invoice f
JOIN Dim_Date d
  ON f.DateKey = d.DateKey
WHERE
  d.Month = 12 AND d.Year = 2009
GROUP BY
  d.Year
ORDER BY
  d.Year
;