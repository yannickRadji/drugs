-- GET REVENUES BY DATE FOR DECO & MEUBLE BETWEEN 2020-01-01 & 2020-12-31
SELECT [date]
      ,SUM(prod_price*prod_qty) AS ventes
  FROM TRANSACTIONS
  WHERE [date] BETWEEN CAST('2020-01-01' AS DATE) AND CAST('2020-12-31' AS DATE) --CAST IN ORDER TO WORK ON ANY PLATFORM
  GROUP BY  [date]
  ORDER BY [date]