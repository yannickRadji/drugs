# SQL

The purpose here is to do clear SQL queries

## First part

The query has been slightly change to use a date interval in 2020 as from the required sample result seems to be on this interval: 
```sql
-- GET REVENUES BY DATE FOR DECO & MEUBLE BETWEEN 2020-01-01 & 2020-12-31
SELECT [date]
      ,SUM(prod_price*prod_qty) AS ventes
  FROM TRANSACTIONS
  WHERE [date] BETWEEN CAST('2020-01-01' AS DATE) AND CAST('2020-12-31' AS DATE) --CAST IN ORDER TO WORK ON ANY PLATFORM
  GROUP BY  [date]
  ORDER BY [date]
```

## Second part

The query as requested:
```sql
-- GET REVENUES BY CLIENT FOR DECO & MEUBLE BETWEEN 2020-01-01 & 2020-12-31
WITH sub_query_revenues AS (
	SELECT T.client_id,
		CASE WHEN product_type = 'MEUBLE' THEN SUM(T.prod_price*T.prod_qty) END AS ventes_meuble,
		CASE WHEN product_type = 'DECO' THEN SUM(T.prod_price*T.prod_qty)  END AS ventes_deco
	FROM TRANSACTIONS as T
	INNER JOIN PRODUCT_NOMENCLATURE AS P ON P.product_id=T.prod_id
	WHERE T.[date] BETWEEN  CAST('2020-01-01' AS DATE) AND CAST('2020-12-31' AS DATE)
	GROUP BY T.client_id, P.[product_type]
			),
			-- SUM BY CLIENT TO GET RID OFF NULL VALUES DUE TO product_type IN sub_query_revenues
			sub_query_clean AS (
	SELECT client_id,
		SUM(ventes_meuble) AS chiffre_affaire_meuble,
		SUM(ventes_deco) AS chiffre_affaire_hors_meuble
	FROM sub_query_revenues
	GROUP BY client_id
	)
SELECT * FROM sub_query_clean
```
