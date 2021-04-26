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