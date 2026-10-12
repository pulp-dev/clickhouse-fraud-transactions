-- Категория наибольшей транзакции по каждому штату
SELECT
    us_state,
    argMax(cat_id, amount) AS top_category,
    max(amount)            AS max_amount
FROM transactions
GROUP BY us_state
ORDER BY us_state
