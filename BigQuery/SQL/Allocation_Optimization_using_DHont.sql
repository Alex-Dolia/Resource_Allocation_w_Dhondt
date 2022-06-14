WITH  
demand_weight AS (
      SELECT  'shop_1' AS shop, 0.2 AS demand_weight UNION ALL
      SELECT  'shop_2',         0.3                  UNION ALL
      SELECT  'shop_3',         0.5 
),
shop_demand   AS(
      SELECT  'shop_1' AS shop, 150 AS demand_quantity UNION ALL
      SELECT  'shop_2',         500                    UNION ALL
      SELECT  'shop_3',         300
),
demand AS (
      SELECT '11/06/2022' AS day, 'Camden'                 AS London_Borough, False AS Shop_1,  False AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '11/06/2022' AS day, 'Ealing'                 AS London_Borough, False AS Shop_1,  False AS Shop_2,  True   AS Shop_3 UNION ALL
      SELECT '11/06/2022' AS day, 'Greenwich'              AS London_Borough, False AS Shop_1,  True  AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '11/06/2022' AS day, 'Hounslow'               AS London_Borough, False AS Shop_1,  True  AS Shop_2,  True   AS Shop_3 UNION ALL
      SELECT '11/06/2022' AS day, 'Richmond upon Thames'   AS London_Borough, True  AS Shop_1,  False AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '11/06/2022' AS day, 'Hammersmith and Fulham' AS London_Borough, True  AS Shop_1,  False AS Shop_2,  True   AS Shop_3 UNION ALL
      SELECT '11/06/2022' AS day, 'Kensington and Chelsea' AS London_Borough, True  AS Shop_1,  True  AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '11/06/2022' AS day, 'City of Westminster'    AS London_Borough, True  AS Shop_1,  True  AS Shop_2,  True   AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'Camden'                 AS London_Borough, False AS Shop_1,  False AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'Ealing'                 AS London_Borough, False AS Shop_1,  False AS Shop_2,  True   AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'Greenwich'              AS London_Borough, False AS Shop_1,  True  AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'Hounslow'               AS London_Borough, False AS Shop_1,  True  AS Shop_2,  True   AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'Richmond upon Thames'   AS London_Borough, True  AS Shop_1,  False AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'Hammersmith and Fulham' AS London_Borough, True  AS Shop_1,  False AS Shop_2,  True   AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'Kensington and Chelsea' AS London_Borough, True  AS Shop_1,  True  AS Shop_2,  False  AS Shop_3 UNION ALL
      SELECT '12/06/2022' AS day, 'City of Westminster'    AS London_Borough, True  AS Shop_1,  True  AS Shop_2,  True   AS Shop_3 
),
-- SUPPLY
prob_of_supply_per_borough AS (
SELECT 'Camden'                 AS London_Borough, 0.118 AS supplier_1, 0.118 AS supplier_2 UNION ALL
SELECT 'Ealing'                 AS London_Borough, 0.157 AS supplier_1, 0.157 AS supplier_2 UNION ALL
SELECT 'Greenwich'              AS London_Borough, 0.118 AS supplier_1, 0.118 AS supplier_2 UNION ALL
SELECT 'Hounslow'               AS London_Borough, 0.220 AS supplier_1, 0.220 AS supplier_2 UNION ALL
SELECT 'Richmond upon Thames'   AS London_Borough, 0.078 AS supplier_1, 0.078 AS supplier_2 UNION ALL
SELECT 'Hammersmith and Fulham' AS London_Borough, 0.192 AS supplier_1, 0.192 AS supplier_2 UNION ALL
SELECT 'Kensington and Chelsea' AS London_Borough, 0.039 AS supplier_1, 0.039 AS supplier_2 UNION ALL
SELECT 'City of Westminster'    AS London_Borough, 0.078 AS supplier_1, 0.078 AS supplier_2
),
daily_total_supplied_quantity AS(
SELECT '11/06/2022' AS day, 255 AS supplier_1, 255 AS supplier_2 UNION ALL 
SELECT '12/06/2022' AS day, 510 AS supplier_1, 255 AS supplier_2 
),
un_pivot_demand AS (
SELECT day, London_Borough, shop, indicator FROM demand
UNPIVOT(indicator FOR shop IN (shop_1, shop_2, shop_3))
), 
deman_weight_un_pivot_demand AS (
SELECT day, 
       London_Borough,  
       u.shop,  
       indicator, 
       demand_weight,
       CASE WHEN indicator THEN demand_weight ELSE 0 END AS indicator_weight,

FROM un_pivot_demand AS u
INNER JOIN demand_weight AS d ON d.shop=u.shop
),
row_normalised_deman_weight_un_pivot_demand AS (
 SELECT
       day, 
       London_Borough,  
       shop,  
       indicator, 
       demand_weight,
       indicator_weight,
       CASE WHEN SUM(indicator_weight) OVER(PARTITION BY day, London_Borough) > 0 
                 THEN indicator_weight / SUM(indicator_weight) OVER(PARTITION BY day, London_Borough) 
                 ELSE 0
       END AS row_normalise_weight 
FROM deman_weight_un_pivot_demand
),
un_pivot_prob_of_supply_per_borough AS (
SELECT London_Borough, supplier, probability FROM prob_of_supply_per_borough
UNPIVOT(probability FOR supplier IN (supplier_1, supplier_2))
),
un_pivot_daily_total_supplied_quantity AS (
SELECT day, supplier, quantity FROM daily_total_supplied_quantity
UNPIVOT(quantity FOR supplier IN (supplier_1, supplier_2))
),
-- START DHondt_daily_supplier
initial_step_of_DHondt_daily_supplier AS
(
SELECT day, 
       London_Borough,  
       d.supplier, 
       probability, 
       quantity, 
       probability * (quantity + 1)        AS fraction,
       FLOOR(probability * (quantity + 1)) AS res,

       SUM( FLOOR(probability * (quantity + 1)) ) OVER (PARTITION BY day, d.supplier)              AS total_res,
       quantity -  SUM( FLOOR(probability * (quantity + 1)) ) OVER (PARTITION BY day, d.supplier)  AS n, 
       ROW_NUMBER() OVER (PARTITION BY day, d.supplier ORDER BY ( probability * (quantity + 1) - FLOOR(probability * (quantity + 1)) ) DESC) AS th_largest_reminder, 
       ROW_NUMBER() OVER (PARTITION BY day, d.supplier ORDER BY probability)                                                                 AS smallest_weight
FROM un_pivot_daily_total_supplied_quantity AS d
INNER JOIN un_pivot_prob_of_supply_per_borough AS p ON d.supplier = p.supplier
),
final_step_of_DHondt_daily_supplier as (
SELECT *, 
       CAST(CASE WHEN (n = -1) AND (smallest_weight      = 1) THEN res - 1
                 WHEN (n >  0) AND (th_largest_reminder <= n) THEN res + 1
                 ELSE res
            END AS INT64) AS daily_supplier_allocation
FROM initial_step_of_DHondt_daily_supplier
),
-- END   DHondt_daily_supplier
-- START DHondt_supply_AS_demand
initial_step_of_DHondt_supply_AS_demand AS (
SELECT
       r.day,
       r.London_Borough,  
       shop,  
       row_normalise_weight,
       daily_supplier_allocation, 


       row_normalise_weight * (daily_supplier_allocation + 1)        AS fraction,
       FLOOR(row_normalise_weight * (daily_supplier_allocation + 1)) AS res,

       SUM( FLOOR(row_normalise_weight * (daily_supplier_allocation + 1)) ) OVER (PARTITION BY r.day, r.London_Borough)                                AS total_res,
       daily_supplier_allocation  -  SUM( FLOOR(row_normalise_weight * (daily_supplier_allocation + 1)) ) OVER (PARTITION BY r.day, r.London_Borough)  AS n, 
       ROW_NUMBER() OVER (PARTITION BY r.day, r.London_Borough ORDER BY ( row_normalise_weight * (daily_supplier_allocation + 1) - FLOOR(row_normalise_weight * (daily_supplier_allocation + 1)) ) DESC) AS th_largest_reminder, 
       ROW_NUMBER() OVER (PARTITION BY r.day, r.London_Borough ORDER BY row_normalise_weight) AS smallest_weight
FROM row_normalised_deman_weight_un_pivot_demand AS r
INNER JOIN (
              SELECT  day,  
                      London_Borough,
                      SUM(daily_supplier_allocation) AS daily_supplier_allocation
              FROM final_step_of_DHondt_daily_supplier
              GROUP BY day, London_Borough
           ) as s ON r.day = s.day AND r.London_Borough = s.London_Borough 
WHERE row_normalise_weight > 0
),
final_step_of_DHondt_supply_AS_demand  as (
SELECT *, 
       CAST(CASE WHEN (n = -1) AND (smallest_weight      = 1) THEN res - 1
                 WHEN (n >  0) AND (th_largest_reminder <= n) THEN res + 1
                 ELSE res
            END AS INT64) AS allocation
FROM initial_step_of_DHondt_supply_AS_demand
),
available_demand AS (
SELECT
       s.shop,
       demand_quantity,
       supply_quantity,
       CASE WHEN demand_quantity < supply_quantity 
                 THEN demand_quantity
                 ELSE supply_quantity
       END  AS available_demand
FROM  shop_demand as s
INNER JOIN ( 
              SELECT 
                      shop,
                      SUM(allocation) AS supply_quantity
              FROM final_step_of_DHondt_supply_AS_demand
              GROUP BY shop 
   
           ) AS f ON s.shop = f.shop 
),
pre_final AS
(
    SELECT 
            day,
            London_Borough, 
            f.shop, 
            allocation * 1.0 / SUM(allocation) OVER(PARTITION BY f.shop) AS column_normalised_weight, 
            available_demand
    FROM  final_step_of_DHondt_supply_AS_demand AS f
    INNER JOIN available_demand  AS a on f.shop = a.shop
),
initial_step_of_DHondt_final AS (
SELECT
       day,
       London_Borough,  
       shop,  
       column_normalised_weight,
       available_demand, 

       column_normalised_weight * (available_demand + 1)           AS fraction,
       FLOOR( column_normalised_weight * (available_demand + 1)  ) AS res,

       SUM( FLOOR( column_normalised_weight * (available_demand + 1)) ) OVER (PARTITION BY shop)                                AS total_res,
       available_demand -  SUM( FLOOR( column_normalised_weight * (available_demand + 1)) ) OVER (PARTITION BY shop)   AS n, 
       ROW_NUMBER() OVER (PARTITION BY shop ORDER BY ( column_normalised_weight * (available_demand + 1) - FLOOR(  column_normalised_weight * (available_demand + 1)  )) DESC) AS th_largest_reminder, 
       ROW_NUMBER() OVER (PARTITION BY shop ORDER BY column_normalised_weight) AS smallest_weight
FROM pre_final
),
final_step_of_DHondt_final  as (
SELECT *, 
       CAST(CASE WHEN (n = -1) AND (smallest_weight      = 1) THEN res - 1
                 WHEN (n >  0) AND (th_largest_reminder <= n) THEN res + 1
                 ELSE res
            END AS INT64) AS allocation
FROM initial_step_of_DHondt_final
),
pivot_final AS(
SELECT * FROM
(
  -- #1 from_item
  SELECT 
    day,
    London_Borough,
    shop,
    allocation
  FROM final_step_of_DHondt_final
)
PIVOT
(
  -- #2 aggregate
  AVG(allocation) AS allocation
  -- #3 pivot_column
  FOR shop in ('shop_1', 'shop_2', 'shop_3')
)
ORDER BY day, London_Borough
)

--select * from  Demand_Weight
--select * from  shop_demand
--select * from prob_of_supply_per_borough
--select * from un_pivot_demand
--select * from deman_weight_un_pivot_demand
--select * from row_normalised_deman_weight_un_pivot_demand
--select * from un_pivot_prob_of_supply_per_borough
--select * from un_pivot_daily_total_supplied_quantity
--select * from unpivot_daily_supplier_probability_quantity
-- select * from DHondt_daily_supplier
--select * from final_step_of_DHondt_supply_AS_demand ORDER BY day, London_Borough, shop
--select * from available_demand 
--select * from final_step_of_DHondt_final
--select sum(allocation_shop_1), sum( allocation_shop_2), sum(allocation_shop_3) from pivot_final
--select day,	London_Borough, allocation_shop_1, allocation_shop_2,  allocation_shop_3 from pivot_final
SELECT 
      day,	
      London_Borough, 
      CAST(IFNULL(allocation_shop_1, 0) AS INT64) AS shop_1, 
      CAST(IFNULL(allocation_shop_2, 0) AS INT64) AS shop_2,  
      CAST(IFNULL(allocation_shop_3, 0) AS INT64) AS shop_3
FROM pivot_final
ORDER BY day, London_Borough 