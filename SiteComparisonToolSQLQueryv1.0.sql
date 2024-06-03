----- Site Comparison Tool Query -----

WITH VIEW_1 AS (
SELECT dc_name,
       hellofresh_week,
       SUM(CASE WHEN metric_id=129 THEN (numerator_value*60)/denominator_value END) AS worked_mpb,
       SUM(CASE WHEN metric_id=50 THEN numerator_value/denominator_value END) AS production_error_rate,
       SUM(CASE WHEN metric_id=9 THEN numerator_value/denominator_value END) AS production_asl_error_rate,
       SUM(CASE WHEN metric_id=8 THEN numerator_value/denominator_value END) AS production_kit_error_rate,
       (1-(SUM(CASE WHEN metric_id=114 THEN numerator_value END)/SUM(CASE WHEN metric_id=111 THEN numerator_value END))) AS absence_rate,
       (SUM(CASE WHEN metric='Labour Order Fulfilled  | Agency' THEN numerator_value END)/SUM(CASE WHEN metric='Labour Order Actual  | Agency' THEN numerator_value END)) AS agency_fill_rate,
       SUM(CASE WHEN metric_id=50 THEN denominator_value END) AS boxes_shipped
FROM materialized_views.isa__wbr_dashboard_layer_view
WHERE hellofresh_year = 2024
    AND metric IN ('Worked | Hours' ---MPB
    ,'Production | Error Rate [W-1]','Production | P&P | Assembly | Error Rate [W-1]','Production | P&P | Kitting | Error Rate [W-1]' --- Errors
    ,'Labour Order Fulfilled | HF','Labour Order Actual | HF' --- Absence Rate
    ,'Labour Order Fulfilled  | Agency','Labour Order Actual  | Agency' --- Agency Fill Rate
                  )
    --AND dc_name IN ('Barleben','Verden')--AND dc_name<>'Windmill'
GROUP BY 1,2
ORDER BY 1,2
)

----- Waste Inventory FCMS -----

--this cte provides basic calendar data
, calendar as (
SELECT distinct hellofresh_week
        ,hellofresh_month
        ,hellofresh_quarter
        ,hellofresh_year
FROM dimensions.date_dimension
WHERE hellofresh_year>=2022
)

-- cte to retrieve the supplier of each po_id
, PO AS (
    SELECT
        po_id
        ,MAX(supplier) as Supplier
    FROM fcms_mis.mi_po as POs
    WHERE TO_DATE(FROM_UNIXTIME(CAST(FLOOR(POs.event_time/1000) AS INT))) >= TO_DATE(DATEADD(WEEK,-60,now()))
    GROUP BY 1
)

-- this cte gives all the events in fcms of stock deletions with waste related reasons (donation, disposal and resale to processor)
, inventory AS(
    SELECT
        UPPER(ST.dc) as DC
        ,CAST(DD.hellofresh_week AS STRING) as hf_week
        ,IFNULL(CASE WHEN UPPER(PO.Supplier) LIKE '% BX' OR UPPER(PO.Supplier) LIKE '% VE' THEN SUBSTRING(PO.Supplier,1,LENGTH(PO.Supplier)-3) ELSE PO.Supplier END,'Unmapped') Supplier
        ,ST.prod_code as sku_code
        ,"Inventory" as source
        ,LEFT(ST.prod_code, 3) as sku_category
        ,ST.tm_id as tmid
        ,ST.po_id
        ,SUM(ST.qty) as waste_units
    FROM fcms_mis.mi_stock ST
    LEFT JOIN dimensions.date_dimension AS DD
        ON left(from_unixtime(CAST(FLOOR(ST.event_time/1000) AS INT)),10) = DD.date_string_backwards
    LEFT JOIN PO ON ST.po_id = PO.po_id
    WHERE ST.oel_class = "OEL_STOCK_DELETE" --"OEL_STOCK_QTY_CHANGE" is included by global query as well but adds only 41 rows as of now and with a lot of data quality issues (e.g. negative quantities from_value-to_value)
    AND ST.dc IN ('bx','ve')
    AND st.qty<10000000 --exclude extreme wrong cases (e.g.,PHF-22-10322-3 in 2023-W43 =229010560 units
    AND UPPER(ST.sla_reason_id) IN ('DIS','DON','RES')
    AND TO_DATE(FROM_UNIXTIME(CAST(FLOOR(ST.event_time/1000) AS INT))) >= TO_DATE(DATEADD(WEEK,-60,now()))
    AND LEFT(ST.prod_code,3) NOT IN ('VBM', 'VPM', 'XYZ') --- consumables (ink, paper, gloves, etc) and old verpackungsmaterial (PCK) category
    AND (case when (dd.hellofresh_week='2024-W13' and st.prod_code='BAK-11-102333-2') then st.qty<3000000 else true end ) --exclude error from 2024-W13 with almost 4Million units
    GROUP BY 1,2,3,4,5,6,7,8--,9
)

-- cte to retrieve sku data
, sku as (
    SELECT  DISTINCT code
                ,name as sku_name
                ,packaging_type
                ,CASE WHEN packaging_type like 'Meal Kit%' THEN 'YES' ELSE 'NO' END as in_meal_kit
    FROM materialized_views.procurement_services_culinarysku
    WHERE market='dach'
)

-- cte to retrieve sku prices according to the supplier splits (averaged across suppliers for the week and dc combination)
, prices as (
    SELECT splits.culinary_sku_code as sku_code
            ,splits.culinary_sku_id as sku_id
            ,splits.dc
            ,splits.hellofresh_week
            ,avg(splits.price) as price
    FROM materialized_views.procurement_services_csku_suppliersplits splits
    LEFT JOIN materialized_views.procurement_services_suppliersku as supplier
    ON supplier.supplier_id=splits.supplier_id
    AND supplier.culinary_sku_id=splits.culinary_sku_id
    WHERE splits.market='dach'
    GROUP BY 1,2,3,4
)

-- alternative #1 if previous cte is not enough to find a price for a specific week (here we get the average price of the sku according to the supplier splits. regardless of the week)
, prices_avg as (
    SELECT splits.culinary_sku_code as sku_code
            ,avg(splits.price) as price
    FROM materialized_views.procurement_services_csku_suppliersplits splits
    LEFT JOIN materialized_views.procurement_services_suppliersku as supplier
    ON supplier.supplier_id=splits.supplier_id
    AND supplier.culinary_sku_id=splits.culinary_sku_id
    WHERE splits.market='dach'
    AND splits.hellofresh_week>='2021-W01'
    GROUP BY 1
)

-- alternative #2 for sku price: price of the last order found in OT
, last_ot_price as (
    SELECT *
    FROM (
        SELECT sku
                ,unit_price as price
                ,row_number() over (partition by sku order by created_at desc) as created_rank
        FROM materialized_views.int_scm_analytics_ot_consolidated_view
        WHERE `WEEK` >= '2017-W01'
        AND dc_name IN ('HelloFresh DE - Verden (VE)','DE - Barleben (BX)','HelloFresh DE - Verden')
        AND item_quantity != 0
        AND item_total_price > 0.0001)t
    WHERE t.created_rank=1
)

-- alternative #3 for sku price: static price
, static_prices as (
    SELECT sp.hellofresh_week
        , cs.code sku_code
        , avg(sp.price) as price
    FROM materialized_views.procurement_services_staticprices sp
    JOIN materialized_views.procurement_services_culinarysku cs
    ON sp.culinary_sku_id = cs.id
    WHERE market = 'dach'
    GROUP BY 1,2
)

-- this cte retrieves the picklist data, which is necessary to reach the sku level with the mk overproduction data
, picklist as (
SELECT distinct picklist.hellofresh_week
    ,ucase(region_code) as region
    ,picklist.slot_number as recipe_index
    ,picklist.code as sku_code
    ,picklist.size
    ,picklist.pick_count as picks
    ,picklist.name as sku_name
FROM materialized_views.isa_services_menu_picklist AS picklist
WHERE 1=1
AND picklist.hellofresh_week >='2022-W01'
AND picklist.region_code ='deat'
AND picklist.unique_recipe_code NOT LIKE '%-TM-%' --exclude thermomix rows from picklist
)

-- this cte merges the two data sources of waste into one: waste derived from inventory deletions and mealkit overproduction
, total_waste as (
--waste from overproduction
SELECT
         mk.dc
        ,mk.week as hf_week
        ,'' as supplier
        , picklist.sku_code
        ,'overkitting' as source
        --,'DONATION' as destination  --assumption: all mealkit overproduction goes to donation
        ,left(picklist.sku_code,3) as sku_category
        ,'' as tmid
        ,'' as po_id
        , SUM( picklist.picks* CASE WHEN picklist.size = 2 then mk_2p
                                    WHEN picklist.size = 3 then mk_3p
                                    WHEN picklist.size = 4 then mk_4p
                                END
            ) as waste_units
FROM public_dach_oa_gsheets.dach_mk_overproduction mk
LEFT JOIN picklist
ON mk.slot = picklist.recipe_index
AND mk.week = picklist.hellofresh_week
WHERE 1=1
GROUP BY 1,2,3,4,5,6,7,8--,9

UNION ALL

--waste from inventory deletions
SELECT *
FROM inventory

)

-- enrich previous cte with sku, price and calendar data
, total_waste_enriched as (
SELECT distinct total.dc
       ,total.hf_week
       ,total.supplier
       ,total.sku_code
       ,total.waste_units
       ,total.source
       --,total.destination
       ,total.sku_category
       ,total.tmid
       ,sku.sku_name
       ,sku.packaging_type
       ,coalesce(prices.price, prices_avg.price, last_ot_price.price, static_prices.price) as price --fallback logic to have as many prices as possible
        ,calendar.hellofresh_month
        ,calendar.hellofresh_quarter
        ,calendar.hellofresh_year
FROM total_waste as total
LEFT JOIN sku
ON sku.code = total.sku_code
LEFT JOIN prices
ON total.sku_code = prices.sku_code
AND total.DC=prices.dc
AND total.hf_week = prices.hellofresh_week
LEFT JOIN prices_avg
ON prices_avg.sku_code = total.sku_code
LEFT JOIN last_ot_price
ON last_ot_price.sku = total.sku_code
LEFT JOIN static_prices
ON total.sku_code = static_prices.sku_code
AND total.hf_week = static_prices.hellofresh_week
LEFT JOIN calendar
ON calendar.hellofresh_week = total.hf_week
WHERE 1=1
AND total.waste_units>0 --some rows have 0 units because of the mk overproduction files, which has all slots even if with 0 units
AND (CASE WHEN total.source='Inventory' THEN true
          WHEN total.source='overkitting' THEN sku.in_meal_kit='YES' END)=TRUE --criteria to only get the skus that are packed in the mealkit if the waste source is mealkit overproduction
)

, waste_inventory AS (
SELECT dc,
       hf_week,
       source,
       SUM(waste_units * price) AS total_cost
FROM total_waste_enriched
WHERE hellofresh_year=2024
GROUP BY 1,2,3
ORDER BY 1,2,3
)

----- Revenue Box Count
, boxes_shipped as (
    SELECT bs.hellofresh_delivery_week
    ,bs.box_id
    ,CASE WHEN prod.is_mealbox=true then 1 else 0 end as mealbox
    ,pdl.distribution_center
    ,sum(bs.full_retail_price_eur) as full_retail_price_eur
    ,sum(bs.full_retail_price_eur + bs.shipping_fee_incl_vat_eur) as full_retail_price_inc_ship_eur
    FROM fact_tables.boxes_shipped bs
    JOIN dimensions.product_dimension as prod
    ON prod.sk_product = bs.fk_product
    --AND prod.is_mealbox=true ---mealboxes only
    LEFT JOIN materialized_views.isa_parent_box_lookup parent
    ON  parent.box_id=bs.box_id
    LEFT JOIN uploads.opsbi_de_pdl_sequence_trackingdata_at_de pdl
    ON pdl.parent_boxid = parent.parent_box_id
    WHERE bs.country in ('DE','AT')
    AND bs.hellofresh_delivery_week between (SELECT MIN(HELLOFRESH_WEEK) FROM DIMENSIONS.DATE_DIMENSION WHERE TO_DATE(DATE_STRING_BACKWARDS) >= TO_DATE(DATEADD(WEEK,-60,now())))
                            AND (SELECT MIN(HELLOFRESH_WEEK) FROM DIMENSIONS.DATE_DIMENSION WHERE TO_DATE(DATE_STRING_BACKWARDS) >= TO_DATE(now()))
    GROUP BY 1,2,3,4
)

, revenue_boxcount AS (
SELECT bs.hellofresh_delivery_week
        ,case when bs.distribution_center='BY' THEN 'BX'
             when bs.distribution_center is null THEN 'VE' --<100 boxes per cant be mapped, let's assume they were produced in VE
            ELSE bs.distribution_center end as distribution_center
        ,sum(mealbox) as boxes
        ,sum(full_retail_price_eur) as revenue_frp
        ,sum(full_retail_price_inc_ship_eur) as revenue_frp_incl_ship
FROM boxes_shipped bs
LEFT JOIN calendar
ON calendar.hellofresh_week = bs.hellofresh_delivery_week
GROUP BY 1,2
)

----- This cte is to get the Waste % Gross Revenue and Waste CPB Metrics	
, waste_gr_cpb AS (
SELECT CASE WHEN a.dc='BX' THEN 'Barleben' WHEN a.dc='VE' THEN 'Verden' END AS distribution_center,
       a.hf_week,
       SUM(a.total_cost/b.revenue_frp) AS waste_percentage_GR,
       MAX(CASE WHEN a.source='overkitting' THEN a.total_cost/b.boxes END) AS overkitting_cpb,
       MAX(CASE WHEN a.source='Inventory' THEN a.total_cost/b.boxes END) AS inventory_cpb,
       SUM(a.total_cost/b.boxes) AS waste_CPB
FROM waste_inventory AS a
LEFT JOIN revenue_boxcount AS b
    ON a.dc=b.distribution_center
    AND a.hf_week=b.hellofresh_delivery_week
WHERE a.hf_week LIKE '2024%' AND b.hellofresh_delivery_week LIKE '2024%'
GROUP BY 1,2
ORDER BY 1,2
)

----- this cte is to get the CPB Euro metric	
, cpb_eur AS (
SELECT
    CASE WHEN country_group='BENELUX' THEN 'Prismalaan'
         WHEN country_group='FR' THEN 'Lisses'
         WHEN country_group='Ireland' THEN 'Dublin'
         WHEN country_group='IT' THEN 'Milan'
         WHEN country_group='Spain' THEN 'Madrid'
         WHEN country='NZ' THEN 'Auckland'
         WHEN country IN ('SE','SE-unknown','DK') THEN 'Bjuv'
         WHEN country = 'NZ' THEN 'Auckland'
         WHEN country IN ('GB','UK','GN') THEN
             CASE WHEN dc IN ('GR','Banbury')THEN 'Banbury'
                  WHEN dc IN ('Beehive','BV') THEN 'Nuneaton'--'Beehive'
	              WHEN dc = 'TO' THEN 'Derby'
                  ELSE 'GB-undefined' END
         WHEN dc IN ('AT','AT-unknown','DE') THEN 'Verden'
         WHEN dc IN ('CH','CH-unknown') THEN 'Koelliken'
         WHEN dc IN ('Beehive','BV') THEN 'Nuneaton'
         WHEN dc IN ('MO','NO','NO-unknown') THEN 'Oslo'
    ELSE dc END AS dc,
    hellofresh_week AS HelloFresh_Week, 
    SUM(total_pnp_costs_eur)/SUM(box_count) AS cpb_eur
FROM materialized_views.global_pc2_dashboard
WHERE cluster = 'International'
    AND dc NOT IN ('TFB WA','DE-unknown','TK-unknown','TV-unknown','AO-unknown','AU-unknown','Derby','YE-unknown','GB-undefined','Derby','Moss')
    AND hellofresh_week >= '2023-W01'
GROUP BY 1,2
ORDER BY 1,2
)

--- source for the assembly throughput metric for Barleben      
, bx_asl_tph_1 AS (
SELECT mapped_dc,
    area_left_date,
    hellofresh_week,
    area_left_hour,
    SUM(boxes_finished_per_minute) AS boxes_finished_per_minute
FROM materialized_views.hybrid_line_boxes_per_area_per_minute
WHERE area_left_date BETWEEN '2024-01-01' AND '2024-12-31' AND area_left_hour NOT IN (0,1,2,22,23)
GROUP BY 1,2,3,4
)

, bx_asl_tph_2 AS (
SELECT mapped_dc,
       hellofresh_week,
       AVG(boxes_finished_per_minute) AS boxes_finished_per_minute
FROM bx_asl_tph_1
WHERE mapped_dc='Barleben' AND hellofresh_week>='2024-W01'
GROUP BY 1,2
ORDER BY 1,2
)

--- source for the assembly throughput metric for Verden   
, ve_asl_tph AS (
SELECT
  mapped_dc,
  hellofresh_week,
  weighted_throughput_per_dc AS assembly_throughput_ve
FROM materialized_views.prod_db_executive_summary
WHERE hellofresh_week>='2024-W01' AND mapped_dc='Verden'
ORDER BY 1,2
)

SELECT a.*,
       b.waste_percentage_GR,
       b.overkitting_cpb,
       b.inventory_cpb,
       b.waste_CPB,
       c.cpb_eur,
       d.boxes_finished_per_minute,
       e.assembly_throughput_ve
FROM VIEW_1 AS a
LEFT JOIN waste_gr_cpb AS b
    ON a.dc_name=b.distribution_center
    AND a.hellofresh_week=b.hf_week
LEFT JOIN cpb_eur AS c
    ON a.dc_name=c.dc
    AND a.hellofresh_week=c.HelloFresh_Week
LEFT JOIN bx_asl_tph_2 AS d
    ON a.dc_name=d.mapped_dc
    AND a.hellofresh_week=d.hellofresh_week
LEFT JOIN ve_asl_tph AS e
    ON a.dc_name=e.mapped_dc
    AND a.hellofresh_week=e.hellofresh_week
WHERE a.dc_name IN ('Barleben','Verden') AND a.hellofresh_week<='2024-W21'
ORDER BY 1,2
