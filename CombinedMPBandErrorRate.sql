--- This query is to extract the combined MPB and Error Rate for the Production Error Rate and Worked MPB charts ---
  
SELECT dc_name,
       hellofresh_week,
       metric,
       CASE WHEN metric IN ('Worked | Production | Hours','Worked | Support | Hours') THEN SUM((numerator_value*60)/denominator_value)
           WHEN metric IN ('Production | P&P | Assembly | Error Rate [W-1]','Production | P&P | Kitting | Error Rate [W-1]') THEN SUM(numerator_value/denominator_value)
           END AS mpb_value
FROM materialized_views.isa__wbr_dashboard_layer_view
WHERE hellofresh_year >= 2024
    AND metric IN ('Worked | Production | Hours','Worked | Support | Hours', ---MPB
                   'Production | P&P | Assembly | Error Rate [W-1]','Production | P&P | Kitting | Error Rate [W-1]' --- Errors
                  )
    AND dc_name IN ('Barleben','Verden')--AND dc_name<>'Windmill'
GROUP BY 1,2,3
ORDER BY 1,3,2
