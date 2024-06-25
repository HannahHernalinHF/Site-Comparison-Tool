-- transform hires and openings data (accessible in MySQL database)
with base as (
    select case when market_subdivision ='DACH MAG' and office ='Germany' then 'Barleben'
                when market_subdivision ='DACH MAG' and office ='Magdeburg, Germany' then 'Barleben'
                when market_subdivision ='DACH VER' and office ='Berlin, Germany' then 'Verden'
                when market_subdivision ='DACH VER' and office ='Germany' then 'Verden'
                when market_subdivision ='DACH VER' and office ='Verden, Germany' then 'Verden'
                when market_subdivision ='DACH WFS' and office ='Germany' then 'Verden'
                when market_subdivision ='DACH WFS' and office ='Magdeburg, Germany' then 'Barleben'
                when market_subdivision ='DACH WFS' and office ='Verden, Germany' then 'Verden'
            end dc_name,
           job_id,
           job_status,
           opening_id,
           opening_status,
           opening_open_date,
           opening_closed_date,
           start_date,
           case
                when opening_status = 'open' then
                concat(left(yearweek(opening_open_date + interval 2 day,3),4), '-W',right(yearweek(opening_open_date + interval 2 day,3),2))
                when opening_status != 'open' then
                concat(left(yearweek(opening_closed_date + interval 2 day,3),4), '-W',right(yearweek(opening_closed_date + interval 2 day,3),2))
           end hellofresh_week
    from pa_dm.external_greenhouse__hires_and_openings
    where job_status in ('closed', 'open')
    having dc_name = 'Barleben'
    )
     ,

    openings as (
        select
            dc_name
            , hellofresh_week
            , count(distinct opening_id) no_of_openings
            , count(distinct case when opening_status != 'open' then opening_id end) no_of_closings
        from base
        group by 1,2
    ),

    final as (
        select
            dc_name
            , hellofresh_week
            /*, sum(no_of_openings) no_of_openings
            , sum(no_of_closings) no_of_closed_openings */
            , sum(no_of_openings - no_of_closings) over (partition by o.dc_name order by hellofresh_week) vacancies
        from openings o
        group by 1,2
    )
select
    *
from final
where left(hellofresh_week,4) = '2024'
order by 2 desc, 1
