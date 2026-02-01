{{ config(materialized='view') }}

with base as (
    select 
        id as coin_id,
        report_date,

        market_data__current_price__usd as price,
        market_data__market_cap__usd as cap,
        market_data__total_volume__usd as volume,
        
        price_moving_avg_7d,
        cap_moving_avg_7d,
        volume_moving_avg_7d
    from {{ ref('fct_prices') }}
),
features as (
    select
        *,
        lead(price) over (partition by coin_id order by report_date) as lead_price,
        
        lag(price, 1) over (partition by coin_id order by report_date) as lag_price_1d,
        lag(price, 7) over (partition by coin_id order by report_date) as lag_price_7d,
        
        lag(cap, 1) over (partition by coin_id order by report_date) as lag_cap_1d,
        lag(cap, 7) over (partition by coin_id order by report_date) as lag_cap_7d,
        
        lag(volume, 1) over (partition by coin_id order by report_date) as lag_volume_1d,
        lag(volume, 7) over (partition by coin_id order by report_date) as lag_volume_7d,
        
        -- Волатильность (Стандартное отклонение за 7 дней)
        stddev(price) over 
            (partition by coin_id order by report_date rows between 6 preceding and current row) as volatility_7d
    from base
),
dataset as (
	select 
	coin_id, report_date, 
	
	price, lag_price_1d, lag_price_7d, price_moving_avg_7d, 
	
	cap, lag_cap_1d, lag_cap_7d, cap_moving_avg_7d, 
	
	volume, lag_volume_1d, lag_volume_7d, volume_moving_avg_7d, 
	
	-- отклонение цены (в %)
    (price - lag_price_1d) / nullif(lag_price_1d, 0) as price_diff_1d,
    (price - lag_price_7d) / nullif(lag_price_7d, 0) as price_diff_7d,
    (price - price_moving_avg_7d) / nullif(price_moving_avg_7d, 0) as price_diff_avg_7d,
    
    -- отклонение cap (в %)
    (cap - lag_cap_1d) / nullif(lag_cap_1d, 0) as cap_diff_1d,
    (cap - lag_cap_7d) / nullif(lag_cap_7d, 0) as cap_diff_7d,
    (cap - cap_moving_avg_7d) / nullif(cap_moving_avg_7d, 0) as cap_diff_avg_7d,
    
    -- отклонение volume (в %)
    (volume - lag_volume_1d) / nullif(lag_volume_1d, 0) as volume_diff_1d,
    (volume - lag_volume_7d) / nullif(lag_volume_7d, 0) as volume_diff_7d,
    (volume - volume_moving_avg_7d) / nullif(volume_moving_avg_7d, 0) as volume_diff_avg_7d,
    
    volatility_7d,
    
    -- Упрощенный расчет RSI: средний рост / среднее падение
    avg(case when (price - lag_price_1d) > 0 then (price - lag_price_1d) else 0 end) 
    		over (partition by coin_id order by report_date rows between 6 preceding and current row) as avg_gain,
    avg(case when (price - lag_price_1d) < 0 then abs(price - lag_price_1d) else 0 end) 
    		over (partition by coin_id order by report_date rows between 6 preceding and current row) as avg_loss,

	-- lead_price,
	-- Создаем таргет: 1 если цена завтра будет больше чем сегодня, если меньше, то 0
	case 
		when lead_price > price then 1 
		when lead_price is null then null
		when lead_price < price then 0
		else -1 
	end as target_direction
	from features
)
select 
	coin_id, report_date, 
	
	price, lag_price_1d, lag_price_7d, price_moving_avg_7d, 
	
	cap, lag_cap_1d, lag_cap_7d, cap_moving_avg_7d, 
	
	volume, lag_volume_1d, lag_volume_7d, volume_moving_avg_7d,
	
	price_diff_1d, price_diff_7d, price_diff_avg_7d,
	
	cap_diff_1d, cap_diff_7d, cap_diff_avg_7d,
	
	volume_diff_1d, volume_diff_7d, volume_diff_avg_7d,
	
	-- Финальная формула RSI
	case when avg_loss = 0 then 100 else 100 - (100 / (1 + (avg_gain / avg_loss))) end as rsi_7d,
	target_direction
from dataset
--where target_direction is not null and lag_price_7d is not null
order by coin_id, report_date