{{
    config(
        materialized='incremental',
        unique_key=['id', 'report_date'],
        on_schema_change='append_new_columns'
    )
}}

with staging as (
    select * from {{ ref('stg_prices') }}
    
    --{% if is_incremental() %}
      -- Берем данные с запасом в 7 дней, чтобы оконная функция сработала корректно
      where report_date >= cast('{{ var("execution_date", run_started_at.strftime("%Y-%m-%d")) }}' as date) - interval '7 days'
    --{% endif %}
),

calculations as (
    select
        id,
        symbol,
        name,
        image__small,
        report_date,
        market_data__current_price__usd,
        market_data__market_cap__usd,
        market_data__total_volume__usd,
        -- Пример простой метрики
        avg(market_data__current_price__usd) 
            over (partition by id order by report_date rows between 6 preceding and current row) as price_moving_avg_7d,
        avg(market_data__market_cap__usd) 
            over (partition by id order by report_date rows between 6 preceding and current row) as cap_moving_avg_7d,
        avg(market_data__total_volume__usd) 
            over (partition by id order by report_date rows between 6 preceding and current row) as volume_moving_avg_7d
    from staging
)

select * from calculations
--{% if is_incremental() %}
    -- А вот здесь уже отсекаем лишнее, чтобы вставить в итоговую таблицу только свежий день
    where report_date = '{{ var("execution_date", run_started_at.strftime("%Y-%m-%d")) }}'
--{% endif %}