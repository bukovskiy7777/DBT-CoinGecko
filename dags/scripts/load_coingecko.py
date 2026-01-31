import dlt
from dlt.destinations import postgres
import requests
from datetime import datetime
import time

def fetch_coin_history(coin_id, date, api_key):
    """Запрашивает данные из API за конкретную дату"""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
    headers = {"accept": "application/json", "x-cg-demo-api-key": api_key}
    params = {'date': date, 'localization': 'false'}
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка {coin_id}: {response.status_code}")
        return None



def get_coingecko_data(target_date: str, api_key, coins):
    
    # Конвертируем YYYY-MM-DD в DD-MM-YYYY для CoinGecko
    date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    cg_date = date_obj.strftime('%d-%m-%Y')
    
    cleaned_data = []
    for coin in coins:
        raw_json = fetch_coin_history(coin, cg_date, api_key)
        
        if raw_json:
            # Формируем плоский словарь с твоими колонками
            # dlt поймет вложенность через двойное подчеркивание
            record = {
                "id": raw_json.get("id"),
                "symbol": raw_json.get("symbol"),
                "name": raw_json.get("name"),
                "image__thumb": raw_json.get("image", {}).get("thumb"),
                "image__small": raw_json.get("image", {}).get("small"),
                "market_data__current_price__usd": raw_json.get("market_data", {}).get("current_price", {}).get("usd"),
                "market_data__market_cap__usd": raw_json.get("market_data", {}).get("market_cap", {}).get("usd"),
                "market_data__total_volume__usd": raw_json.get("market_data", {}).get("total_volume", {}).get("usd"),
                "community_data__reddit_average_posts_48h": raw_json.get("community_data", {}).get("reddit_average_posts_48h"),
                "community_data__reddit_average_comments_48h": raw_json.get("community_data", {}).get("reddit_average_comments_48h"),
                "community_data__reddit_accounts_active_48h": raw_json.get("community_data", {}).get("reddit_accounts_active_48h"),
                "report_date": date_obj.date() # Используем объект даты напрямую
            }
            cleaned_data.append(record)
        
        # time.sleep(1.5) # Пауза для бесплатного лимита
        
    return cleaned_data


def run_load_to_postgres(target_date, db_credentials, api_key, coins, dataset_name, table_name):
    """Обертка для запуска в Airflow"""
    data = get_coingecko_data(target_date, api_key, coins)
    if data:
        # Инициализируем пайплайн прямо здесь
        pipeline = dlt.pipeline(
            pipeline_name='coingecko_ingestion',
            destination=postgres(credentials=db_credentials),
            dataset_name=dataset_name
        )
        return pipeline.run(data, table_name=table_name, write_disposition="append")
    return "No data to load"


