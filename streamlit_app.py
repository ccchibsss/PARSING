import requests
import time
import os
import pandas as pd
import sqlite3
import streamlit as st
from bs4 import BeautifulSoup
import logging
import random
import threading

def main():
    # Логирование
    logging.basicConfig(filename='wildberries_parser.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Конфигурация
    MAX_RETRIES = 10
    BACKOFF_BASE = 2
    DELAY_BETWEEN_REQUESTS = 1
    USE_PROXIES = False
    PROXY_LIST = []

    # Инициализация Streamlit
    st.set_page_config(page_title="Расширенный парсер Wildberries", layout="wide")
    if 'progress' not in st.session_state:
        st.session_state['progress'] = 0
    if 'stop' not in st.session_state:
        st.session_state['stop'] = False
    if 'pause' not in st.session_state:
        st.session_state['pause'] = False

    st.title("Расширенный парсер Wildberries")
    st.write("Настройте параметры и запускайте парсинг.")

    save_format = st.sidebar.radio("Формат сохранения", ["CSV", "SQLite"])
    filename_csv = st.sidebar.text_input("Имя файла CSV", value='wildberries_products_extended.csv')
    if save_format == "SQLite":
        db_name = st.sidebar.text_input("Имя базы данных SQLite", value="wildberries_extended.db")
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS products (
                link TEXT PRIMARY KEY,
                title TEXT,
                price TEXT,
                images TEXT,
                description TEXT,
                characteristics TEXT
            )
        ''')
        conn.commit()

    if USE_PROXIES:
        PROXY_LIST = [
            'http://proxy1:port',
            'http://proxy2:port',
        ]

    def get_random_proxy():
        if USE_PROXIES and PROXY_LIST:
            return {'http': random.choice(PROXY_LIST), 'https': random.choice(PROXY_LIST)}
        return None

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/105.0.0.0 Safari/537.36"
    }

    def safe_request(url, headers=None, retries=MAX_RETRIES):
        attempt = 0
        while attempt < retries:
            proxy = get_random_proxy()
            try:
                response = requests.get(url, headers=headers, proxies=proxy, timeout=15)
                if response.status_code == 200:
                    if "captcha" in response.text.lower() or "block" in response.text.lower():
                        logging.warning(f"Обнаружена капча или блокировка на {url}")
                        time.sleep(10 * attempt)
                    else:
                        return response
                elif response.status_code in [429, 403]:
                    wait_time = BACKOFF_BASE ** attempt + random.uniform(0, 1)
                    logging.warning(f"Блокировка (статус {response.status_code}). Повтор через {wait_time:.2f} сек.")
                    time.sleep(wait_time)
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                wait_time = BACKOFF_BASE ** attempt + random.uniform(0, 1)
                logging.warning(f"Ошибка запроса: {e}. Повтор через {wait_time:.2f} сек.")
                time.sleep(wait_time)
            attempt += 1
        logging.error(f"Не удалось получить {url} после {retries} попыток.")
        return None

    def save_product_csv(product_data, filename):
        df = pd.DataFrame([product_data], columns=["link", "title", "price", "images", "description", "characteristics"])
        if os.path.exists(filename):
            df.to_csv(filename, mode='a', index=False, header=False)
        else:
            df.to_csv(filename, mode='w', index=False)

    def save_product_sqlite(cursor, product_data):
        cursor.execute('''
            INSERT OR REPLACE INTO products (link, title, price, images, description, characteristics)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', product_data)
        if save_format == "SQLite":
            conn.commit()

    def parse_product(link):
        response = safe_request(link)
        if response:
            soup = BeautifulSoup(response.text, 'html.parser')
            title_tag = soup.find('h1', {'class': 'title'})
            title = title_tag.text.strip() if title_tag else ''
            price_tag = soup.find('ins', {'class': 'price-block__final-price'})
            price = price_tag.text.strip() if price_tag else ''
            images = [img.get('src') for img in soup.find_all('img', {'class': 'swiper-slide__img'})]
            description = ''
            desc_tag = soup.find('div', {'class': 'product-page__description'})
            if desc_tag:
                description = desc_tag.text.strip()
            characteristics = {}
            details = soup.find('div', {'class': 'product-details'})
            if details:
                for row in details.find_all('div', {'class': 'product-details__row'}):
                    key_tag = row.find('div', {'class': 'product-details__name'})
                    value_tag = row.find('div', {'class': 'product-details__value'})
                    if key_tag and value_tag:
                        key = key_tag.text.strip().lower()
                        value = value_tag.text.strip()
                        characteristics[key] = value
            return (
                link,
                title,
                price,
                '; '.join(images),
                description,
                '; '.join([f"{k}: {v}" for k, v in characteristics.items()])
            )
        return None

    def get_store_products(store_url):
        page = 1
        total_products = 0
        while True:
            if st.session_state['stop']:
                break
            if st.session_state['pause']:
                time.sleep(1)
                continue
            url = f"{store_url}?page={page}"
            response = safe_request(url)
            if response:
                soup = BeautifulSoup(response.text, 'html.parser')
                product_cards = soup.find_all('a', {'class': 'product-card__main'})
                if not product_cards:
                    break
                for a in product_cards:
                    link = 'https://wildberries.ru' + a['href']
                    product_data = parse_product(link)
                    if product_data:
                        if save_format == "CSV":
                            save_product_csv(product_data, filename_csv)
                        elif save_format == "SQLite":
                            save_product_sqlite(cursor, product_data)
                        st.session_state['progress'] += 1
                        progress = st.session_state['progress']
                        st.write(f'Обработано товаров: {progress}')
                        st.progress(progress / 1000)  # Настройте по необходимости
                        time.sleep(DELAY_BETWEEN_REQUESTS)
                next_btn = soup.find('a', {'class': 'pagination__next'})
                if not next_btn or 'disabled' in next_btn.get('class', []):
                    break
                else:
                    page += 1
            else:
                break
        return total_products

    def collect_full_store():
        store_url = st.text_input("Введите ссылку магазина для сбора всех товаров", value='https://wildberries.ru/категории/автозапчасти')
        if st.button("Собрать все товары магазина"):
            st.session_state['stop'] = False
            total_collected = get_store_products(store_url)
            st.write(f"Обработано товаров: {st.session_state['progress']}")

    def run():
        # Можно запускать разные функции
        pass

    # Запуск по кнопкам
    if st.button("Запустить парсинг"):
        st.session_state['stop'] = False
        threading.Thread(target=run).start()

    if st.button("Остановить парсинг"):
        st.session_state['stop'] = True

    if st.button("Пауза/Продолжить"):
        st.session_state['pause'] = not st.session_state['pause']

    # Вызов функции для сбора всего магазина
    collect_full_store()

# Запускаем main
if __name__ == "__main__":
    main()
