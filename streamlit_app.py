import asyncio
import streamlit as st
import pandas as pd
from io import BytesIO
import random
import time
import json
from datetime import datetime

# --- УСТАНОВКА ЗАВИСИМОСТЕЙ ---
# pip install streamlit pandas openpyxl ozonapi-async yandex-market-api wildberries-api aiohttp

from ozonapi import SellerAPI, SellerAPIConfig
from yandex_market_api import YandexMarketClient
from wb_api.async_api import AsyncAPI as WildberriesAPI
import aiohttp

# ============================================
# 1. КОНФИГУРАЦИЯ (secrets.toml)
# ============================================
# secrets.toml:
# OZON_CLIENT_ID = "ваш_id"
# OZON_API_KEY = "ваш_ключ"
# YANDEX_API_KEY = "ваш_токен"
# WILDBERRIES_TOKEN = "ваш_токен"

# ============================================
# 2. ФУНКЦИЯ ДЛЯ OZON (все характеристики)
# ============================================
async def get_ozon_full_info(article_list):
    """
    Получает ВСЕ характеристики товаров через Ozon API v4
    Метод: POST /v4/product/info/attributes
    """
    results = {}
    try:
        config = SellerAPIConfig(
            client_id=st.secrets["OZON_CLIENT_ID"],
            api_key=st.secrets["OZON_API_KEY"],
            max_requests_per_second=25,
            max_retries=5,
            retry_min_wait=2.0,
            retry_max_wait=10.0,
            request_timeout=30.0
        )
        async with SellerAPI(config=config) as api:
            # Получаем ВСЕ атрибуты товаров
            products = await api.product_info_attributes(
                product_ids=article_list,
                # Специфические параметры для Ozon API
                # Можно добавить language="RU"
            )
            
            for product in products:
                # Базовая информация
                result = {
                    "sku": getattr(product, "sku", None),
                    "offer_id": getattr(product, "offer_id", None),
                    "name": getattr(product, "name", None),
                    "category_id": getattr(product, "category_id", None),
                    "price": getattr(product, "price", None),
                    "old_price": getattr(product, "old_price", None),
                    "weight": getattr(product, "weight", None),
                    "width": getattr(product, "width", None),
                    "height": getattr(product, "height", None),
                    "depth": getattr(product, "depth", None),
                }
                
                # Все характеристики из attributes
                attributes = getattr(product, "attributes", [])
                for attr in attributes:
                    attr_name = attr.get("name", f"attr_{attr.get('id')}")
                    attr_value = attr.get("value")
                    # Если значение — список, преобразуем в строку
                    if isinstance(attr_value, list):
                        attr_value = ", ".join([str(v) for v in attr_value])
                    result[attr_name] = attr_value
                
                # Картинки
                images = getattr(product, "images", [])
                result["images_count"] = len(images)
                if images:
                    result["main_image"] = images[0].get("url", "")
                
                results[product.offer_id] = result
                
    except Exception as e:
        st.error(f"Ошибка Ozon API: {e}")
        st.exception(e)
    return results

# ============================================
# 3. ФУНКЦИЯ ДЛЯ ЯНДЕКС МАРКЕТ (все характеристики)
# ============================================
async def get_yandex_full_info(article_list):
    """
    Получает ВСЕ характеристики товаров через Yandex Market API
    Метод: POST /v2/campaigns/{campaignId}/offers
    """
    results = {}
    client = YandexMarketClient(api_key=st.secrets["YANDEX_API_KEY"])
    try:
        campaigns = await client.campaigns.list_campaigns()
        if not campaigns:
            st.warning("Нет активных кампаний в Яндекс Маркет")
            return results
        campaign_id = campaigns[0].id
        
        # Запрашиваем полную информацию о товарах
        offers = await client.offers.get_offers(
            campaign_id,
            offer_ids=article_list,
            # Включаем все доступные параметры
            include=["weight_dimensions", "attributes", "images", "prices"]
        )
        
        for offer in offers:
            result = {
                "sku": getattr(offer, "shop_sku", None),
                "name": getattr(offer, "name", None),
                "vendor": getattr(offer, "vendor", None),
                "price": getattr(offer, "price", None),
                "old_price": getattr(offer, "old_price", None),
                "category": getattr(offer, "category", None),
            }
            
            # Весогабариты
            weight_dim = getattr(offer, "weight_dimensions", None)
            if weight_dim:
                result["weight"] = getattr(weight_dim, "weight", None)
                result["width"] = getattr(weight_dim, "width", None)
                result["height"] = getattr(weight_dim, "height", None)
                result["depth"] = getattr(weight_dim, "depth", None)
            
            # Дополнительные атрибуты
            attributes = getattr(offer, "attributes", [])
            for attr in attributes:
                attr_name = attr.get("name", f"attr_{attr.get('id')}")
                attr_value = attr.get("value")
                if isinstance(attr_value, list):
                    attr_value = ", ".join([str(v) for v in attr_value])
                result[attr_name] = attr_value
            
            results[offer.shop_sku] = result
            
    except Exception as e:
        st.error(f"Ошибка Яндекс API: {e}")
        st.exception(e)
    finally:
        await client.close()
    return results

# ============================================
# 4. ФУНКЦИЯ ДЛЯ WILDBERRIES (все характеристики)
# ============================================
async def get_wb_full_info(article_list):
    """
    Получает ВСЕ характеристики товаров через Wildberries API
    Использует Content API и Market API
    """
    results = {}
    try:
        # Инициализация API
        api = await WildberriesAPI.build(
            token=st.secrets["WILDBERRIES_TOKEN"]
        )
        
        for article in article_list:
            try:
                # Получаем полную информацию о товаре
                product = await api.products.get_product(nm_id=article)
                
                result = {
                    "nm_id": getattr(product, "nm_id", None),
                    "vendor_code": getattr(product, "vendor_code", None),
                    "name": getattr(product, "name", None),
                    "brand": getattr(product, "brand", None),
                    "price": getattr(product, "sale_price", None),
                    "old_price": getattr(product, "old_price", None),
                    "discount": getattr(product, "discount", None),
                    "rating": getattr(product, "rating", None),
                    "reviews_count": getattr(product, "reviews_count", None),
                    "weight": getattr(product, "weight", None),  # в граммах
                    "width": getattr(product, "width", None),    # в см
                    "height": getattr(product, "height", None),  # в см
                    "depth": getattr(product, "depth", None),    # в см
                    "category": getattr(product, "category", None),
                    "subject_id": getattr(product, "subject_id", None),
                }
                
                # Дополнительные характеристики
                characteristics = getattr(product, "characteristics", [])
                for char in characteristics:
                    name = char.get("name", f"char_{char.get('id')}")
                    value = char.get("value")
                    if isinstance(value, list):
                        value = ", ".join([str(v) for v in value])
                    result[name] = value
                
                # Картинки
                images = getattr(product, "images", [])
                result["images_count"] = len(images)
                if images:
                    result["main_image"] = images[0].get("url", "")
                
                results[str(article)] = result
                await asyncio.sleep(random.uniform(0.5, 1.0))
                
            except Exception as e:
                results[str(article)] = {"error": str(e)}
        
        await api.close()
        
    except Exception as e:
        st.error(f"Ошибка Wildberries API: {e}")
        st.exception(e)
    return results

# ============================================
# 5. ОСНОВНОЙ ИНТЕРФЕЙС STREAMLIT
# ============================================
st.set_page_config(
    page_title="Полный парсер характеристик товаров",
    page_icon="📦",
    layout="wide"
)

st.title("📦 Полный сбор всех характеристик товаров")
st.caption("Сбор всех параметров, атрибутов и характеристик с трех маркетплейсов")

# --- СТИЛИ ---
st.markdown("""
<style>
.stProgress > div > div > div > div {
    background-color: #FF6B00;
}
</style>
""", unsafe_allow_html=True)

# --- БОКОВАЯ ПАНЕЛЬ ---
with st.sidebar:
    st.header("⚙️ Настройки")
    
    marketplace = st.selectbox(
        "Выберите маркетплейс",
        options=["Ozon", "Яндекс Маркет", "Wildberries", "Все маркетплейсы"]
    )
    
    batch_size = st.number_input(
        "Размер батча",
        min_value=5,
        max_value=100,
        value=20,
        help="Рекомендуется 20-50 для стабильности"
    )
    
    st.divider()
    
    # Дополнительные опции
    include_images = st.checkbox("Включить ссылки на изображения", value=True)
    include_attributes = st.checkbox("Включить все атрибуты", value=True)
    
    st.divider()
    st.caption("🔑 API-ключи в secrets.toml")
    st.caption("📌 Поддерживаются: Ozon, Яндекс Маркет, Wildberries")

# --- ЗАГРУЗКА ФАЙЛА ---
uploaded_file = st.file_uploader(
    "📁 Загрузите Excel-файл с артикулами",
    type=["xlsx", "xls", "csv"],
    help="Первый столбец — артикулы продавца"
)

if uploaded_file:
    try:
        # Определяем формат файла
        if uploaded_file.name.endswith('.csv'):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file, engine="openpyxl")
        
        st.success(f"✅ Загружено: {len(df)} артикулов")
        article_column = df.columns[0]
        articles = df[article_column].astype(str).tolist()
        
        # Предпросмотр
        with st.expander("📋 Предпросмотр загруженных данных"):
            st.dataframe(df.head(10))
            st.caption(f"Всего артикулов: {len(articles)}")
        
        # Кнопка запуска
        if st.button("🚀 Запустить сбор всех характеристик", type="primary"):
            # --- ИНИЦИАЛИЗАЦИЯ ---
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Контейнер для результатов
            all_results = {}
            
            # --- АСИНХРОННЫЙ СБОР ---
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            total_batches = (len(articles) + batch_size - 1) // batch_size
            
            for idx, i in enumerate(range(0, len(articles), batch_size)):
                batch = articles[i:i+batch_size]
                status_text.text(f"📊 Обработка батча {idx+1}/{total_batches}...")
                
                # Выбор маркетплейса
                try:
                    if marketplace in ["Ozon", "Все маркетплейсы"]:
                        data = loop.run_until_complete(get_ozon_full_info(batch))
                        for art, vals in data.items():
                            if "error" not in vals:
                                key = f"Ozon_{art}"
                                all_results[key] = vals
                            else:
                                all_results[f"Ozon_{art}"] = {"error": vals.get("error")}
                    
                    if marketplace in ["Яндекс Маркет", "Все маркетплейсы"]:
                        data = loop.run_until_complete(get_yandex_full_info(batch))
                        for art, vals in data.items():
                            if "error" not in vals:
                                key = f"Yandex_{art}"
                                all_results[key] = vals
                            else:
                                all_results[f"Yandex_{art}"] = {"error": vals.get("error")}
                    
                    if marketplace in ["Wildberries", "Все маркетплейсы"]:
                        data = loop.run_until_complete(get_wb_full_info(batch))
                        for art, vals in data.items():
                            if "error" not in vals:
                                key = f"WB_{art}"
                                all_results[key] = vals
                            else:
                                all_results[f"WB_{art}"] = {"error": vals.get("error")}
                    
                except Exception as e:
                    st.error(f"Ошибка в батче {idx+1}: {e}")
                
                # Обновление прогресса
                progress_bar.progress((idx + 1) / total_batches)
            
            status_text.text("✅ Сбор всех характеристик завершен!")
            
            # --- ФОРМИРОВАНИЕ РЕЗУЛЬТАТА ---
            if all_results:
                # Преобразуем в DataFrame
                rows = []
                for key, values in all_results.items():
                    row = {"Артикул": key}
                    if isinstance(values, dict):
                        row.update(values)
                    else:
                        row["error"] = str(values)
                    rows.append(row)
                
                result_df = pd.DataFrame(rows)
                
                # Убираем колонку с ошибками, если все успешно
                if "error" in result_df.columns:
                    error_count = result_df["error"].notna().sum()
                    if error_count > 0:
                        st.warning(f"⚠️ {error_count} записей содержат ошибки")
                    else:
                        result_df = result_df.drop(columns=["error"])
                
                # Показываем результат
                st.subheader(f"📊 Собрано характеристик: {len(result_df.columns)} колонок")
                st.dataframe(result_df, use_container_width=True)
                
                # --- ЭКСПОРТ В EXCEL ---
                st.subheader("📥 Экспорт данных")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Экспорт в Excel (все данные)
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine="openpyxl") as writer:
                        result_df.to_excel(writer, index=False, sheet_name="Характеристики")
                        
                        # Настройка ширины колонок
                        worksheet = writer.sheets["Характеристики"]
                        for column in worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                            adjusted_width = min(max_length + 2, 50)
                            worksheet.column_dimensions[column_letter].width = adjusted_width
                    
                    st.download_button(
                        label="⬇️ Скачать Excel (все характеристики)",
                        data=output.getvalue(),
                        file_name=f"all_characteristics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                with col2:
                    # Экспорт только базовых характеристик
                    base_cols = ["Артикул", "name", "price", "weight", "width", "height", "depth"]
                    existing_cols = [col for col in base_cols if col in result_df.columns]
                    if len(existing_cols) > 1:
                        base_df = result_df[existing_cols]
                        output_base = BytesIO()
                        with pd.ExcelWriter(output_base, engine="openpyxl") as writer:
                            base_df.to_excel(writer, index=False, sheet_name="Базовые характеристики")
                        
                        st.download_button(
                            label="⬇️ Скачать Excel (только базовые)",
                            data=output_base.getvalue(),
                            file_name=f"base_characteristics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                
                # --- СТАТИСТИКА ---
                with st.expander("📈 Статистика собранных данных"):
                    col3, col4, col5 = st.columns(3)
                    with col3:
                        st.metric("Всего товаров", len(result_df))
                    with col4:
                        st.metric("Всего характеристик", len(result_df.columns) - 1)
                    with col5:
                        # Считаем количество заполненных полей
                        filled = result_df.select_dtypes(include=['object']).apply(lambda x: x.notna().sum())
                        avg_filled = filled.mean() if len(filled) > 0 else 0
                        st.metric("Среднее заполнение", f"{avg_filled:.0f}%")
            
            else:
                st.error("❌ Не удалось собрать данные. Проверьте API-ключи.")
            
            loop.close()
            
    except Exception as e:
        st.error(f"❌ Ошибка: {e}")
        st.exception(e)

# ============================================
# 6. ПОМОЩЬ И ПОДСКАЗКИ
# ============================================
with st.expander("🔑 Как получить API-ключи"):
    st.markdown("""
    ### Ozon Seller API
    Перейдите в [Ozon Seller API](https://seller-api.ozon.ru/)
    - Создайте приложение и получите Client ID и API Key
    - Необходимые права: product_info
    
    ### Яндекс Маркет Partner API
    Перейдите в [Яндекс Маркет для продавцов](https://partner.market.yandex.ru/)
    - В разделе API получите OAuth-токен
    - Необходимые права: offers, prices
    
    ### Wildberries API
    Перейдите в [Wildberries Developers](https://dev.wildberries.ru/)
    - Получите токен продавца в личном кабинете
    - Необходимые права: content, products
    
    ### Настройка secrets.toml
    Создайте файл `.streamlit/secrets.toml`:
    ```toml
    OZON_CLIENT_ID = "ваш_client_id"
    OZON_API_KEY = "ваш_api_key"
    YANDEX_API_KEY = "ваш_токен"
    WILDBERRIES_TOKEN = "ваш_токен"
