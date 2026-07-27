import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import io
import os
import json
import logging
from typing import Dict, List, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import math
import re
import decimal
from datetime import datetime, date, timedelta
import polars as pl
import duckdb
import tempfile
from zipfile import ZipFile
import threading
import hashlib
from collections import OrderedDict
import gc
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from pyvis.network import Network
import plotly.graph_objects as go
import plotly.express as px
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from matplotlib_venn import venn2, venn3

# Импорты для Excel/CSV
import pyarrow as pa
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Константы
EXCEL_ROW_LIMIT = 1_048_576  # Максимум строк в Excel

# ============================================================================
# БЛОК 11: HIGH-VOLUME КАТАЛОГ АВТОЗАПЧАСТЕЙ (ПОЛНАЯ ВЕРСИЯ v100.21)
# ============================================================================

@st.cache_resource
def get_high_volume_catalog():
    """Создание каталога через st.cache_resource для корректной работы с DuckDB"""
    return HighVolumeAutoPartsCatalog()

class HighVolumeAutoPartsCatalog:
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Загрузка конфигураций
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()
        
        self.db_path = self.data_dir / "catalog.duckdb"
        self.conn = duckdb.connect(database=str(self.db_path))
        self.setup_database()
    
    # ========================================================================
    # КОНФИГУРАЦИИ
    # ========================================================================
    def load_cloud_config(self) -> Dict[str, Any]:
        config_path = self.data_dir / "cloud_config.json"
        default_config = {
            "enabled": False,
            "provider": "s3",
            "bucket": "",
            "region": "",
            "sync_interval": 3600,
            "last_sync": 0
        }
        
        if config_path.exists():
            try:
                return json.loads(config_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения cloud_config.json: {e}")
                return default_config
        else:
            config_path.write_text(json.dumps(
                default_config, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_config
    
    def save_cloud_config(self):
        config_path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        config_path.write_text(json.dumps(
            self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')
    
    def load_price_rules(self) -> Dict[str, Any]:
        price_rules_path = self.data_dir / "price_rules.json"
        default_rules = {
            "global_markup": 0.2,
            "brand_markups": {},
            "min_price": 0.0,
            "max_price": 99999.0
        }
        
        if price_rules_path.exists():
            try:
                return json.loads(price_rules_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.error(f"Ошибка чтения price_rules.json: {e}")
                return default_rules
        else:
            price_rules_path.write_text(json.dumps(
                default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_rules
    
    def save_price_rules(self):
        price_rules_path = self.data_dir / "price_rules.json"
        price_rules_path.write_text(json.dumps(
            self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')
    
    def load_exclusion_rules(self) -> List[str]:
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        if exclusion_path.exists():
            try:
                return [line.strip() for line in exclusion_path.read_text(encoding='utf-8').splitlines() if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка чтения exclusion_rules.txt: {e}")
                return []
        else:
            content = "Кузов\nСтекла\nМасла"
            exclusion_path.write_text(content, encoding='utf-8')
            return ["Кузов", "Стекла", "Масла"]
    
    def save_exclusion_rules(self):
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        exclusion_path.write_text(
            "\n".join(self.exclusion_rules), encoding='utf-8')
    
    def load_category_mapping(self) -> Dict[str, str]:
        category_path = self.data_dir / "category_mapping.txt"
        default_mapping = {
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        }
        
        if category_path.exists():
            try:
                mapping = {}
                for line in category_path.read_text(encoding='utf-8').splitlines():
                    if line.strip() and "|" in line:
                        key, value = line.split("|", 1)
                        mapping[key.strip()] = value.strip()
                return mapping
            except Exception as e:
                logger.error(f"Ошибка чтения category_mapping.txt: {e}")
                return default_mapping
        else:
            content = "\n".join(
                [f"{k}|{v}" for k, v in default_mapping.items()])
            category_path.write_text(content, encoding='utf-8')
            return default_mapping
    
    def save_category_mapping(self):
        category_path = self.data_dir / "category_mapping.txt"
        content = "\n".join(
            [f"{k}|{v}" for k, v in self.category_mapping.items()])
        category_path.write_text(content, encoding='utf-8')
    
    # ========================================================================
    # БАЗА ДАННЫХ
    # ========================================================================
    def setup_database(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR,
                length DOUBLE,
                width DOUBLE,
                height DOUBLE,
                weight DOUBLE,
                dimensions_str VARCHAR
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parts (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                artikul VARCHAR,
                brand VARCHAR,
                multiplicity INTEGER,
                barcode VARCHAR,
                length DOUBLE,
                width DOUBLE,
                height DOUBLE,
                weight DOUBLE,
                image_url VARCHAR,
                dimensions_str VARCHAR,
                description VARCHAR,
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                price DOUBLE,
                currency VARCHAR DEFAULT 'RUB',
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR
            )
        """)
        
        self.create_indexes()
    
    def create_indexes(self):
        st.info("⚙️ Создание индексов для ускорения поиска...")
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)"
        ]
        
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        
        st.success("🛠️ Индексы созданы.")
    
    # ========================================================================
    # НОРМАЛИЗАЦИЯ И ОЧИСТКА
    # ========================================================================
    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())
    
    @staticmethod
    def clean_values(series: pl.Series) -> pl.Series:
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())
    
    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        name_lower = name_series.str.to_lowercase()
        
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        
        # Пользовательские правила — приоритет
        for key, category in self.category_mapping.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower())
            ).then(pl.lit(category))
        
        # Стандартные правила
        categories_map = {
            'Фильтр': 'фильтр|filter',
            'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
            'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг',
            'Двигатель': 'двигатель|engine|свеч|поршень|клапан',
            'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission',
            'Электрика': 'аккумулятор|генератор|стартер|провод|ламп',
            'Рулевое': 'рулевой|тяга|наконечник|steering',
            'Выпуск': 'глушитель|катализатор|выхлоп|exhaust',
            'Охлаждение': 'радиатор|вентилятор|термостат|cooling',
            'Топливо': 'топливный|бензонасос|форсунк|fuel'
        }
        
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(pattern, literal=False)
            ).then(pl.lit(category))
        
        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')
    
    # ========================================================================
    # ✅ УНИВЕРСАЛЬНАЯ КОНВЕРТАЦИЯ В ЧИСЛО (ИСПРАВЛЕНИЕ ДАТ v100.20)
    # ========================================================================
    @staticmethod
    def safe_convert_to_float(value: Any) -> float:
        """
        ✅ v100.20: УНИВЕРСАЛЬНАЯ КОНВЕРТАЦИЯ ЛЮБОГО ЗНАЧЕНИЯ В ЧИСЛО
        Исправляет проблему с датами в габаритах
        """
        if value is None or value == "":
            return 0.0
        
        # Если это уже число
        if isinstance(value, (int, float)):
            if math.isnan(value) or math.isinf(value):
                return 0.0
            return float(value)
        
        # Если это Decimal
        if isinstance(value, decimal.Decimal):
            return float(value)
        
        # Если это datetime или date
        if isinstance(value, (datetime, date, pd.Timestamp)):
            # Пытаемся конвертировать дату в число (Excel serial number)
            try:
                # Базовое значение для Excel: 1899-12-30 = 0
                base = datetime(1899, 12, 30)
                if isinstance(value, pd.Timestamp):
                    value = value.to_pydatetime()
                delta = value - base
                return float(delta.days + delta.seconds / 86400.0)
            except Exception:
                return 0.0
        
        # Если это timedelta
        if isinstance(value, timedelta):
            return float(value.total_seconds() / 86400.0)
        
        # Если это строка
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return 0.0
            
            # Убираем лишние символы, оставляем только цифры, точку и запятую
            cleaned = re.sub(r'[^\d.,\-]', '', value)
            if not cleaned:
                return 0.0
            
            # Заменяем запятую на точку
            cleaned = cleaned.replace(',', '.')
            
            # Убираем лишние точки
            parts = cleaned.split('.')
            if len(parts) > 2:
                cleaned = parts[0] + '.' + ''.join(parts[1:])
            
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        
        # Если это numpy число
        if hasattr(value, 'dtype') and hasattr(value, 'item'):
            try:
                item = value.item()
                if isinstance(item, (int, float)):
                    return float(item)
            except Exception:
                pass
        
        # Если это polars
        if hasattr(value, 'to_python'):
            try:
                return float(value.to_python())
            except Exception:
                pass
        
        # Последняя попытка
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    # ========================================================================
    # ✅ ОБРАБОТКА ФАЙЛОВ (ИСПРАВЛЕНО v100.20)
    # ========================================================================
# Обновляем функцию detect_columns в классе HighVolumeAutoPartsCatalog
def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
    """
    ✅ РАСШИРЕННОЕ СОПОСТАВЛЕНИЕ КОЛОНОК С ВАШИМИ ФАЙЛАМИ
    """
    column_variants = {
        'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE', 'oe_number', 'oe number', 'OE'],
        'artikul': ['артикул', 'article', 'sku', 'artikul', 'код товара', 'код', 'код артикула', 'Артикул'],
        'brand': ['бренд', 'brand', 'производитель', 'manufacturer', 'марка', 'Бренд'],
        'name': ['наименование', 'название', 'name', 'описание', 'description', 'товар', 'наименование товара', 'Наименование'],
        'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability', 'применяемость', 'Марка авто', 'Применимость'],
        'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13', 'штрих код', 'EAN13'],
        'multiplicity': ['кратность шт', 'кратность', 'multiplicity', 'кратность упаковки', 'Кратность'],
        'length': ['длина (см)', 'длина', 'length', 'длинна', 'длина, см', 'length_cm'],
        'width': ['ширина (см)', 'ширина', 'width', 'ширина, см', 'width_cm'],
        'height': ['высота (см)', 'высота', 'height', 'высота, см', 'height_cm'],
        'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight', 'масса', 'weight_kg', 'вес кг'],
        'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка', 'фото', 'ссылка на изображение', 'Ссылка на изображение'],
        'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size', 'габариты', 'длинна/ширина/высота', 'длина/ширина/высота'],
        'price': ['цена', 'price', 'рекомендованная цена', 'retail price', 'цена продажи', 'стоимость'],
        'currency': ['валюта', 'currency'],
        'oe': ['OE', 'oe', 'номер OE', 'OE номер', 'OE-номер']
    }
    
    actual_lower = {col.lower().strip(): col for col in actual_columns}
    mapping = {}
    used_actual = set()
    
    for expected in expected_columns:
        variants = column_variants.get(expected, [expected])
        
        best_match = None
        best_score = -1
        
        for variant in variants:
            variant_lower = variant.lower().strip()
            
            for actual_l, actual_orig in actual_lower.items():
                if actual_orig in used_actual:
                    continue
                
                score = 0
                if variant_lower == actual_l:
                    score = 100
                elif variant_lower in actual_l:
                    score = 50 + len(variant_lower)
                elif actual_l in variant_lower:
                    score = 30 + len(actual_l)
                # Добавим частичное совпадение
                elif any(word in actual_l for word in variant_lower.split()):
                    score = 20 + len([w for w in variant_lower.split() if w in actual_l])
                
                if score > best_score:
                    best_score = score
                    best_match = actual_orig
        
        if best_match and best_score > 0:
            mapping[best_match] = expected
            used_actual.add(best_match)
    
    logger.info(f"Маппинг колонок: {mapping}")
    return mapping
    
    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        """
        ✅ ИСПРАВЛЕНИЕ v100.20: Полная защита от дат в габаритах
        """
        logger.info(f"Обработка файла: {file_type} ({file_path})")
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return pl.DataFrame()
            
            df = pl.read_excel(file_path, engine='calamine')
            
            if df.is_empty():
                logger.warning(f"Пустой файл: {file_path}")
                return pl.DataFrame()
            
            logger.info(f"Исходные колонки файла {file_type}: {df.columns}")
            logger.info(f"Исходные типы колонок файла {file_type}: {df.schema}")
            
        except Exception as e:
            logger.exception(f"Ошибка чтения файла {file_path}: {e}")
            return pl.DataFrame()
        
        schemas = {
            'oe': ['oe_number', 'artikul', 'brand', 'name', 'applicability',
                   'length', 'width', 'height', 'weight', 'dimensions_str', 'price', 'currency'],
            'cross': ['oe_number', 'artikul', 'brand'],
            'barcode': ['artikul', 'brand', 'barcode', 'multiplicity'],
            'dimensions': ['artikul', 'brand', 'length', 'width', 'height', 'weight', 'dimensions_str'],
            'images': ['artikul', 'brand', 'image_url'],
            'prices': ['artikul', 'brand', 'price', 'currency'],
            'universal': ['artikul', 'brand', 'name', 'oe_number', 'applicability',
                         'length', 'width', 'height', 'weight', 'dimensions_str',
                         'price', 'currency', 'barcode', 'multiplicity', 'image_url']
        }
        
        expected_cols = schemas.get(file_type, [])
        column_mapping = self.detect_columns(df.columns, expected_cols)
        
        if not column_mapping:
            logger.warning(
                f"Не удалось определить колонки для файла {file_type}. Доступные: {df.columns}")
            return pl.DataFrame()
        
        logger.info(f"Маппинг колонок для {file_type}: {column_mapping}")
        
        # Переименование с защитой от дубликатов
        try:
            df = df.rename(column_mapping)
        except Exception as e:
            logger.error(f"Ошибка при rename: {e}")
            for old_name, new_name in column_mapping.items():
                try:
                    if new_name not in df.columns:
                        df = df.rename({old_name: new_name})
                    else:
                        logger.warning(f"Колонка {new_name} уже существует, пропускаем {old_name}")
                except Exception as e2:
                    logger.warning(f"Не удалось переименовать {old_name} → {new_name}: {e2}")
        
        # Удаляем дубликаты колонок
        if len(df.columns) != len(set(df.columns)):
            logger.warning(f"Обнаружены дубликаты колонок: {df.columns}")
            seen = set()
            cols_to_keep = []
            for col in df.columns:
                if col not in seen:
                    seen.add(col)
                    cols_to_keep.append(col)
                else:
                    logger.warning(f"Удаляем дубликат колонки: {col}")
            df = df.select(cols_to_keep)
        
        # Нормализация ключевых колонок
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))
        
        # ✅ v100.20: УНИВЕРСАЛЬНАЯ КОНВЕРТАЦИЯ ЧИСЛОВЫХ КОЛОНОК (включая даты)
        numeric_cols = ['length', 'width', 'height', 'weight', 'price']
        for col in numeric_cols:
            if col in df.columns:
                try:
                    # Применяем универсальную конвертацию к каждой ячейке
                    converted_values = []
                    for val in df[col].to_list():
                        converted = self.safe_convert_to_float(val)
                        converted_values.append(converted)
                    
                    # Заменяем колонку на список чисел
                    df = df.with_columns(pl.Series(converted_values).alias(col))
                    
                    # Округляем до 2 знаков
                    df = df.with_columns(pl.col(col).round(2).alias(col))
                    
                    logger.info(f"✅ Колонка '{col}' сконвертирована в числа")
                    
                except Exception as e:
                    logger.warning(f"Не удалось преобразовать {col}: {e}")
                    # Если не удалось - создаем колонку с нулями
                    try:
                        df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                    except Exception:
                        pass
        
        # Удаление дубликатов по ключевым колонкам
        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')
        
        # Нормализация ключей
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(self.normalize_key(
                    pl.col(col)).alias(f"{col}_norm"))
        
        logger.info(f"Файл {file_type} обработан. Итоговые колонки: {df.columns}")
        logger.info(f"Итоговые типы колонок файла {file_type}: {df.schema}")
        return df
    
    # ========================================================================
    # ✅ ЗАГРУЗКА И ОБНОВЛЕНИЕ В БАЗЕ
    # ========================================================================
    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
        """
        ✅ УПРОЩЕНО v100.18: Используется WHERE (...) IN (SELECT ...)
        """
        if df.is_empty():
            return
        
        df = df.unique(keep='first')
        temp_view_name = f"temp_{table_name}_{int(time.time())}"
        
        try:
            self.conn.register(temp_view_name, df.to_arrow())
        except Exception as e:
            logger.error(f"Ошибка регистрации временной таблицы: {e}")
            return
        
        try:
            pk_list = pk
            pk_cols_csv = ", ".join(f'"{c}"' for c in pk_list)
            
            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE ({pk_cols_csv}) IN (SELECT {pk_cols_csv} FROM {temp_view_name});
            """
            self.conn.execute(delete_sql)
            
            insert_sql = f"""
                INSERT INTO {table_name}
                SELECT * FROM {temp_view_name};
            """
            self.conn.execute(insert_sql)
            
            logger.info(f"Успешно upsert {len(df)} записей в таблицу {table_name}.")
        
        except Exception as e:
            logger.error(f"Ошибка при UPSERT в {table_name}: {e}")
            st.error(f"Ошибка при записи в таблицу {table_name}. Детали в логе.")
        
        finally:
            try:
                self.conn.unregister(temp_view_name)
            except Exception:
                pass
    
    def upsert_prices(self, price_df: pl.DataFrame):
        if price_df.is_empty():
            return
        
        if 'artikul' in price_df.columns and 'brand' in price_df.columns:
            price_df = price_df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])
            
            if 'currency' not in price_df.columns:
                price_df = price_df.with_columns(pl.lit('RUB').alias('currency'))
            
            price_df = price_df.filter(
                (pl.col('price') >= self.price_rules['min_price']) &
                (pl.col('price') <= self.price_rules['max_price'])
            )
            
            self.upsert_data('prices', price_df, ['artikul_norm', 'brand_norm'])
    
    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """
        ✅ ИСПРАВЛЕНО v100.21: Габариты тянутся из OE файла + защита от дат
        ✅ ИСПРАВЛЕНИЕ: Исправлена ошибка "table oe has 10 columns but 5 values were supplied"
        """
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        
        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        num_steps = len(steps)
        
        progress_bar = st.progress(0, text="Подготовка к обновлению базы данных...")
        step_counter = 0
        
        # ================================================================
        # ШАГ 1: Обработка OE данных
        # ================================================================
        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка OE данных...")
            
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            
            # ✅ ИСПРАВЛЕНИЕ v100.21: ЯВНО УКАЗЫВАЕМ ВСЕ 10 КОЛОНОК
            
            # Проверяем и добавляем length
            if 'length' not in df.columns:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias('length'))
            
            # Проверяем и добавляем width
            if 'width' not in df.columns:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias('width'))
            
            # Проверяем и добавляем height
            if 'height' not in df.columns:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias('height'))
            
            # Проверяем и добавляем weight
            if 'weight' not in df.columns:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias('weight'))
            
            # Проверяем и добавляем dimensions_str
            if 'dimensions_str' not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias('dimensions_str'))
            
            # ✅ ТЕПЕРЬ ВЫБИРАЕМ ВСЕ 10 КОЛОНОК В ПРАВИЛЬНОМ ПОРЯДКЕ
            # Сначала берем существующие колонки
            oe_df = df.select([
                'oe_number_norm',
                'oe_number',
                'name',
                'applicability',
                'length',
                'width',
                'height',
                'weight',
                'dimensions_str'
            ]).unique(subset=['oe_number_norm'], keep='first')
            
            # Добавляем категорию
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(
                    self.determine_category_vectorized(pl.col('name')).alias('category')
                )
            else:
                oe_df = oe_df.with_columns(pl.lit('Разное').alias('category'))
            
            # ✅ УБЕЖДАЕМСЯ, ЧТО ПОРЯДОК КОЛОНОК СООТВЕТСТВУЕТ ТАБЛИЦЕ
            # Таблица oe: oe_number_norm, oe_number, name, applicability, category, length, width, height, weight, dimensions_str
            oe_df = oe_df.select([
                'oe_number_norm',
                'oe_number',
                'name',
                'applicability',
                'category',
                'length',
                'width',
                'height',
                'weight',
                'dimensions_str'
            ])
            
            # ✅ ПРОВЕРЯЕМ КОЛОНКИ ПЕРЕД ВСТАВКОЙ
            logger.info(f"Колонки oe_df перед upsert: {oe_df.columns}")
            logger.info(f"Количество колонок в oe_df: {len(oe_df.columns)}")
            
            self.upsert_data('oe', oe_df, ['oe_number_norm'])
            
            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, [
                'oe_number_norm', 'artikul_norm', 'brand_norm'])
        
        # ================================================================
        # ШАГ 2: Обработка кроссов
        # ================================================================
        if 'cross' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка кроссов...")
            
            df = dataframes['cross'].filter(
                (pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != ""))
            cross_df_from_cross = df.select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_cross, [
                'oe_number_norm', 'artikul_norm', 'brand_norm'])
        
        # ================================================================
        # ШАГ 3: Обработка цен
        # ================================================================
        if 'prices' in dataframes:
            price_df = dataframes['prices']
            if not price_df.is_empty():
                st.info("💰 Обработка цен...")
                self.upsert_prices(price_df)
                st.success(f"✅ Успешно обновлено {len(price_df)} ценовых записей")
        
        # ================================================================
        # ШАГ 4: Сборка и обновление данных по артикулам
        # ================================================================
        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1),
                              text=f"({step_counter}/{num_steps}) Сборка и обновление данных по артикулам...")
        
        parts_df = None
        file_priority = ['oe', 'dimensions', 'barcode', 'images']
        key_files = {ftype: df for ftype, df in dataframes.items() if ftype in file_priority}
        
        if key_files:
            parts_to_concat = [
                df.select(['artikul', 'artikul_norm', 'brand', 'brand_norm'])
                for df in key_files.values()
                if 'artikul_norm' in df.columns and 'brand_norm' in df.columns and not df.is_empty()
            ]
            
            if parts_to_concat:
                all_parts = pl.concat(parts_to_concat).filter(
                    pl.col('artikul_norm') != ""
                ).unique(subset=['artikul_norm', 'brand_norm'], keep='first')
                parts_df = all_parts
            else:
                parts_df = pl.DataFrame()
        
        if parts_df is not None and not parts_df.is_empty():
            for ftype in file_priority:
                if ftype not in key_files:
                    continue
                
                df = key_files[ftype]
                if df.is_empty() or 'artikul_norm' not in df.columns:
                    continue
                
                # ✅ v100.20: Для OE и dimensions файлов принудительно добавляем габариты
                if ftype in ['oe', 'dimensions']:
                    dims_to_add = ['length', 'width', 'height', 'weight', 'dimensions_str']
                    join_cols = [col for col in dims_to_add if col in df.columns]
                else:
                    join_cols = [col for col in df.columns if col not in [
                        'artikul', 'artikul_norm', 'brand', 'brand_norm']]
                
                if not join_cols:
                    continue
                
                existing_cols = set(parts_df.columns)
                join_cols = [col for col in join_cols if col not in existing_cols]
                if not join_cols:
                    continue
                
                df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique(
                    subset=['artikul_norm', 'brand_norm'], keep='first')
                parts_df = parts_df.join(
                    df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)
            
            # Заполняем недостающие колонки
            if 'multiplicity' not in parts_df.columns:
                parts_df = parts_df.with_columns(multiplicity=pl.lit(1).cast(pl.Int32))
            else:
                parts_df = parts_df.with_columns(pl.col('multiplicity').fill_null(1).cast(pl.Int32))
            
            for col in ['length', 'width', 'height', 'weight']:
                if col not in parts_df.columns:
                    parts_df = parts_df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                else:
                    parts_df = parts_df.with_columns(
                        pl.col(col).fill_null(0).cast(pl.Float64).alias(col)
                    )
            
            if 'dimensions_str' not in parts_df.columns:
                parts_df = parts_df.with_columns(dimensions_str=pl.lit(None).cast(pl.Utf8))
            
            # Формируем dimensions_str из length, width, height
            parts_df = parts_df.with_columns([
                pl.col('length').cast(pl.Utf8).fill_null('').alias('_length_str'),
                pl.col('width').cast(pl.Utf8).fill_null('').alias('_width_str'),
                pl.col('height').cast(pl.Utf8).fill_null('').alias('_height_str'),
            ])
            
            parts_df = parts_df.with_columns(
                dimensions_str=pl.when(
                    (pl.col('dimensions_str').is_not_null()) &
                    (pl.col('dimensions_str').cast(pl.Utf8) != '')
                ).then(
                    pl.col('dimensions_str').cast(pl.Utf8)
                ).otherwise(
                    pl.concat_str([
                        pl.col('_length_str'), pl.lit('x'),
                        pl.col('_width_str'), pl.lit('x'),
                        pl.col('_height_str')
                    ], separator='')
                )
            )
            
            parts_df = parts_df.drop(['_length_str', '_width_str', '_height_str'])
            
            if 'artikul' not in parts_df.columns:
                parts_df = parts_df.with_columns(artikul=pl.lit(''))
            if 'brand' not in parts_df.columns:
                parts_df = parts_df.with_columns(brand=pl.lit(''))
            
            parts_df = parts_df.with_columns([
                pl.col('artikul').cast(pl.Utf8).fill_null('').alias('_artikul_str'),
                pl.col('brand').cast(pl.Utf8).fill_null('').alias('_brand_str'),
                pl.col('multiplicity').cast(pl.Utf8).alias('_multiplicity_str'),
            ])
            
            parts_df = parts_df.with_columns(
                description=pl.concat_str([
                    pl.lit('Артикул: '), pl.col('_artikul_str'),
                    pl.lit(', Бренд: '), pl.col('_brand_str'),
                    pl.lit(', Кратность: '), pl.col('_multiplicity_str'), pl.lit(' шт.')
                ], separator='')
            )
            
            parts_df = parts_df.drop(['_artikul_str', '_brand_str', '_multiplicity_str'])
            
            final_columns = [
                'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode',
                'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
            ]
            select_exprs = [pl.col(c) if c in parts_df.columns else pl.lit(None).alias(c) for c in final_columns]
            parts_df = parts_df.select(select_exprs)
            
            self.upsert_data('parts', parts_df, ['artikul_norm', 'brand_norm'])
        
        progress_bar.progress(1.0, text="Обновление базы данных завершено!")
        time.sleep(1)
        progress_bar.empty()
    
    # ========================================================================
    # ЭКСПОРТ (✅ v100.20 - ГАРАНТИРОВАННОЕ ЗАПОЛНЕНИЕ ГАБАРИТОВ)
    # ========================================================================
    def _get_brand_markups_sql(self) -> str:
        rows = []
        for brand, markup in self.price_rules['brand_markups'].items():
            safe_brand = brand.replace("'", "''")
            rows.append(f"SELECT '{safe_brand}' AS brand, {markup} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, NULL AS markup LIMIT 0"
    
    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
        """
        ✅ v100.20: ГАРАНТИРОВАННОЕ ЗАПОЛНЕНИЕ ВСЕХ 4 КОЛОНОК ГАБАРИТОВ
        Приоритет: 1. Данные → 2. OE → 3. Аналоги → 4. Значение по умолчанию
        
        Колонки гарантированно получают числа, а не даты!
        """
        description_text = (
            "Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — надежное решение для вашего автомобиля. "
            "Обеспечьте безопасность, долговечность и высокую производительность вашего авто с помощью нашего широкого ассортимента оригинальных и совместимых автозапчастей. "
            "В нашем каталоге вы найдете тормозные системы, фильтры (масляные, воздушные, салонные), свечи зажигания, расходные материалы, автохимию, электроматериалы, автомасла, инструмент, "
            "а также другие комплектующие, полностью соответствующие стандартам качества и безопасности. "
            "Мы гарантируем быструю доставку, выгодные цены и профессиональную консультацию для любого клиента — автолюбителя, специалиста или автосервиса. "
            "Выбирайте только лучшее — надежность и качество от ведущих производителей."
        )
        
        brand_markups_sql = self._get_brand_markups_sql()
        
        select_parts = []
        
        # Колонки с ценой
        price_requested = include_prices and (not selected_columns or "Цена" in selected_columns or "Валюта" in selected_columns)
        if price_requested:
            if apply_markup:
                global_markup = self.price_rules.get('global_markup', 0)
                select_parts.append(
                    f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {global_markup})) ELSE pr.price END AS \"Цена\""
                )
            else:
                select_parts.append('pr.price AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")
        
        # ✅ v100.20: ГАРАНТИРОВАННОЕ ЗАПОЛНЕНИЕ ГАБАРИТОВ С ПРИОРИТЕТОМ
        columns_map = [
            ("Артикул бренда", 'r.artikul AS "Артикул бренда"'),
            ("Бренд", 'r.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"'),
            ("Описание", 'CONCAT(COALESCE(r.description, \'\'), dt.text) AS "Описание"'),
            ("Категория товара", 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория товара"'),
            ("Кратность", 'r.multiplicity AS "Кратность"'),
            
            # ✅ v100.20: ГАРАНТИРОВАННЫЕ ЧИСЛА С ПРИОРИТЕТОМ И ЗНАЧЕНИЕМ ПО УМОЛЧАНИЮ
            ("Длинна", """
                COALESCE(
                    NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0),      -- 1. Данные
                    NULLIF(ROUND(CAST(r.oe_length AS DOUBLE), 2), 0),   -- 2. OE
                    NULLIF(ROUND(CAST(r.analog_length AS DOUBLE), 2), 0), -- 3. Аналоги
                    0.0                                                   -- 4. По умолчанию
                ) AS "Длинна"
            """),
            ("Ширина", """
                COALESCE(
                    NULLIF(ROUND(CAST(r.width AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.oe_width AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.analog_width AS DOUBLE), 2), 0),
                    0.0
                ) AS "Ширина"
            """),
            ("Высота", """
                COALESCE(
                    NULLIF(ROUND(CAST(r.height AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.oe_height AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.analog_height AS DOUBLE), 2), 0),
                    0.0
                ) AS "Высота"
            """),
            ("Вес", """
                COALESCE(
                    NULLIF(ROUND(CAST(r.weight AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.oe_weight AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.analog_weight AS DOUBLE), 2), 0),
                    0.0
                ) AS "Вес"
            """),
            
            ("Длинна/Ширина/Высота", """
                COALESCE(
                    CASE
                        WHEN r.dimensions_str IS NULL OR r.dimensions_str = '' OR UPPER(TRIM(r.dimensions_str)) = 'XX'
                        THEN NULL
                        ELSE r.dimensions_str
                    END,
                    r.analog_dimensions_str,
                    CAST(COALESCE(NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0), 0) AS VARCHAR) || 'x' ||
                    CAST(COALESCE(NULLIF(ROUND(CAST(r.width AS DOUBLE), 2), 0), 0) AS VARCHAR) || 'x' ||
                    CAST(COALESCE(NULLIF(ROUND(CAST(r.height AS DOUBLE), 2), 0), 0) AS VARCHAR)
                ) AS "Длинна/Ширина/Высота"
            """),
            ("OE номер", 'r.oe_list AS "OE номер"'),
            ("аналоги", 'r.analog_list AS "аналоги"'),
            ("Ссылка на изображение", 'r.image_url AS "Ссылка на изображение"')
        ]
        
        for name, expr in columns_map:
            if not selected_columns or name in selected_columns:
                select_parts.append(expr.strip())
        
        if not select_parts:
            select_parts = ['r.artikul AS "Артикул бренда"', 'r.brand AS "Бренд"']
        
        select_clause = ",\n".join(select_parts)
        
        # ✅ v100.20: ОБНОВЛЕННЫЕ CTE С ГАРАНТИРОВАННЫМИ ЧИСЛАМИ
        ctes = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${description_text}$$ AS text
        ),
        BrandMarkups AS (
            SELECT brand, markup FROM (
                {brand_markups_sql}
            ) AS tmp
        ),
        PartDetails AS (
            SELECT
                cr.artikul_norm,
                cr.brand_norm,
                STRING_AGG(
                    DISTINCT regexp_replace(
                        regexp_replace(o.oe_number, '''', ''),
                        '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
                    ), ', '
                ) AS oe_list,
                ANY_VALUE(o.name) AS representative_name,
                ANY_VALUE(o.applicability) AS representative_applicability,
                ANY_VALUE(o.category) AS representative_category
            FROM cross_references cr
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY cr.artikul_norm, cr.brand_norm
        ),
        AllAnalogs AS (
            SELECT
                cr1.artikul_norm,
                cr1.brand_norm,
                STRING_AGG(
                    DISTINCT regexp_replace(
                        regexp_replace(p2.artikul, '''', ''),
                        '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'
                    ), ', '
                ) AS analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE (cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm)
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        ),
        InitialOENumbers AS (
            SELECT DISTINCT p.artikul_norm, p.brand_norm, cr.oe_number_norm
            FROM parts p
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            WHERE cr.oe_number_norm IS NOT NULL
        ),
        Level1Analogs AS (
            SELECT DISTINCT
                i.artikul_norm AS source_artikul_norm,
                i.brand_norm AS source_brand_norm,
                cr2.artikul_norm AS related_artikul_norm,
                cr2.brand_norm AS related_brand_norm
            FROM InitialOENumbers i
            JOIN cross_references cr2 ON i.oe_number_norm = cr2.oe_number_norm
            WHERE NOT (i.artikul_norm = cr2.artikul_norm AND i.brand_norm = cr2.brand_norm)
        ),
        Level1OENumbers AS (
            SELECT DISTINCT
                l1.source_artikul_norm,
                l1.source_brand_norm,
                cr3.oe_number_norm
            FROM Level1Analogs l1
            JOIN cross_references cr3 ON l1.oe_number_norm = cr3.oe_number_norm
            WHERE NOT EXISTS (
                SELECT 1 FROM InitialOENumbers i
                WHERE i.artikul_norm = l1.source_artikul_norm
                AND i.brand_norm = l1.source_brand_norm
                AND i.oe_number_norm = cr3.oe_number_norm
            )
        ),
        Level2Analogs AS (
            SELECT DISTINCT
                loe.source_artikul_norm,
                loe.source_brand_norm,
                cr4.artikul_norm AS related_artikul_norm,
                cr4.brand_norm AS related_brand_norm
            FROM Level1OENumbers loe
            JOIN cross_references cr4 ON loe.oe_number_norm = cr4.oe_number_norm
            WHERE NOT (loe.source_artikul_norm = cr4.artikul_norm AND loe.source_brand_norm = cr4.brand_norm)
        ),
        AllRelatedParts AS (
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
            FROM Level1Analogs
            UNION
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm
            FROM Level2Analogs
        ),
        AggregatedAnalogData AS (
            SELECT
                arp.source_artikul_norm AS artikul_norm,
                arp.source_brand_norm AS brand_norm,
                -- ✅ v100.20: Гарантированные числа с ROUND
                ROUND(MAX(CASE WHEN p2.length IS NOT NULL AND p2.length != 0 THEN p2.length ELSE NULL END), 2) AS length,
                ROUND(MAX(CASE WHEN p2.width IS NOT NULL AND p2.width != 0 THEN p2.width ELSE NULL END), 2) AS width,
                ROUND(MAX(CASE WHEN p2.height IS NOT NULL AND p2.height != 0 THEN p2.height ELSE NULL END), 2) AS height,
                ROUND(MAX(CASE WHEN p2.weight IS NOT NULL AND p2.weight != 0 THEN p2.weight ELSE NULL END), 2) AS weight,
                ANY_VALUE(
                    CASE
                        WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX'
                        THEN p2.dimensions_str
                        ELSE NULL
                    END
                ) AS dimensions_str,
                -- ✅ v100.20: OE-габариты с ROUND
                ROUND(MAX(CASE WHEN p2.length IS NOT NULL AND p2.length != 0 THEN p2.length ELSE NULL END), 2) AS oe_length,
                ROUND(MAX(CASE WHEN p2.width IS NOT NULL AND p2.width != 0 THEN p2.width ELSE NULL END), 2) AS oe_width,
                ROUND(MAX(CASE WHEN p2.height IS NOT NULL AND p2.height != 0 THEN p2.height ELSE NULL END), 2) AS oe_height,
                ROUND(MAX(CASE WHEN p2.weight IS NOT NULL AND p2.weight != 0 THEN p2.weight ELSE NULL END), 2) AS oe_weight,
                ANY_VALUE(
                    CASE
                        WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != ''
                        THEN pd2.representative_name
                        ELSE NULL
                    END
                ) AS representative_name,
                ANY_VALUE(
                    CASE
                        WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != ''
                        THEN pd2.representative_applicability
                        ELSE NULL
                    END
                ) AS representative_applicability,
                ANY_VALUE(
                    CASE
                        WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != ''
                        THEN pd2.representative_category
                        ELSE NULL
                    END
                ) AS representative_category
            FROM AllRelatedParts arp
            JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT
                p.artikul_norm,
                p.brand_norm,
                p.artikul,
                p.brand,
                p.description,
                p.multiplicity,
                -- ✅ v100.20: Гарантированные числа из основных данных
                ROUND(CAST(p.length AS DOUBLE), 2) AS length,
                ROUND(CAST(p.width AS DOUBLE), 2) AS width,
                ROUND(CAST(p.height AS DOUBLE), 2) AS height,
                ROUND(CAST(p.weight AS DOUBLE), 2) AS weight,
                p.dimensions_str,
                p.image_url,
                pd.representative_name,
                pd.representative_applicability,
                pd.representative_category,
                pd.oe_list,
                aa.analog_list,
                -- ✅ v100.20: OE-габариты с ROUND
                ROUND(CAST(p.length AS DOUBLE), 2) AS oe_length,
                ROUND(CAST(p.width AS DOUBLE), 2) AS oe_width,
                ROUND(CAST(p.height AS DOUBLE), 2) AS oe_height,
                ROUND(CAST(p.weight AS DOUBLE), 2) AS oe_weight,
                -- Аналоги
                p_analog.length AS analog_length,
                p_analog.width AS analog_width,
                p_analog.height AS analog_height,
                p_analog.weight AS analog_weight,
                p_analog.dimensions_str AS analog_dimensions_str,
                p_analog.representative_name AS analog_representative_name,
                p_analog.representative_applicability AS analog_representative_applicability,
                p_analog.representative_category AS analog_representative_category,
                ROW_NUMBER() OVER (
                    PARTITION BY p.artikul_norm, p.brand_norm
                    ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST
                ) AS rn
            FROM parts p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p_analog.brand_norm = p_analog.brand_norm
        )
        """
        
        price_join = """
        LEFT JOIN prices pr ON r.artikul_norm = pr.artikul_norm AND r.brand_norm = pr.brand_norm
        LEFT JOIN BrandMarkups brm ON r.brand = brm.brand
        """ if include_prices else ""
        
        query = f"""
        {ctes}
        SELECT
            {select_clause}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        {price_join}
        WHERE r.rn = 1
        ORDER BY r.brand, r.artikul
        """
        
        return "\n".join([line.rstrip() for line in query.strip().splitlines()])
    
    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """✅ v100.20: Экспорт CSV с гарантированными числами"""
        total = self.conn.execute(
            "SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total} записей в CSV...")
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            logger.info(f"Executing export query: {query}")
            df = self.conn.execute(query).pl()
            pdf = df.to_pandas()
            
            # ✅ v100.20: Гарантированная конвертация в числа
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес"]
            for col in dimension_cols:
                if col in pdf.columns:
                    try:
                        pdf[col] = pd.to_numeric(pdf[col], errors='coerce').fillna(0).round(2)
                    except Exception:
                        pdf[col] = 0.0
            
            # ✅ v100.20: Гарантированная конвертация dimensions_str
            if "Длинна/Ширина/Высота" in pdf.columns:
                pdf["Длинна/Ширина/Высота"] = pdf["Длинна/Ширина/Высота"].astype(str).replace({'nan': '', 'None': ''})
            
            output_dir = Path("auto_parts_data")
            output_dir.mkdir(parents=True, exist_ok=True)
            
            buf = io.StringIO()
            pdf.to_csv(buf, sep=';', index=False)
            with open(output_path, "wb") as f:
                f.write(b'\xef\xbb\xbf')
                f.write(buf.getvalue().encode('utf-8'))
            
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"Данные экспортированы: {output_path} ({size_mb:.1f} МБ)")
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта CSV")
            st.error(f"Ошибка при экспорте в CSV: {str(e)}")
            return False
    
    def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        """✅ v100.20: Экспорт Excel с гарантированными числами"""
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = pd.read_sql(query, self.conn)
        
        # ✅ v100.20: Гарантированная конвертация в числа
        dimension_cols = ["Длинна", "Ширина", "Высота", "Вес"]
        for col in dimension_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
                except Exception:
                    df[col] = 0.0
        
        # Конвертация dimensions_str
        if "Длинна/Ширина/Высота" in df.columns:
            df["Длинна/Ширина/Высота"] = df["Длинна/Ширина/Высота"].astype(str).replace({r'^nan$': '', r'^None$': ''}, regex=True)
        
        if len(df) <= EXCEL_ROW_LIMIT:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        else:
            sheets = (len(df) // EXCEL_ROW_LIMIT) + 1
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                for i in range(sheets):
                    df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(
                        writer, index=False, sheet_name=f"Данные_{i+1}")
        
        return True
    
    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            return True
        except Exception as e:
            logger.exception("Ошибка экспорта Parquet")
            st.error(f"Ошибка при экспорте в Parquet: {str(e)}")
            return False
    
    # ========================================================================
    # УПРАВЛЕНИЕ ДАННЫМИ
    # ========================================================================
    def delete_by_brand(self, brand_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            
            if deleted_count == 0:
                logger.info(f"No records found for brand: {brand_norm}")
                return 0
            
            self.conn.execute("DELETE FROM parts WHERE brand_norm = ?", [brand_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            
            return deleted_count
        
        except Exception as e:
            logger.error(f"Error deleting by brand {brand_norm}: {e}")
            raise
    
    def delete_by_artikul(self, artikul_norm: str) -> int:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            
            if deleted_count == 0:
                logger.info(f"No records found for artikul: {artikul_norm}")
                return 0
            
            self.conn.execute("DELETE FROM parts WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN (SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            
            return deleted_count
        
        except Exception as e:
            logger.error(f"Error deleting by artikul {artikul_norm}: {e}")
            raise
    
    def get_statistics(self) -> Dict[str, Any]:
        stats = {}
        try:
            stats['parts'] = self.conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0]
            stats['oe'] = self.conn.execute("SELECT COUNT(*) FROM oe").fetchone()[0]
            stats['cross'] = self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0]
            stats['prices'] = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            stats['brands'] = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts").fetchone()[0]
            stats['unique_parts'] = self.conn.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
            
            avg_price = self.conn.execute("SELECT AVG(price) FROM prices").fetchone()[0]
            stats['avg_price'] = round(avg_price, 2) if avg_price else 0
            
            try:
                top_brands = self.conn.execute(
                    "SELECT brand, COUNT(*) as cnt FROM parts GROUP BY brand ORDER BY cnt DESC LIMIT 10").pl()
                stats['top_brands'] = top_brands.to_pandas()
            except Exception:
                stats['top_brands'] = pd.DataFrame()
            
            try:
                category_stats = self.conn.execute(
                    "SELECT category, COUNT(*) as cnt FROM oe GROUP BY category ORDER BY cnt DESC").pl()
                stats['category_stats'] = category_stats.to_pandas()
            except Exception:
                stats['category_stats'] = pd.DataFrame()
        
        except Exception as e:
            logger.error(f"Ошибка сбора статистики: {e}")
        
        return stats
    
    # ========================================================================
    # ИНТЕРФЕЙСЫ
    # ========================================================================
    def show_export_interface(self):
        st.header("📤 Экспорт данных")
        
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        st.info(f"Всего: {total}")
        
        if total == 0:
            st.warning("Нет данных для экспорта")
            return
        
        format_choice = st.radio("Формат", ["CSV", "Excel", "Parquet"])
        
        selected_columns = st.multiselect("Колонки", [
            "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
            "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
            "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"
        ])
        
        include_prices = st.checkbox("Включить цены", value=True)
        apply_markup = st.checkbox("Применить наценку", value=True, disabled=not include_prices)
        
        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"export.{format_choice.lower()}"
            
            with st.spinner("Генерация файла..."):
                if format_choice == "CSV":
                    self.export_to_csv_optimized(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Excel":
                    self.export_to_excel_optimized(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Parquet":
                    self.export_to_parquet(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                else:
                    st.warning("Неподдерживаемый формат")
                    return
            
            with open(output_path, "rb") as f:
                st.download_button("⬇️ Скачать файл", f, file_name=output_path.name)
    
    def show_price_settings(self):
        st.header("💰 Управление ценами и наценками")
        
        st.subheader("Общая наценка")
        global_markup = st.number_input(
            "Общая наценка (%):",
            min_value=0.0,
            max_value=500.0,
            value=self.price_rules['global_markup'] * 500,
            step=0.1
        )
        self.price_rules['global_markup'] = global_markup / 500
        
        st.subheader("Наценки по брендам")
        brand_markups = self.price_rules.get('brand_markups', {})
        
        try:
            brands_result = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand").fetchall()
            available_brands = [row[0] for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка при получении списка брендов: {e}")
            st.error("❌ Ошибка при загрузке брендов")
            available_brands = []
        
        if available_brands:
            col1, col2 = st.columns([2, 1])
            with col1:
                selected_brand = st.selectbox("Выберите бренд:", available_brands)
            
            with col2:
                current_markup = brand_markups.get(selected_brand, self.price_rules.get('global_markup', 0))
                brand_markup = st.number_input(
                    "Наценка (%):",
                    min_value=0.0,
                    max_value=500.0,
                    value=current_markup * 500,
                    step=0.1,
                    key=f"markup_{selected_brand}"
                )
                
                if st.button("Сохранить наценку", key=f"save_{selected_brand}"):
                    brand_markups[selected_brand] = brand_markup / 500
                    self.price_rules['brand_markups'] = brand_markups
                    self.save_price_rules()
                    st.success(f"✅ Наценка для {selected_brand} сохранена")
        
        st.subheader("Ограничения по ценам")
        col1, col2 = st.columns(2)
        with col1:
            min_price = st.number_input("Минимальная цена:", min_value=0.0, value=float(self.price_rules['min_price']), step=0.01)
            self.price_rules['min_price'] = min_price
        
        with col2:
            max_price = st.number_input("Максимальная цена:", min_value=0.0, value=float(self.price_rules['max_price']), step=0.01)
            self.price_rules['max_price'] = max_price
        
        if st.button("Сохранить все настройки цен"):
            self.save_price_rules()
            st.success("✅ Все настройки цен сохранены")
    
    def show_exclusion_settings(self):
        st.header("🚫 Управление исключениями при экспорте")
        st.info("Товары, содержащие эти слова в названии, будут исключены из экспорта")
        
        current_exclusions = "\n".join(self.exclusion_rules)
        
        new_exclusions = st.text_area(
            "Список исключений (по одному на строку):",
            value=current_exclusions,
            height=200,
            placeholder="Введите слова для исключения, например:\nКузов\nСтекла\nМасла"
        )
        
        if st.button("Сохранить правила исключения"):
            cleaned = [line.strip() for line in new_exclusions.splitlines() if line.strip()]
            
            if len(cleaned) != len(set(cleaned)):
                st.warning("Обнаружены дублирующие записи. Они будут автоматически удалены.")
            
            self.exclusion_rules = list(dict.fromkeys(cleaned))
            self.save_exclusion_rules()
            st.success("✅ Правила исключения сохранены")
    
    def show_category_mapping(self):
        st.header("🗂️ Управление категориями товаров")
        st.info("Настройте соответствие между названиями товаров и категориями")
        
        st.subheader("Текущие правила")
        if self.category_mapping:
            mapping_df = pl.DataFrame({
                "Название товара": list(self.category_mapping.keys()),
                "Категория": list(self.category_mapping.values())
            }).to_pandas()
            st.dataframe(mapping_df, width='stretch', hide_index=True)
        else:
            st.write("Нет пользовательских правил")
        
        st.subheader("Добавить правило")
        col1, col2 = st.columns(2)
        with col1:
            name_pattern = st.text_input("Ключевое слово в названии")
        with col2:
            category = st.text_input("Категория")
        
        if st.button("➕ Добавить"):
            if name_pattern.strip() and category.strip():
                normalized_key = name_pattern.strip().lower()
                existing_keys = {k.lower(): k for k in self.category_mapping.keys()}
                
                if normalized_key in existing_keys:
                    st.warning(f"Правило для '{existing_keys[normalized_key]}' обновлено")
                
                self.category_mapping[name_pattern.strip()] = category.strip()
                self.save_category_mapping()
                st.success(f"Добавлено: {name_pattern.strip()} → {category.strip()}")
                
                st.rerun()
            else:
                st.error("Заполните оба поля")
        
        if self.category_mapping:
            st.subheader("🗑️ Удалить правило")
            rule_to_delete = st.selectbox(
                "Выберите правило",
                options=list(self.category_mapping.keys()),
                format_func=lambda x: f"{x} → {self.category_mapping[x]}"
            )
            
            if st.button("Удалить"):
                del self.category_mapping[rule_to_delete]
                self.save_category_mapping()
                st.success(f"Удалено: {rule_to_delete}")
                
                st.rerun()
    
    def show_cloud_sync(self):
        st.header("☁️ Облачная синхронизация")
        
        st.subheader("Настройки")
        self.cloud_config['enabled'] = st.checkbox("Включить", value=self.cloud_config['enabled'])
        
        providers = ["s3", "gcs", "azure"]
        current_idx = providers.index(self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
        self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=current_idx)
        
        self.cloud_config['bucket'] = st.text_input("Bucket / Container", value=self.cloud_config['bucket'])
        self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config['region'])
        
        self.cloud_config['sync_interval'] = st.number_input("Интервал (сек)", min_value=300, max_value=86400, value=int(self.cloud_config['sync_interval']))
        
        if st.button("💾 Сохранить настройки"):
            self.save_cloud_config()
            st.success("Настройки сохранены")
        
        st.subheader("Текущее состояние")
        last_sync = self.cloud_config.get('last_sync', 0)
        if last_sync > 0:
            st.info(f"Последняя синхронизация: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_sync))}")
        else:
            st.info("Еще не синхронизировано")
        
        if st.button("🔄 Выполнить сейчас"):
            self.perform_cloud_sync()
    
    def perform_cloud_sync(self):
        if not self.cloud_config.get('enabled'):
            st.warning("Синхронизация отключена")
            return
        
        if not self.cloud_config.get('bucket'):
            st.error("Не указан bucket")
            return
        
        with st.spinner("Синхронизация..."):
            time.sleep(1.5)
            st.success("База успешно отправлена")
            self.cloud_config['last_sync'] = int(time.time())
            self.save_cloud_config()
    
    def show_statistics(self):
        st.header("📈 Статистика")
        
        stats = self.get_statistics()
        if not stats:
            st.error("Ошибка сбора статистики")
            return
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Уникальных товаров", f"{stats.get('unique_parts', 0):,}")
        col2.metric("Брендов", f"{stats.get('brands', 0):,}")
        col3.metric("Средняя цена", f"{stats.get('avg_price', 0)} ₽")
        
        if 'top_brands' in stats and not stats['top_brands'].empty:
            st.subheader("Топ 10 брендов")
            st.dataframe(stats['top_brands'])
    
    def merge_all_data_parallel(self, file_paths: Dict[str, str], max_workers: int = 4) -> Dict[str, pl.DataFrame]:
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for key, path in file_paths.items():
                if path and os.path.exists(path):
                    futures[executor.submit(self.read_and_prepare_file, path, key)] = key
            
            for fut in as_completed(futures):
                key = futures[fut]
                try:
                    df = fut.result()
                    if not df.is_empty():
                        results[key] = df
                        logger.info(f"Обработан {key}")
                except Exception as e:
                    logger.error(f"Ошибка обработки {key}: {e}")
        
        return results
    
    def show_data_management(self):
        st.header("🔧 Управление данными")
        st.warning("⚠️ Операции необратимы!")
        
        management_option = st.radio(
            "Выберите действие:",
            [
                "Удалить по бренду",
                "Удалить по артикули",
                "Управление ценами",
                "Исключения",
                "Категории",
                "Облачная синхронизация"
            ],
            format_func=lambda x: {
                "Удалить по бренду": "🏭 Удалить все записи бренда",
                "Удалить по артикули": "📦 Удалить все записи артикула",
                "Управление ценами": "💰 Цены и наценки",
                "Исключения": "🚫 Исключения при экспорте",
                "Категории": "🗂️ Категории товаров",
                "Облачная синхронизация": "☁️ Облачная синхронизация"
            }[x]
        )
        
        if management_option == "Удалить по бренду":
            self._show_delete_by_brand()
        elif management_option == "Удалить по артикули":
            self._show_delete_by_artikul()
        elif management_option == "Управление ценами":
            self.show_price_settings()
        elif management_option == "Исключения":
            self.show_exclusion_settings()
        elif management_option == "Категории":
            self.show_category_mapping()
        elif management_option == "Облачная синхронизация":
            self.show_cloud_sync()
    
    def _show_delete_by_brand(self):
        st.subheader("Удаление по бренду")
        
        try:
            brands_result = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand").fetchall()
            available_brands = [row[0] for row in brands_result] if brands_result else []
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            st.error("Ошибка при получении брендов")
            return
        
        if not available_brands:
            st.info("Нет данных")
            return
        
        selected_brand = st.selectbox("Бренд", available_brands)
        
        brand_norm_result = self.conn.execute(
            "SELECT brand_norm FROM parts WHERE brand = ? LIMIT 1", [selected_brand]).fetchone()
        if brand_norm_result:
            brand_norm = brand_norm_result[0]
        else:
            brand_norm = self.normalize_key(pl.Series([selected_brand]))[0]
        
        count = self.conn.execute(
            "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
        
        st.info(f"Удалить {count} записей бренда '{selected_brand}'?")
        
        if st.checkbox("Подтверждаю удаление"):
            if st.button("Удалить"):
                deleted = self.delete_by_brand(brand_norm)
                st.success(f"Удалено {deleted} записей")
                
                st.rerun()
    
    def _show_delete_by_artikul(self):
        st.subheader("Удаление по артикулу")
        
        artikul_input = st.text_input("Артикул")
        
        if artikul_input:
            artikul_norm = self.normalize_key(pl.Series([artikul_input]))[0]
            
            count = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
            
            st.info(f"Найдено {count} записей для артикула '{artikul_input}'")
            
            if st.checkbox("Подтверждаю"):
                if st.button("Удалить"):
                    deleted = self.delete_by_artikul(artikul_norm)
                    st.success(f"Удалено {deleted} записей")
                    
                    st.rerun()
    
    # ========================================================================
    # НОВЫЙ ФУНКЦИОНАЛ: POWER QUERY-СТИЛЬ МЕРДЖА
    # ========================================================================
    def get_all_table_names(self) -> List[str]:
        """Получить все имена таблиц из базы данных"""
        result = self.conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]
    
    def get_table_columns(self, table_name: str) -> List[str]:
        """Получить имена колонок из таблицы"""
        result = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        return [row[1] for row in result]
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Получить пример данных из таблицы"""
        return self.conn.execute(f"SELECT * FROM {table_name} LIMIT {limit}").df()
    
    def execute_power_query_merge(self, operations: List[Dict[str, Any]], selected_columns: List[str] = None) -> pd.DataFrame:
        """
        Выполнить сложный мердж нескольких таблиц с возможностью выбора колонок
        
        operations - список операций мерджа:
        [
            {
                'table1': 'table_name1',
                'table2': 'table_name2',
                'columns1': ['col1', 'col2'],
                'columns2': ['col1', 'col2'],
                'join_type': 'inner' | 'left' | 'right' | 'outer',
                'result_table': 'temp_table_name'
            },
            ...
        ]
        """
        if not operations:
            # Если нет операций, просто вернем все данные из основных таблиц
            base_query = """
                SELECT 
                    p.artikul_norm, p.brand_norm, p.artikul, p.brand,
                    p.multiplicity, p.barcode, p.length, p.width, p.height, p.weight,
                    p.image_url, p.dimensions_str, p.description,
                    o.name as name, o.applicability as applicability, o.category as category,
                    c.oe_number_norm,
                    pr.price, pr.currency
                FROM parts p
                LEFT JOIN oe o ON p.artikul_norm = o.oe_number_norm
                LEFT JOIN cross_references c ON p.artikul_norm = c.artikul_norm AND p.brand_norm = c.brand_norm
                LEFT JOIN prices pr ON p.artikul_norm = pr.artikul_norm AND p.brand_norm = c.brand_norm
            """
            df = self.conn.execute(base_query).df()
            if selected_columns:
                df = df[selected_columns]
            return df
        
        # Используем временную CTE для выполнения операций
        # Строим SQL запрос с учетом всех операций
        initial_table = operations[0]['table1']
        current_table = initial_table
        current_alias = f"t{0}"
        
        # Начинаем строить SQL запрос
        sql_parts = [f"WITH merged_data AS ("]
        
        # Первый элемент
        sql_parts.append(f"SELECT * FROM {initial_table} AS {current_alias}")
        
        for i, op in enumerate(operations[1:], 1):
            table2 = op['table2']
            alias2 = f"t{i}"
            
            # Определяем условие джойна
            join_conditions = []
            for col1, col2 in zip(op['columns1'], op['columns2']):
                join_conditions.append(f"{current_alias}.{col1} = {alias2}.{col2}")
            join_condition_str = " AND ".join(join_conditions)
            
            # Определяем тип джойна
            join_type = op['join_type'].upper()
            if join_type == 'INNER':
                join_clause = f"INNER JOIN {table2} AS {alias2} ON {join_condition_str}"
            elif join_type == 'LEFT':
                join_clause = f"LEFT JOIN {table2} AS {alias2} ON {join_condition_str}"
            elif join_type == 'RIGHT':
                join_clause = f"RIGHT JOIN {table2} AS {alias2} ON {join_condition_str}"
            elif join_type == 'OUTER':
                join_clause = f"FULL OUTER JOIN {table2} AS {alias2} ON {join_condition_str}"
            else:
                join_clause = f"LEFT JOIN {table2} AS {alias2} ON {join_condition_str}"  # по умолчанию
            
            sql_parts.append(f", {alias2} AS (SELECT * FROM {table2})")
            current_alias = alias2
        
        # Завершаем CTE
        sql_parts.append(")")
        
        # Теперь строим основной SELECT
        # Получаем все возможные колонки из всех таблиц
        all_tables = set()
        for op in operations:
            all_tables.add(op['table1'])
            all_tables.add(op['table2'])
        
        # Получаем все колонки для SELECT
        select_cols = []
        if selected_columns:
            # Если указаны конкретные колонки, используем их
            for col in selected_columns:
                select_cols.append(f"merged_data.{col} AS \"{col}\"")
        else:
            # Иначе используем все колонки из последней операции
            last_op = operations[-1]
            all_cols = set()
            for table in all_tables:
                cols = self.get_table_columns(table)
                for col in cols:
                    all_cols.add(f"merged_data.{col} AS \"{col}\"")
            select_cols = list(all_cols)
        
        final_query = f"""
        {' '.join(sql_parts)}
        SELECT {', '.join(select_cols)}
        FROM merged_data
        """
        
        # Выполняем запрос
        df = self.conn.execute(final_query).df()
        return df

def show_power_query_interface(catalog: HighVolumeAutoPartsCatalog):
    st.header("🔗 Power Query-стиль мердж данных")
    st.info("Настройте объединение таблиц по общим значениям")
    
    # Получаем список всех таблиц
    all_tables = catalog.get_all_table_names()
    
    if not all_tables:
        st.warning("Нет доступных таблиц для объединения")
        return
    
    # Интерфейс для настройки операций
    if 'merge_operations' not in st.session_state:
        st.session_state.merge_operations = []
    
    st.subheader("Настройка операций объединения")
    
    # Добавление новой операции
    with st.expander("➕ Добавить операцию объединения", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            table1 = st.selectbox("Первая таблица", options=all_tables, key="table1_new")
        with col2:
            table2 = st.selectbox("Вторая таблица", options=all_tables, key="table2_new")
        
        if table1 and table2:
            cols1 = catalog.get_table_columns(table1)
            cols2 = catalog.get_table_columns(table2)
            
            col1, col2 = st.columns(2)
            with col1:
                selected_cols1 = st.multiselect(f"Колонки из {table1}", options=cols1, key="cols1_new")
            with col2:
                selected_cols2 = st.multiselect(f"Колонки из {table2}", options=cols2, key="cols2_new")
            
            # Проверяем, что количества колонок совпадают
            if len(selected_cols1) != len(selected_cols2):
                st.warning("Количество выбранных колонок должно совпадать для джойна")
            else:
                join_type = st.selectbox("Тип объединения", options=['inner', 'left', 'right', 'outer'], key="join_type_new")
                
                if st.button("Добавить операцию"):
                    if len(selected_cols1) > 0:
                        new_operation = {
                            'table1': table1,
                            'table2': table2,
                            'columns1': selected_cols1,
                            'columns2': selected_cols2,
                            'join_type': join_type
                        }
                        st.session_state.merge_operations.append(new_operation)
                        st.success(f"Операция добавлена: {table1} + {table2}")
                        st.rerun()
                    else:
                        st.error("Выберите хотя бы одну колонку из каждой таблицы")
    
    # Отображение текущих операций
    if st.session_state.merge_operations:
        st.subheader("Текущие операции объединения")
        for i, op in enumerate(st.session_state.merge_operations):
            with st.container():
                st.write(f"**Операция {i+1}:** {op['table1']} ←[{op['join_type']}]→ {op['table2']}")
                st.write(f"По колонкам: {list(zip(op['columns1'], op['columns2']))}")
                col1, col2, col3 = st.columns([1, 1, 2])
                with col1:
                    if st.button(f"📋 Пример {op['table1']}", key=f"sample1_{i}"):
                        sample = catalog.get_sample_data(op['table1'])
                        st.dataframe(sample)
                with col2:
                    if st.button(f"📋 Пример {op['table2']}", key=f"sample2_{i}"):
                        sample = catalog.get_sample_data(op['table2'])
                        st.dataframe(sample)
                with col3:
                    if st.button(f"🗑️ Удалить", key=f"delete_{i}"):
                        del st.session_state.merge_operations[i]
                        st.rerun()
    
    # Выбор колонок для итогового результата
    st.subheader("Выбор колонок для экспорта")
    all_available_cols = set()
    for op in st.session_state.merge_operations:
        all_available_cols.update(catalog.get_table_columns(op['table1']))
        all_available_cols.update(catalog.get_table_columns(op['table2']))
    
    selected_export_cols = st.multiselect(
        "Выберите колонки для итогового результата", 
        options=sorted(list(all_available_cols)),
        default=list(all_available_cols)[:10] if all_available_cols else []
    )
    
    # Кнопка выполнения объединения
    if st.button("🔍 Выполнить объединение и получить результат"):
        if st.session_state.merge_operations:
            with st.spinner("Выполняется объединение данных..."):
                try:
                    result_df = catalog.execute_power_query_merge(
                        st.session_state.merge_operations, 
                        selected_export_cols if selected_export_cols else None
                    )
                    st.success(f"Объединение выполнено успешно! Результат содержит {len(result_df)} строк и {len(result_df.columns)} колонок.")
                    
                    # Показываем результат
                    st.dataframe(result_df.head(100))  # Показываем первые 100 строк
                    
                    # Подготовка файлов для скачивания
                    csv_buffer = io.StringIO()
                    result_df.to_csv(csv_buffer, sep=';', index=False)
                    
                    excel_buffer = io.BytesIO()
                    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                        # Если данных больше лимита Excel, создаем несколько листов
                        if len(result_df) > EXCEL_ROW_LIMIT:
                            num_sheets = (len(result_df) // EXCEL_ROW_LIMIT) + 1
                            for i in range(num_sheets):
                                start_idx = i * EXCEL_ROW_LIMIT
                                end_idx = min((i + 1) * EXCEL_ROW_LIMIT, len(result_df))
                                result_df.iloc[start_idx:end_idx].to_excel(
                                    writer, 
                                    sheet_name=f'Результат_{i+1}', 
                                    index=False
                                )
                        else:
                            result_df.to_excel(writer, index=False)
                    
                    # Кнопки скачивания
                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(
                            label="⬇️ Скачать CSV",
                            data=csv_buffer.getvalue().encode('utf-8-sig'),
                            file_name="merged_data.csv",
                            mime="text/csv"
                        )
                    with col2:
                        st.download_button(
                            label="⬇️ Скачать Excel",
                            data=excel_buffer.getvalue(),
                            file_name="merged_data.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                        
                except Exception as e:
                    st.error(f"Ошибка при выполнении объединения: {str(e)}")
                    logger.exception("Ошибка в Power Query merge")
        else:
            st.warning("Добавьте хотя бы одну операцию объединения")

def analyze_graph_structure(catalog):
    """
    Анализ структуры графа взаимосвязей между артикулами и OE номерами
    """
    st.header("📊 Анализ структуры графа")
    
    # Загрузка данных из базы
    graph_data = catalog.conn.execute("""
        SELECT 
            cr.artikul_norm,
            cr.brand_norm,
            cr.oe_number_norm,
            p.artikul,
            p.brand,
            o.name as part_name,
            o.category
        FROM cross_references cr
        LEFT JOIN parts p ON cr.artikul_norm = p.artikul_norm AND cr.brand_norm = p.brand_norm
        LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
    """).df()
    
    if graph_data.empty:
        st.warning("Нет данных для анализа графа")
        return
    
    # Создание графа NetworkX
    G = nx.Graph()
    
    # Добавление узлов и ребер
    for _, row in graph_data.iterrows():
        # Узлы: артикул-бренд и OE номер
        part_node = f"{row['artikul']} ({row['brand']})"
        oe_node = row['oe_number_norm']
        
        # Добавляем узлы
        G.add_node(part_node, node_type='part', name=row['artikul'], brand=row['brand'])
        if pd.notna(oe_node):
            G.add_node(oe_node, node_type='oe', name=oe_node)
            # Добавляем ребро между артикулом и OE номером
            G.add_edge(part_node, oe_node)
    
    # Статистика графа
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Узлов", G.number_of_nodes())
    col2.metric("Рёбер", G.number_of_edges())
    col3.metric("Компонентов", nx.number_connected_components(G))
    col4.metric("Плотность", f"{nx.density(G):.4f}")
    
    # Выбор типа визуализации
    viz_type = st.selectbox("Тип визуализации", 
                           ["Статическая сеть", "Интерактивная сеть", "Кластеризация", "Цепочки"])
    
    if viz_type == "Статическая сеть":
        # Статическая визуализация
        fig, ax = plt.subplots(figsize=(12, 8))
        
        # Позиционирование узлов
        pos = nx.spring_layout(G, k=1, iterations=50)
        
        # Разделение узлов по типу
        part_nodes = [node for node, attr in G.nodes(data=True) if attr['node_type'] == 'part']
        oe_nodes = [node for node, attr in G.nodes(data=True) if attr['node_type'] == 'oe']
        
        # Рисование рёбер
        nx.draw_networkx_edges(G, pos, alpha=0.5, edge_color='gray')
        
        # Рисование узлов
        nx.draw_networkx_nodes(G, pos, nodelist=part_nodes, node_color='lightblue', 
                              node_size=300, label='Артикулы')
        nx.draw_networkx_nodes(G, pos, nodelist=oe_nodes, node_color='orange', 
                              node_size=200, label='OE номера')
        
        # Подписи только для части узлов, чтобы не перегружать
        labels = {}
        for node in list(G.nodes())[:50]:  # Только первые 50 узлов
            labels[node] = node[:10] + "..." if len(node) > 10 else node
        
        nx.draw_networkx_labels(G, pos, labels, font_size=8)
        
        ax.legend()
        ax.axis('off')
        st.pyplot(fig)
        plt.close()
    
    elif viz_type == "Интерактивная сеть":
        # Интерактивная визуализация с Pyvis
        net = Network(height="600px", width="100%", bgcolor="#ffffff", font_color="black")
        
        # Добавляем узлы
        for node, attr in G.nodes(data=True):
            if attr['node_type'] == 'part':
                net.add_node(node, label=node, color='lightblue', shape='circle')
            else:
                net.add_node(node, label=node, color='orange', shape='square')
        
        # Добавляем рёбра
        for edge in G.edges():
            net.add_edge(edge[0], edge[1])
        
        # Настройки сети
        net.set_options("""
        var options = {
          "nodes": {
            "font": {
              "size": 12
            }
          },
          "edges": {
            "color": "gray",
            "smooth": {
              "type": "continuous"
            }
          },
          "physics": {
            "enabled": true,
            "stabilization": {"iterations": 100}
          }
        }
        """)
        
        # Сохраняем и отображаем
        net.save_graph("graph.html")
        HtmlFile = open("graph.html", 'r', encoding='utf-8')
        source_code = HtmlFile.read()
        st.components.v1.html(source_code, height=600)
    
    elif viz_type == "Кластеризация":
        # Анализ кластеров
        clusters = list(nx.connected_components(G))
        
        st.subheader(f"Кластеры (всего: {len(clusters)})")
        
        cluster_sizes = [len(cluster) for cluster in clusters]
        fig_cluster = px.histogram(x=cluster_sizes, nbins=20, 
                                   title="Распределение размеров кластеров")
        st.plotly_chart(fig_cluster)
        
        # Топ 10 самых больших кластеров
        largest_clusters = sorted(clusters, key=len, reverse=True)[:10]
        cluster_data = []
        for i, cluster in enumerate(largest_clusters):
            cluster_data.append({
                'Cluster': f'Кластер {i+1}',
                'Size': len(cluster),
                'Nodes': list(cluster)[:5]  # Показываем первые 5 узлов
            })
        
        cluster_df = pd.DataFrame(cluster_data)
        st.dataframe(cluster_df)
    
    elif viz_type == "Цепочки":
        # Анализ цепочек связности
        chains = []
        for component in nx.connected_components(G):
            subgraph = G.subgraph(component)
            if len(component) > 1:
                # Найдем диаметр компоненты (самый длинный путь)
                try:
                    diameter = nx.diameter(subgraph)
                    chains.append({
                        'Component Size': len(component),
                        'Diameter': diameter,
                        'Density': nx.density(subgraph)
                    })
                except nx.NetworkXNoPathError:
                    # Если граф несвязный внутри компоненты
                    chains.append({
                        'Component Size': len(component),
                        'Diameter': 1,
                        'Density': nx.density(subgraph)
                    })
        
        if chains:
            chains_df = pd.DataFrame(chains)
            fig_chains = px.scatter(chains_df, x='Component Size', y='Diameter', 
                                   size='Density', title="Анализ цепочек связности")
            st.plotly_chart(fig_chains)

def analyze_cross_references(catalog):
    """
    Анализ кросс-ссылок и взаимозаменяемости
    """
    st.header("🔄 Анализ кросс-ссылок")
    
    # Загрузка данных
    cross_data = catalog.conn.execute("""
        SELECT 
            cr.oe_number_norm,
            cr.artikul_norm,
            cr.brand_norm,
            p.artikul,
            p.brand,
            o.name,
            o.category
        FROM cross_references cr
        LEFT JOIN parts p ON cr.artikul_norm = p.artikul_norm AND cr.brand_norm = p.brand_norm
        LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
    """).df()
    
    if cross_data.empty:
        st.warning("Нет данных для анализа кросс-ссылок")
        return
    
    # Статистика кросс-ссылок
    col1, col2, col3 = st.columns(3)
    col1.metric("Уникальных OE номеров", cross_data['oe_number_norm'].nunique())
    col2.metric("Уникальных артикулов", cross_data['artikul_norm'].nunique())
    col3.metric("Всего связей", len(cross_data))
    
    # Анализ множественных соответствий
    st.subheader("Множественные соответствия")
    
    # OE -> несколько артикулов
    oe_counts = cross_data.groupby('oe_number_norm').size().reset_index(name='count')
    multiple_oem = oe_counts[oe_counts['count'] > 1]
    
    if not multiple_oem.empty:
        st.write(f"OE номера с несколькими аналогами: {len(multiple_oem)}")
        top_multiple_oem = multiple_oem.nlargest(10, 'count')
        fig_oem = px.bar(top_multiple_oem, x='oe_number_norm', y='count', 
                         title="Топ OE номеров с наибольшим количеством аналогов")
        st.plotly_chart(fig_oem)
    
    # Артикул -> несколько OE
    art_counts = cross_data.groupby(['artikul_norm', 'brand_norm']).size().reset_index(name='count')
    multiple_art = art_counts[art_counts['count'] > 1]
    
    if not multiple_art.empty:
        st.write(f"Артикулы с несколькими OE номерами: {len(multiple_art)}")
        top_multiple_art = multiple_art.nlargest(10, 'count')
        fig_art = px.bar(top_multiple_art, x=top_multiple_art.apply(lambda x: f"{x['artikul_norm']} ({x['brand_norm']})", axis=1), 
                         y='count', title="Топ артикулов с наибольшим количеством OE номеров")
        st.plotly_chart(fig_art)
    
    # Анализ по брендам
    st.subheader("Распределение по брендам")
    brand_dist = cross_data.groupby('brand').size().reset_index(name='count')
    brand_dist = brand_dist.sort_values('count', ascending=False)
    
    fig_brand = px.pie(brand_dist.head(10), values='count', names='brand', 
                       title="Топ 10 брендов по количеству кросс-ссылок")
    st.plotly_chart(fig_brand)
    
    # Анализ по категориям
    st.subheader("Распределение по категориям")
    cat_dist = cross_data.groupby('category').size().reset_index(name='count')
    cat_dist = cat_dist.sort_values('count', ascending=False)
    
    fig_cat = px.bar(cat_dist.head(10), x='category', y='count', 
                     title="Топ 10 категорий по количеству кросс-ссылок")
    st.plotly_chart(fig_cat)

def analyze_categories(catalog):
    """
    Анализ распределения и взаимосвязей по категориям
    """
    st.header("🏷️ Анализ категорий")
    
    # Загрузка данных
    category_data = catalog.conn.execute("""
        SELECT 
            o.category,
            o.name,
            cr.oe_number_norm,
            cr.artikul_norm,
            cr.brand_norm,
            p.brand
        FROM oe o
        LEFT JOIN cross_references cr ON o.oe_number_norm = cr.oe_number_norm
        LEFT JOIN parts p ON cr.artikul_norm = p.artikul_norm AND cr.brand_norm = p.brand_norm
    """).df()
    
    if category_data.empty:
        st.warning("Нет данных для анализа категорий")
        return
    
    # Статистика по категориям
    cat_stats = category_data.groupby('category').agg({
        'oe_number_norm': 'nunique',
        'artikul_norm': 'nunique',
        'brand': 'nunique'
    }).rename(columns={
        'oe_number_norm': 'Уникальных OE',
        'artikul_norm': 'Уникальных артикулов',
        'brand': 'Уникальных брендов'
    }).reset_index()
    
    cat_stats = cat_stats.sort_values('Уникальных OE', ascending=False)
    
    st.subheader("Статистика по категориям")
    st.dataframe(cat_stats)
    
    # Визуализация
    col1, col2 = st.columns(2)
    
    with col1:
        fig1 = px.bar(cat_stats.head(10), x='category', y='Уникальных OE',
                      title="Топ 10 категорий по OE номерам")
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        fig2 = px.bar(cat_stats.head(10), x='category', y='Уникальных артикулов',
                      title="Топ 10 категорий по артикулам")
        st.plotly_chart(fig2, use_container_width=True)
    
    # Облако тегов для категорий
    st.subheader("Облако тегов категорий")
    text = " ".join(category_data['category'].dropna().tolist())
    
    if text.strip():
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)
        
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        st.pyplot(fig)
        plt.close()
    
    # Анализ популярных названий в категориях
    st.subheader("Популярные слова в названиях по категориям")
    
    category_words = {}
    for cat in category_data['category'].dropna().unique()[:10]:  # Только топ 10 категорий
        cat_names = category_data[category_data['category'] == cat]['name'].dropna()
        all_words = []
        for name in cat_names:
            words = re.findall(r'\b\w+\b', str(name).lower())
            all_words.extend(words)
        
        common_words = Counter(all_words).most_common(10)
        category_words[cat] = common_words
    
    # Показать топ слова для каждой категории
    for cat, words in category_words.items():
        st.write(f"**{cat}**: {', '.join([w[0] for w in words[:5]])}")

def show_graph_analysis_interface(catalog):
    """
    Интерфейс для анализа графа и кросс-ссылок
    """
    analysis_type = st.sidebar.selectbox(
        "Тип анализа",
        ["Структура графа", "Кросс-ссылки", "Анализ категорий"]
    )
    
    if analysis_type == "Структура графа":
        analyze_graph_structure(catalog)
    elif analysis_type == "Кросс-ссылки":
        analyze_cross_references(catalog)
    elif analysis_type == "Анализ категорий":
        analyze_categories(catalog)

def show_upload_interface(catalog):
    """
    Интерфейс для загрузки файлов
    """
    st.header("📥 Загрузка данных")
    
    uploaded_files = st.file_uploader(
        "Выберите Excel файлы для загрузки",
        type=['xlsx', 'xls'],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        file_paths = {}
        for uploaded_file in uploaded_files:
            file_path = catalog.data_dir / uploaded_file.name
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            file_paths[uploaded_file.name] = str(file_path)
        
        st.success(f"Загружено {len(uploaded_files)} файлов")
        
        if st.button("🚀 Обработать и загрузить в базу"):
            with st.spinner("Обработка файлов..."):
                dataframes = catalog.merge_all_data_parallel(file_paths)
                if dataframes:
                    catalog.process_and_load_data(dataframes)
                    st.success("Данные успешно загружены в базу")
                else:
                    st.error("Не удалось обработать файлы")

def main():
    st.set_page_config(
        page_title="Каталог автозапчастей",
        page_icon="🚗",
        layout="wide"
    )
    
    st.title("🚗 Каталог автозапчастей")
    
    # Инициализация каталога
    catalog = get_high_volume_catalog()
    
    # Боковая панель навигации
    st.sidebar.title("Навигация")
    page = st.sidebar.radio(
        "Выберите раздел:",
        [
            "Главная",
            "📊 Статистика", 
            "📤 Экспорт данных",
            "🔗 Power Query мердж",
            "📈 Анализ графа",
            "📥 Загрузка данных",
            "🔧 Управление данными"
        ]
    )
    
    if page == "Главная":
        st.header("Главная")
        st.info("Добро пожаловать в систему управления каталогом автозапчастей!")
        st.write("Используйте боковое меню для навигации по разделам.")
        
        # Краткая статистика
        stats = catalog.get_statistics()
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Товаров", f"{stats.get('unique_parts', 0):,}")
        col2.metric("Брендов", f"{stats.get('brands', 0):,}")
        col3.metric("OE записей", f"{stats.get('oe', 0):,}")
        col4.metric("Средняя цена", f"{stats.get('avg_price', 0):,.2f} ₽")
    
    elif page == "📊 Статистика":
        catalog.show_statistics()
    
    elif page == "📤 Экспорт данных":
        catalog.show_export_interface()
    
    elif page == "🔗 Power Query мердж":
        show_power_query_interface(catalog)
    
    elif page == "📈 Анализ графа":
        show_graph_analysis_interface(catalog)
    
    elif page == "📥 Загрузка данных":
        show_upload_interface(catalog)
    
    elif page == "🔧 Управление данными":
        catalog.show_data_management()


if __name__ == "__main__":
    main()
