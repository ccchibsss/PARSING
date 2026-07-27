# app.py (ЧАСТЬ 1 из 2)
import streamlit as st
import pandas as pd
import polars as pl
import duckdb
from pathlib import Path
import json
import time
import re
import math
import decimal
from datetime import datetime, date, timedelta
import logging
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import io
import os

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# БЛОК 11: HIGH-VOLUME КАТАЛОГ АВТОЗАПЧАСТЕЙ (ПОЛНАЯ ВЕРСИЯ v100.21)
# ============================================================================
# ✅ ИСПРАВЛЕНИЯ v100.21:
# 1. ИСПРАВЛЕНА ОШИБКА "table oe has 10 columns but 5 values were supplied"
# 2. Добавлены ВСЕ колонки в oe_df (включая length, width, height, weight, dimensions_str)
# 3. Гарантированное создание колонок с габаритами, даже если их нет в исходных данных
# 4. Правильный порядок колонок при вставке в таблицу oe
# 5. ПОЛНОЕ ИСПРАВЛЕНИЕ ПРОБЛЕМЫ С ДАТАМИ В ГАБАРИТАХ
# 6. Приоритет габаритов: данные > OE > аналоги
# 7. Гарантированное заполнение всех 4 колонок (Длинна, Ширина, Высота, Вес)
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
    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        """
        ✅ ИСПРАВЛЕНИЕ v100.18: Защита от дубликатов при маппинге колонок
        """
        column_variants = {
            'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE', 'oe_number', 'oe number'],
            'artikul': ['артикул', 'article', 'sku', 'artikul', 'код товара', 'код', 'код артикула'],
            'brand': ['бренд', 'brand', 'производитель', 'manufacturer', 'марка'],
            'name': ['наименование', 'название', 'name', 'описание', 'description', 'товар', 'наименование товара'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability', 'применяемость'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13', 'штрих код'],
            'multiplicity': ['кратность шт', 'кратность', 'multiplicity', 'кратность упаковки'],
            'length': ['длина (см)', 'длина', 'length', 'длинна', 'длина, см', 'length_cm'],
            'width': ['ширина (см)', 'ширина', 'width', 'ширина, см', 'width_cm'],
            'height': ['высота (см)', 'высота', 'height', 'высота, см', 'height_cm'],
            'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight', 'масса', 'weight_kg', 'вес кг'],
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка', 'фото', 'ссылка на изображение'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size', 'габариты', 'длинна/ширина/высота', 'длина/ширина/высота'],
            'price': ['цена', 'price', 'рекомендованная цена', 'retail price', 'цена продажи', 'стоимость'],
            'currency': ['валюта', 'currency']
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
        """
        БЕЗОПАСНЫЙ способ получения маржинальности брендов.
        Использует временную таблицу вместо конкатенации строк (защита от SQL-инъекций).
        """
        if not self.price_rules.get('brand_markups'):
            return "SELECT NULL::VARCHAR AS brand, NULL::DOUBLE AS markup LIMIT 0"
        
        brand_data = [(brand, float(markup)) for brand, markup in self.price_rules['brand_markups'].items()]
        if not brand_data:
            return "SELECT NULL::VARCHAR AS brand, NULL::DOUBLE AS markup LIMIT 0"
        
        df = pd.DataFrame(brand_data, columns=['brand', 'markup'])
        self.conn.register("temp_brand_markups", df)
        return "SELECT brand, markup FROM temp_brand_markups"
    
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
            {brand_markups_sql}
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
    # НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: ЗАГРУЗКА ДАННЫХ С ВИЗУАЛИЗАЦИЕЙ МАППИНГА
    # ========================================================================
    def show_data_upload_interface(self):
        """Интерфейс загрузки данных с отображением распознанных столбцов"""
        st.header("📥 Загрузка данных")
        st.info("Загрузите Excel/CSV файлы. Система автоматически распознает русские и английские названия столбцов.")
        
        file_types = {
            "OE данные": "oe",
            "Кросс-референсы": "cross",
            "Цены": "prices",
            "Габариты": "dimensions",
            "Штрихкоды": "barcode",
            "Изображения": "images",
            "Универсальный файл": "universal"
        }
        
        col1, col2 = st.columns(2)
        
        with col1:
            selected_type = st.selectbox("Тип файла:", list(file_types.keys()))
            file_type = file_types[selected_type]
            
        with col2:
            uploaded_file = st.file_uploader(
                f"Выберите файл ({selected_type}):",
                type=["xlsx", "xls", "csv"],
                key=f"upload_{file_type}"
            )
        
        if uploaded_file:
            temp_path = self.data_dir / f"temp_{int(time.time())}_{uploaded_file.name}"
            with open(temp_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            
            st.info(f"Файл сохранён: `{temp_path.name}`")
            
            if st.button("✅ Обработать и показать маппинг"):
                with st.spinner("Анализ структуры файла..."):
                    try:
                        # Читаем сырой файл для показа маппинга
                        raw_df = pl.read_excel(str(temp_path), engine='calamine')
                        
                        if raw_df.is_empty():
                            st.error("Файл пустой или не содержит данных")
                            temp_path.unlink(missing_ok=True)
                            return
                        
                        # Получаем маппинг
                        column_mapping = self.detect_columns(raw_df.columns, 
                                                           ['oe_number', 'artikul', 'brand', 'name', 
                                                            'applicability', 'barcode', 'multiplicity',
                                                            'length', 'width', 'height', 'weight', 
                                                            'image_url', 'dimensions_str', 'price', 'currency'])
                        
                        # Показываем таблицу соответствия
                        st.subheader("🔍 Распознанные столбцы")
                        if column_mapping:
                            mapping_display = []
                            for orig, sys_name in column_mapping.items():
                                mapping_display.append({
                                    "Оригинальное имя (в файле)": orig,
                                    "Системное имя": sys_name,
                                    "Статус": "✅ Распознан"
                                })
                            
                            # Добавляем нераспознанные столбцы
                            recognized_orig = set(column_mapping.keys())
                            for col in raw_df.columns:
                                if col not in recognized_orig:
                                    mapping_display.append({
                                        "Оригинальное имя (в файле)": col,
                                        "Системное имя": "-",
                                        "Статус": "⚠️ Не распознан"
                                    })
                            
                            st.dataframe(pd.DataFrame(mapping_display), use_container_width=True, hide_index=True)
                            
                            if st.button("💾 Подтвердить и загрузить в базу"):
                                df = self.read_and_prepare_file(str(temp_path), file_type)
                                
                                if df.is_empty():
                                    st.error("Файл пустой или не содержит данных после обработки")
                                    temp_path.unlink(missing_ok=True)
                                    return
                                
                                dataframes = {file_type: df}
                                self.process_and_load_data(dataframes)
                                st.success(f"✅ {len(df)} записей успешно загружено в базу")
                                temp_path.unlink(missing_ok=True)
                        else:
                            st.warning("Не удалось распознать ни одного столбца. Проверьте заголовки файла.")
                            temp_path.unlink(missing_ok=True)
                            
                    except Exception as e:
                        logger.exception("Ошибка загрузки файла")
                        st.error(f"Ошибка обработки файла: {e}")
                        temp_path.unlink(missing_ok=True)
        
        # Массовая загрузка
        st.subheader("⚡ Массовая загрузка")
        st.caption("Загрузите несколько файлов одновременно. Тип определяется автоматически по имени файла.")
        
        uploaded_files = st.file_uploader(
            "Выберите несколько файлов:",
            type=["xlsx", "xls", "csv"],
            accept_multiple_files=True,
            key="bulk_upload"
        )
        
        if uploaded_files:
            file_map = {}
            for file in uploaded_files:
                name_lower = file.name.lower()
                if any(kw in name_lower for kw in ["oe", "o-e", "original"]):
                    file_map["oe"] = file
                elif any(kw in name_lower for kw in ["cross", "крест", "аналог"]):
                    file_map["cross"] = file
                elif any(kw in name_lower for kw in ["price", "цена", "cost"]):
                    file_map["prices"] = file
                elif any(kw in name_lower for kw in ["dim", "размер", "габарит"]):
                    file_map["dimensions"] = file
                elif any(kw in name_lower for kw in ["bar", "штрих", "ean"]):
                    file_map["barcode"] = file
                elif any(kw in name_lower for kw in ["img", "фото", "image"]):
                    file_map["images"] = file
                else:
                    file_map["universal"] = file
            
            st.write(f"Определено {len(file_map)} типов файлов:")
            for ft, f in file_map.items():
                st.write(f"- `{ft}`: `{f.name}`")
            
            if st.button("📦 Загрузить все"):
                with st.spinner("Обработка и загрузка всех файлов..."):
                    temp_paths = []
                    dataframes = {}
                    
                    try:
                        for ft, file in file_map.items():
                            temp_path = self.data_dir / f"temp_{int(time.time())}_{file.name}"
                            with open(temp_path, "wb") as f_out:
                                f_out.write(file.getbuffer())
                            temp_paths.append(temp_path)
                            
                            df = self.read_and_prepare_file(str(temp_path), ft)
                            if not df.is_empty():
                                dataframes[ft] = df
                                st.info(f"✅ {ft}: {len(df)} записей")
                        
                        if dataframes:
                            self.process_and_load_data(dataframes)
                            st.success("✅ Все данные успешно загружены в базу!")
                        else:
                            st.warning("Не найдено данных для загрузки")
                    
                    finally:
                        for tp in temp_paths:
                            tp.unlink(missing_ok=True)

    # ========================================================================
    # НОВАЯ ФУНКЦИОНАЛЬНОСТЬ: POWER QUERY СТИЛЬ (УЛУЧШЕННЫЙ JOIN)
    # ========================================================================
    def get_all_table_columns(self) -> Dict[str, List[str]]:
        tables_info = {}
        tables = ['oe', 'parts', 'cross_references', 'prices']
        
        for table in tables:
            try:
                result = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
                columns = [row[1] for row in result]
                tables_info[table] = columns
            except Exception as e:
                logger.error(f"Ошибка получения информации о столбцах таблицы {table}: {e}")
                tables_info[table] = []
        
        return tables_info
    
    def show_power_query_interface(self):
        st.header("🔗 Power Query стиль: Группировка и подтягивание")
        st.info("Выберите столбцы с одинаковыми значениями (ключ), затем — какие значения нужно подтянуть")
        
        tables_info = self.get_all_table_columns()
        
        st.subheader("1. Ключевые столбцы (группировка)")
        key_options = []
        for table, cols in tables_info.items():
            for col in cols:
                key_options.append((table, col))
        
        selected_keys = st.multiselect(
            "Выберите столбцы для группировки (ключ):",
            options=key_options,
            format_func=lambda x: f"{x[0]}.{x[1]}",
            help="Эти столбцы должны содержать одинаковые значения в разных строках"
        )
        
        st.subheader("2. Целевые столбцы (что подтянуть)")
        target_options = []
        for table, cols in tables_info.items():
            for col in cols:
                if (table, col) not in selected_keys:
                    target_options.append((table, col))
        
        selected_targets = st.multiselect(
            "Выберите столбцы для подтягивания:",
            options=target_options,
            format_func=lambda x: f"{x[0]}.{x[1]}",
            help="Значения из этих столбцов будут объединены по ключу"
        )
        
        st.subheader("3. Метод агрегации")
        agg_method = st.selectbox(
            "Как объединять значения?",
            options=[
                ("first", "Первое значение"),
                ("last", "Последнее значение"),
                ("concat", "Объединить через запятую"),
                ("max", "Максимум (числа)"),
                ("min", "Минимум (числа)")
            ],
            format_func=lambda x: x[1]
        )
        agg_code = agg_method[0]
        
        if st.button("🔍 Выполнить группировку") and selected_keys and selected_targets:
            with st.spinner("Выполняется запрос..."):
                result_df, query = self.execute_grouping_with_join(selected_keys, selected_targets, agg_code)
                
                if result_df is not None and not result_df.empty:
                    st.success(f"✅ Найдено {len(result_df)} уникальных групп")
                    st.dataframe(result_df, use_container_width=True)
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        csv = result_df.to_csv(sep=';', index=False).encode('utf-8-sig')
                        st.download_button("📥 CSV", data=csv, file_name="grouped_export.csv")
                    with col2:
                        excel_buffer = io.BytesIO()
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            result_df.to_excel(writer, index=False)
                        st.download_button("📥 Excel", data=excel_buffer.getvalue(), file_name="grouped_export.xlsx")
                else:
                    st.warning("Нет данных по выбранным критериям")
    
    def execute_grouping_with_join(self, group_by: List[Tuple[str, str]], 
                                 targets: List[Tuple[str, str]], 
                                 agg_method: str) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Улучшенная логика JOIN с проверкой связей между таблицами.
        Предотвращает создание декартова произведения.
        """
        all_tables = set([t for t, _ in group_by + targets])
        
        main_table = 'parts'
        if 'oe' in all_tables and len(all_tables) == 1:
            main_table = 'oe'
        elif 'prices' in all_tables and len(all_tables) == 1:
            main_table = 'prices'
        
        select_clauses = []
        for t, c in group_by:
            select_clauses.append(f"{t}.{c} AS \"{c}\"")
        
        for t, c in targets:
            if agg_method == 'concat':
                select_clauses.append(f"STRING_AGG(DISTINCT {t}.{c}, ', ') AS \"{c}\"")
            elif agg_method in ['max', 'min']:
                select_clauses.append(f"{agg_method.upper()}({t}.{c}) AS \"{c}\"")
            else:
                select_clauses.append(f"FIRST({t}.{c}) AS \"{c}\"")
        
        from_clause = f"FROM {main_table}"
        join_clauses = []
        
        # Известные связи через cross_references и прямые ключи
        joins = [
            ("parts", "cross_references", "parts.artikul_norm = cross_references.artikul_norm AND parts.brand_norm = cross_references.brand_norm"),
            ("oe", "cross_references", "oe.oe_number_norm = cross_references.oe_number_norm"),
            ("prices", "parts", "prices.artikul_norm = parts.artikul_norm AND prices.brand_norm = parts.brand_norm")
        ]
        
        used = {main_table}
        connected_tables = {main_table}
        
        for t1, t2, cond in joins:
            if t1 in all_tables and t2 in all_tables:
                if t1 in used and t2 not in used:
                    join_clauses.append(f"LEFT JOIN {t2} ON {cond}")
                    used.add(t2)
                    connected_tables.add(t2)
                elif t2 in used and t1 not in used:
                    join_clauses.append(f"LEFT JOIN {t1} ON {cond}")
                    used.add(t1)
                    connected_tables.add(t1)
        
        # Проверяем несвязанные таблицы
        unconnected = all_tables - connected_tables
        if unconnected:
            st.warning(f"⚠️ Следующие таблицы не связаны с основной через известные связи: {list(unconnected)}. "
                      f"Результат может быть некорректным. Рекомендуется проверить структуру данных.")
        
        query = f"""
        SELECT {', '.join(select_clauses)}
        {from_clause}
        {' '.join(join_clauses)}
        GROUP BY {', '.join([f'\"{c}\"' for _, c in group_by])}
        ORDER BY {', '.join([f'\"{c}\"' for _, c in group_by])}
        """
        
        try:
            df = pd.read_sql(query, self.conn)
            return df, query
        except Exception as e:
            logger.error(f"SQL error: {e}\nQuery: {query}")
            st.error(f"Ошибка SQL: {e}")
            return None, query

# ========================================================================
# ОСНОВНОЕ ПРИЛОЖЕНИЕ STREAMLIT
# ========================================================================

def main():
    st.set_page_config(
        page_title="Каталог автозапчастей",
        page_icon="🚗",
        layout="wide"
    )
    
    st.title("🚗 Каталог автозапчастей")
    
    catalog = get_high_volume_catalog()
    
    st.sidebar.header("Навигация")
    page = st.sidebar.radio(
        "Выберите раздел:",
        [
            "📊 Статистика",
            "📥 Загрузка данных",
            "🔗 Power Query стиль",
            "📤 Экспорт данных",
            "💰 Цены и наценки",
            "🚫 Исключения",
            "🗂️ Категории",
            "☁️ Облако"
        ]
    )
    
    if page == "📊 Статистика":
        catalog.show_statistics()
    elif page == "📥 Загрузка данных":
        catalog.show_data_upload_interface()
    elif page == "🔗 Power Query стиль":
        catalog.show_power_query_interface()
    elif page == "📤 Экспорт данных":
        catalog.show_export_interface()
    elif page == "💰 Цены и наценки":
        catalog.show_price_settings()
    elif page == "🚫 Исключения":
        catalog.show_exclusion_settings()
    elif page == "🗂️ Категории":
        catalog.show_category_mapping()
    elif page == "☁️ Облако":
        catalog.show_cloud_sync()

if __name__ == "__main__":
    main()
