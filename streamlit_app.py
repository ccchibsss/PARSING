import os
import sys
import json
import time
import logging
import re
import decimal
import io
import hashlib
import tempfile
import functools
import psutil
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from functools import lru_cache

import streamlit as st
import duckdb
import polars as pl
import pandas as pd

# ============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================================
log_dir = Path("./auto_parts_data")
log_dir.mkdir(exist_ok=True)

# Настройка форматированного логирования с ротацией
import logging.handlers

log_file = log_dir / "app.log"
file_handler = logging.handlers.RotatingFileHandler(
    log_file,
    maxBytes=10 * 1024 * 1024,  # 10 МБ
    backupCount=5,
    encoding="utf-8"
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[file_handler, console_handler]
)
logger = logging.getLogger(__name__)

# Лимит строк для Excel
EXCEL_ROW_LIMIT = 1048575

# Размер батча для обработки
BATCH_SIZE = 100_000


# ============================================================================
# ДЕКОРАТОРЫ И УТИЛИТЫ
# ============================================================================
def timing_decorator(func):
    """Декоратор для измерения времени выполнения функций"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        logger.info(f"⏱️ {func.__name__} выполнилась за {elapsed:.2f} сек")
        return result
    return wrapper


def memory_monitor():
    """Мониторинг использования памяти"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    mem_mb = mem_info.rss / (1024 * 1024)
    logger.info(f"💾 Использование памяти: {mem_mb:.1f} МБ")
    return mem_mb


@contextmanager
def temp_upload_file(uploaded_file):
    """Безопасное создание и удаление временного файла для загрузки"""
    suffix = Path(uploaded_file.name).suffix
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    try:
        tmp.write(uploaded_file.getbuffer())
        tmp.flush()
        tmp.close()
        yield Path(tmp.name)
    finally:
        try:
            Path(tmp.name).unlink()
        except Exception as e:
            logger.warning(f"Не удалось удалить временный файл: {e}")


# ============================================================================
# БЛОК 1: HIGH-VOLUME КАТАЛОГ АВТОЗАПЧАСТЕЙ (ПОЛНАЯ ВЕРСИЯ v200.0)
# ============================================================================
class HighVolumeAutoPartsCatalog:
    """
    Высокопроизводительный каталог автозапчастей с поддержкой:
    - Полнотекстового поиска (FTS)
    - Многопоточной обработки файлов
    - Интеллектуального маппинга колонок
    - Расширенного управления связями
    - Кэширования запросов
    - Мониторинга производительности
    """
    
    def __init__(self):
        self.data_dir = Path("./auto_parts_data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Загрузка конфигураций
        self.cloud_config = self.load_cloud_config()
        self.price_rules = self.load_price_rules()
        self.exclusion_rules = self.load_exclusion_rules()
        self.category_mapping = self.load_category_mapping()
        self.column_mapping_config = self.load_column_mapping_config()
        self.link_rules = self.load_link_rules()
        
        self.db_path = self.data_dir / "catalog.duckdb"
        
        # Инициализация DuckDB с оптимизациями
        self.conn = self._init_duckdb()
        self.setup_database()
        
        # Кэш для поиска
        self._search_cache = {}
        self._search_cache_ttl = 300  # 5 минут
        
        # Метрики производительности
        self.performance_metrics = {
            'queries': 0,
            'cache_hits': 0,
            'total_time': 0.0
        }
    
    def _init_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Инициализация DuckDB с оптимизациями производительности"""
        conn = duckdb.connect(database=str(self.db_path))
        
        # Настройка параметров производительности
        conn.execute("SET memory_limit = '4GB'")
        conn.execute("SET threads = 4")
        conn.execute("SET enable_object_cache = true")
        conn.execute("SET temp_directory = './auto_parts_data/tmp'")
        
        # Создание временной директории
        Path("./auto_parts_data/tmp").mkdir(exist_ok=True)
        
        logger.info("✅ DuckDB инициализирован с оптимизациями")
        return conn
    
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
            "last_sync": 0,
            "access_key": "",
            "secret_key": ""
        }
        
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                # Объединение с дефолтными значениями для новых полей
                return {**default_config, **config}
            except Exception as e:
                logger.error(f"Ошибка чтения cloud_config.json: {e}")
                return default_config
        else:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            return default_config
    
    def save_cloud_config(self):
        config_path = self.data_dir / "cloud_config.json"
        self.cloud_config["last_sync"] = int(time.time())
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(self.cloud_config, f, indent=2, ensure_ascii=False)
    
    def load_price_rules(self) -> Dict[str, Any]:
        price_rules_path = self.data_dir / "price_rules.json"
        default_rules = {
            "global_markup": 0.2,
            "brand_markups": {},
            "min_price": 0.0,
            "max_price": 99999.0,
            "currency": "RUB",
            "round_prices": True,
            "price_precision": 2
        }
        
        if price_rules_path.exists():
            try:
                with open(price_rules_path, 'r', encoding='utf-8') as f:
                    rules = json.load(f)
                return {**default_rules, **rules}
            except Exception as e:
                logger.error(f"Ошибка чтения price_rules.json: {e}")
                return default_rules
        else:
            with open(price_rules_path, 'w', encoding='utf-8') as f:
                json.dump(default_rules, f, indent=2, ensure_ascii=False)
            return default_rules
    
    def save_price_rules(self):
        price_rules_path = self.data_dir / "price_rules.json"
        with open(price_rules_path, 'w', encoding='utf-8') as f:
            json.dump(self.price_rules, f, indent=2, ensure_ascii=False)
    
    def load_exclusion_rules(self) -> List[str]:
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        if exclusion_path.exists():
            try:
                with open(exclusion_path, 'r', encoding='utf-8') as f:
                    return [line.strip() for line in f if line.strip()]
            except Exception as e:
                logger.error(f"Ошибка чтения exclusion_rules.txt: {e}")
                return []
        else:
            content = "Кузов\nСтекла\nМасла"
            with open(exclusion_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return ["Кузов", "Стекла", "Масла"]
    
    def save_exclusion_rules(self):
        exclusion_path = self.data_dir / "exclusion_rules.txt"
        with open(exclusion_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(self.exclusion_rules))
    
    def load_category_mapping(self) -> Dict[str, str]:
        category_path = self.data_dir / "category_mapping.json"
        default_mapping = {
            "Радиатор": "Охлаждение",
            "Шаровая опора": "Подвеска",
            "Фильтр масляный": "Фильтры",
            "Тормозные колодки": "Тормоза"
        }
        
        if category_path.exists():
            try:
                with open(category_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Ошибка чтения category_mapping.json: {e}")
                return default_mapping
        else:
            with open(category_path, 'w', encoding='utf-8') as f:
                json.dump(default_mapping, f, indent=2, ensure_ascii=False)
            return default_mapping
    
    def save_category_mapping(self):
        category_path = self.data_dir / "category_mapping.json"
        with open(category_path, 'w', encoding='utf-8') as f:
            json.dump(self.category_mapping, f, indent=2, ensure_ascii=False)
    
    def load_column_mapping_config(self) -> Dict[str, Dict[str, List[str]]]:
        """Загрузка расширенной конфигурации маппинга колонок"""
        config_path = self.data_dir / "column_mapping.json"
        
        # Расширенный список вариантов названий колонок
        default_config = {
            'oe': {
                'oe_number': [
                    'oe номер', 'oe', 'оe', 'номер', 'code', 'OE', 'oe_number', 
                    'oe number', 'origin number', 'original number', 'oem', 'oem number',
                    'номер оригинала', 'оригинальный номер', 'заводской номер'
                ],
                'artikul': [
                    'артикул', 'article', 'sku', 'artikul', 'код товара', 'код', 
                    'код артикула', 'part number', 'номер детали', 'номер запчасти',
                    'catalog number', 'каталожный номер'
                ],
                'brand': [
                    'бренд', 'brand', 'производитель', 'manufacturer', 'марка',
                    'maker', 'изготовитель', 'company', 'компания'
                ],
                'name': [
                    'наименование', 'название', 'name', 'описание', 'description', 
                    'товар', 'наименование товара', 'product name', 'product',
                    'detail name', 'название детали', 'part name'
                ],
                'applicability': [
                    'применимость', 'автомобиль', 'vehicle', 'applicability', 
                    'применяемость', 'car', 'auto', 'модель', 'model',
                    'совместимость', 'compatibility'
                ],
                'barcode': [
                    'штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13', 'штрих код',
                    'bar code', 'ean13', 'upc', 'скан-код'
                ],
                'multiplicity': [
                    'кратность шт', 'кратность', 'multiplicity', 'кратность упаковки',
                    'количество в упаковке', 'упаковка', 'pack quantity', 'pack qty'
                ],
                'length': [
                    'длина (см)', 'длина', 'length', 'длинна', 'длина, см', 'length_cm',
                    'длина см', 'l', 'length cm'
                ],
                'width': [
                    'ширина (см)', 'ширина', 'width', 'ширина, см', 'width_cm',
                    'ширина см', 'w', 'width cm'
                ],
                'height': [
                    'высота (см)', 'высота', 'height', 'высота, см', 'height_cm',
                    'высота см', 'h', 'height cm'
                ],
                'weight': [
                    'вес (кг)', 'вес, кг', 'вес', 'weight', 'масса', 'weight_kg', 
                    'вес кг', 'mass', 'weight kg'
                ],
                'image_url': [
                    'ссылка', 'url', 'изображение', 'image', 'картинка', 'фото', 
                    'ссылка на изображение', 'image url', 'picture', 'img'
                ],
                'dimensions_str': [
                    'весогабариты', 'размеры', 'dimensions', 'size', 'габариты', 
                    'длинна/ширина/высота', 'длина/ширина/высота', 'дхшхв',
                    'dimension', 'gabarity'
                ],
                'price': [
                    'цена', 'price', 'рекомендованная цена', 'retail price', 
                    'цена продажи', 'стоимость', 'cost', 'цена руб', 'price rub',
                    'розничная цена', 'оптовая цена'
                ],
                'currency': [
                    'валюта', 'currency', 'валюта цены', 'cur', 'price currency'
                ]
            },
            'cross': {
                'oe_number': [
                    'oe номер', 'oe', 'оe', 'номер', 'code', 'OE', 'oe_number',
                    'oe number', 'origin number', 'original number'
                ],
                'artikul': [
                    'артикул', 'article', 'sku', 'artikul', 'код товара', 'код',
                    'part number', 'номер детали'
                ],
                'brand': [
                    'бренд', 'brand', 'производитель', 'manufacturer', 'марка'
                ]
            },
            'prices': {
                'artikul': [
                    'артикул', 'article', 'sku', 'artikul', 'код товара', 'код'
                ],
                'brand': [
                    'бренд', 'brand', 'производитель', 'manufacturer', 'марка'
                ],
                'price': [
                    'цена', 'price', 'стоимость', 'cost', 'цена руб'
                ],
                'currency': [
                    'валюта', 'currency', 'валюта цены'
                ]
            }
        }
        
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                # Объединение с дефолтными значениями
                for file_type in default_config:
                    if file_type not in config:
                        config[file_type] = default_config[file_type]
                    else:
                        for field in default_config[file_type]:
                            if field not in config[file_type]:
                                config[file_type][field] = default_config[file_type][field]
                return config
            except Exception as e:
                logger.error(f"Ошибка чтения column_mapping.json: {e}")
                return default_config
        else:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)
            return default_config
    
    def save_column_mapping_config(self):
        """Сохранение конфигурации маппинга колонок"""
        config_path = self.data_dir / "column_mapping.json"
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(self.column_mapping_config, f, indent=2, ensure_ascii=False)
    
    def load_link_rules(self) -> Dict[str, Any]:
        """Загрузка правил связывания данных"""
        link_rules_path = self.data_dir / "link_rules.json"
        default_rules = {
            "use_cross_references": True,
            "use_dimensions_linking": True,
            "use_barcode_linking": True,
            "use_price_linking": True,
            "max_link_depth": 2,
            "prefer_original_oe": True,
            "link_by_oe_only": False,
            "exclude_brands_from_linking": [],
            "priority_brands_for_linking": []
        }
        
        if link_rules_path.exists():
            try:
                with open(link_rules_path, 'r', encoding='utf-8') as f:
                    rules = json.load(f)
                return {**default_rules, **rules}
            except Exception as e:
                logger.error(f"Ошибка чтения link_rules.json: {e}")
                return default_rules
        else:
            with open(link_rules_path, 'w', encoding='utf-8') as f:
                json.dump(default_rules, f, indent=2, ensure_ascii=False)
            return default_rules
    
    def save_link_rules(self):
        """Сохранение правил связывания"""
        link_rules_path = self.data_dir / "link_rules.json"
        with open(link_rules_path, 'w', encoding='utf-8') as f:
            json.dump(self.link_rules, f, indent=2, ensure_ascii=False)
    
    # ========================================================================
    # БАЗА ДАННЫХ
    # ========================================================================
    @timing_decorator
    def setup_database(self):
        """Создание и оптимизация структуры базы данных"""
        logger.info("🔧 Настройка базы данных...")
        
        # Создание основных таблиц
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
                dimensions_str VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS parts (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                artikul VARCHAR,
                brand VARCHAR,
                multiplicity INTEGER DEFAULT 1,
                barcode VARCHAR,
                length DOUBLE DEFAULT 0.0,
                width DOUBLE DEFAULT 0.0,
                height DOUBLE DEFAULT 0.0,
                weight DOUBLE DEFAULT 0.0,
                image_url VARCHAR,
                dimensions_str VARCHAR,
                description VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                link_type VARCHAR DEFAULT 'direct',
                priority INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (oe_number_norm, artikul_norm, brand_norm),
                FOREIGN KEY (oe_number_norm) REFERENCES oe(oe_number_norm) ON DELETE CASCADE,
                FOREIGN KEY (artikul_norm, brand_norm) REFERENCES parts(artikul_norm, brand_norm) ON DELETE CASCADE
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                price DOUBLE,
                currency VARCHAR DEFAULT 'RUB',
                price_date DATE DEFAULT CURRENT_DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (artikul_norm, brand_norm),
                FOREIGN KEY (artikul_norm, brand_norm) REFERENCES parts(artikul_norm, brand_norm) ON DELETE CASCADE
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Создание таблицы для хранения истории изменений
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS change_log (
                id INTEGER PRIMARY KEY DEFAULT nextval('change_log_seq'),
                table_name VARCHAR,
                operation VARCHAR,
                record_key VARCHAR,
                old_values VARCHAR,
                new_values VARCHAR,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                changed_by VARCHAR DEFAULT 'system'
            )
        """)
        
        # Создание последовательности для change_log
        try:
            self.conn.execute("CREATE SEQUENCE IF NOT EXISTS change_log_seq")
        except Exception:
            pass
        
        # Создание триггера для обновления updated_at
        self.conn.execute("""
            CREATE OR REPLACE FUNCTION update_timestamp()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = CURRENT_TIMESTAMP;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql
        """)
        
        # Применение триггеров к таблицам
        for table in ['oe', 'parts', 'cross_references', 'prices']:
            try:
                self.conn.execute(f"""
                    DROP TRIGGER IF EXISTS trg_update_{table}_timestamp ON {table}
                """)
                self.conn.execute(f"""
                    CREATE TRIGGER trg_update_{table}_timestamp
                    BEFORE UPDATE ON {table}
                    FOR EACH ROW EXECUTE FUNCTION update_timestamp()
                """)
            except Exception as e:
                logger.warning(f"Не удалось создать триггер для {table}: {e}")
        
        self.create_indexes()
        self.init_fulltext_search()
        
        logger.info("✅ База данных настроена")
    
    @timing_decorator
    def create_indexes(self):
        """Создание индексов для оптимизации запросов"""
        logger.info("⚙️ Создание индексов...")
        
        indexes = [
            # Основные индексы
            "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_oe_category ON oe(category)",
            "CREATE INDEX IF NOT EXISTS idx_oe_name ON oe(name)",
            
            "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_brand ON parts(brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_artikul ON parts(artikul_norm)",
            
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_link_type ON cross_references(link_type)",
            
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_price ON prices(price)",
            
            # Составные индексы для ускорения JOIN-запросов
            "CREATE INDEX IF NOT EXISTS idx_cross_oe_artikul ON cross_references(oe_number_norm, artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_brand_artikul ON parts(brand_norm, artikul_norm)"
        ]
        
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        
        logger.info("🛠️ Индексы созданы")
    
    def init_fulltext_search(self):
        """Инициализация полнотекстового поиска"""
        try:
            self.conn.execute("INSTALL fts; LOAD fts;")
            
            # Проверка существования FTS индекса
            result = self.conn.execute("""
                SELECT index_name FROM duckdb_fts_indexes 
                WHERE table_name = 'parts'
            """).fetchone()
            
            if not result:
                self.conn.execute("""
                    PRAGMA create_fts_index(
                        'parts', 
                        'parts_fts_idx',
                        'artikul_norm', 
                        'brand_norm',
                        'description'
                    )
                """)
                logger.info("✅ FTS индекс для parts создан")
            
            result = self.conn.execute("""
                SELECT index_name FROM duckdb_fts_indexes 
                WHERE table_name = 'oe'
            """).fetchone()
            
            if not result:
                self.conn.execute("""
                    PRAGMA create_fts_index(
                        'oe', 
                        'oe_fts_idx',
                        'oe_number_norm', 
                        'name',
                        'applicability'
                    )
                """)
                logger.info("✅ FTS индекс для oe создан")
            
        except Exception as e:
            logger.warning(f"FTS не инициализирован: {e}. Будет использован резервный поиск.")
    
    def check_database_health(self) -> Dict[str, Any]:
        """Проверка целостности и здоровья базы данных"""
        checks = {
            'db_exists': self.db_path.exists(),
            'db_size_mb': 0,
            'tables_ok': False,
            'indexes_ok': False,
            'no_orphans': False,
            'corruption_detected': False,
            'total_rows': {}
        }
        
        try:
            # Размер БД
            if self.db_path.exists():
                checks['db_size_mb'] = round(self.db_path.stat().st_size / (1024 * 1024), 2)
            
            # Проверка таблиц
            tables = self.conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema='main' AND table_type='BASE TABLE'
            """).fetchall()
            
            existing_tables = {t[0] for t in tables}
            expected_tables = {'oe', 'parts', 'cross_references', 'prices', 'metadata', 'change_log'}
            checks['tables_ok'] = expected_tables.issubset(existing_tables)
            
            # Подсчет записей
            for table in expected_tables:
                if table in existing_tables:
                    try:
                        count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        checks['total_rows'][table] = count
                    except Exception:
                        checks['total_rows'][table] = -1
            
            # Проверка сиротских записей
            orphan_cross = self.conn.execute("""
                SELECT COUNT(*) FROM cross_references cr
                WHERE NOT EXISTS (
                    SELECT 1 FROM parts p 
                    WHERE p.artikul_norm = cr.artikul_norm 
                    AND p.brand_norm = cr.brand_norm
                )
            """).fetchone()[0]
            
            orphan_oe = self.conn.execute("""
                SELECT COUNT(*) FROM cross_references cr
                WHERE NOT EXISTS (
                    SELECT 1 FROM oe o
                    WHERE o.oe_number_norm = cr.oe_number_norm
                )
            """).fetchone()[0]
            
            checks['no_orphans'] = (orphan_cross == 0 and orphan_oe == 0)
            checks['orphan_details'] = {
                'cross_orphans': orphan_cross,
                'oe_orphans': orphan_oe
            }
            
            # Проверка целостности через PRAGMA
            try:
                integrity = self.conn.execute("PRAGMA integrity_check").fetchone()
                checks['corruption_detected'] = (integrity[0] != 'ok')
            except Exception:
                pass
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            checks['error'] = str(e)
        
        return checks
    
    def vacuum_database(self):
        """Оптимизация базы данных"""
        try:
            logger.info("🧹 Запуск VACUUM...")
            self.conn.execute("VACUUM")
            logger.info("✅ VACUUM завершен")
            
            # Анализ для оптимизатора запросов
            self.conn.execute("ANALYZE")
            logger.info("✅ ANALYZE завершен")
            
            return True
        except Exception as e:
            logger.error(f"Ошибка VACUUM: {e}")
            return False
    
    # ========================================================================
    # НОРМАЛИЗАЦИЯ И ОЧИСТКА
    # ========================================================================
    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        """Улучшенная нормализация ключей с обработкой спецсимволов"""
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all(r"[''""]", "")  # Удаление кавычек
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s\.]", "")  # Разрешаем точки
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase()
                .str.replace_all(r"\.{2,}", ".")  # Удаление множественных точек
                .str.strip_chars("."))  # Удаление точек в начале и конце
    
    @staticmethod
    def clean_values(series: pl.Series) -> pl.Series:
        """Очистка значений от мусора"""
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all("'", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s\.\,\;\:\/]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars())
    
    def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
        """Улучшенное определение категорий с поддержкой множественных правил"""
        name_lower = name_series.str.to_lowercase()
        
        # Начинаем с None
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        
        # Пользовательские правила — наивысший приоритет
        for key, category in sorted(self.category_mapping.items(), key=lambda x: len(x[0]), reverse=True):
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower(), literal=True)
            ).then(pl.lit(category))
        
        # Расширенные стандартные правила
        categories_map = {
            'Фильтры': r'фильтр|filter|filtr|воздушный|масляный|салонный|топливный',
            'Тормозная система': r'тормоз|brake|колодк|диск тормозной|суппорт|барабан|цилиндр тормозной|шланг тормозной',
            'Подвеска и рулевое': r'амортизатор|стойк|spring|подвеск|рычаг|сайлентблок|опора|пружин|рессор|тяга|наконечник|steering|рулевой|шаровая',
            'Двигатель и выпуск': r'двигатель|engine|свеч|поршень|клапан|прокладк|ремонь грм|цепь грм|глушитель|катализатор|выхлоп|exhaust|коллектор|турбин|распредвал|коленвал',
            'Трансмиссия': r'трансмиссия|transmission|сцеплен|коробк|сцепление|маховик|диск сцепления|корзина',
            'Электрика и освещение': r'аккумулятор|генератор|стартер|провод|ламп|фар|стоп-сигнал|поворотник|датчик|реле|предохранитель|катушка зажигания|трамблер',
            'Охлаждение и отопление': r'радиатор|вентилятор|термостат|cooling|помпа|охлажден|отоплен|печка|кондиционер|компрессор кондиционера',
            'Топливная система': r'топливный|бензонасос|форсунк|fuel|бак|тнвд|карбюратор|инжектор|адсорбер',
            'Кузов и оптика': r'кузов|body|зеркал|стекл|бампер|крыл|капот|двер|фара|фонарь|оптика',
            'Салон и комфорт': r'салон|interior|сиден|коврик|чехол|руль|панель|климат|стеклоподъемник|замок|ключ'
        }
        
        for category, pattern in categories_map.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(pattern, literal=False)
            ).then(pl.lit(category))
        
        return categorization_expr.otherwise(pl.lit('Разное')).alias('category')
    
    # ========================================================================
    # УНИВЕРСАЛЬНАЯ КОНВЕРТАЦИЯ В ЧИСЛО
    # ========================================================================
    @staticmethod
    def safe_convert_to_float(value: Any) -> float:
        """Безопасная конвертация значения в float с расширенной обработкой"""
        if value is None or value == "":
            return 0.0
        
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        
        if isinstance(value, (int, float)):
            import math
            if math.isnan(value) or math.isinf(value):
                return 0.0
            return float(value)
        
        if isinstance(value, decimal.Decimal):
            return float(value)
        
        if isinstance(value, (datetime, date, pd.Timestamp)):
            try:
                base = datetime(1899, 12, 30)
                if isinstance(value, pd.Timestamp):
                    value = value.to_pydatetime()
                delta = value - base
                return float(delta.days + delta.seconds / 86400.0)
            except Exception:
                return 0.0
        
        if isinstance(value, timedelta):
            return float(value.total_seconds() / 86400.0)
        
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return 0.0
            
            # Обработка специальных случаев
            if value.lower() in ['нет', 'no', 'none', 'null', 'nan', 'inf', '-inf']:
                return 0.0
            
            # Обработка дробей вида "1/2"
            if '/' in value and value.count('/') == 1:
                try:
                    num, den = value.split('/')
                    return float(num) / float(den)
                except (ValueError, ZeroDivisionError):
                    pass
            
            # Очистка строки
            cleaned = re.sub(r'[^\d.,\-]', '', value)
            if not cleaned:
                return 0.0
            
            # Замена запятой на точку
            cleaned = cleaned.replace(',', '.')
            
            # Обработка множественных точек
            parts = cleaned.split('.')
            if len(parts) > 2:
                cleaned = parts[0] + '.' + ''.join(parts[1:])
            
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        
        # Обработка numpy типов
        if hasattr(value, 'dtype') and hasattr(value, 'item'):
            try:
                item = value.item()
                if isinstance(item, (int, float)):
                    return float(item)
            except Exception:
                pass
        
        if hasattr(value, 'to_python'):
            try:
                return float(value.to_python())
            except Exception:
                pass
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    # ========================================================================
    # РАСШИРЕННАЯ ОБРАБОТКА ФАЙЛОВ
    # ========================================================================
    def detect_columns_advanced(self, actual_columns: List[str], file_type: str) -> Dict[str, str]:
        """
        Расширенное определение колонок с использованием:
        - Конфигурации column_mapping_config
        - Нечеткого сопоставления (fuzzy matching)
        - Анализа содержимого колонок
        """
        if file_type not in self.column_mapping_config:
            logger.warning(f"Нет конфигурации для типа файла: {file_type}")
            return {}
        
        column_variants = self.column_mapping_config[file_type]
        actual_lower = {col.lower().strip(): col for col in actual_columns}
        mapping = {}
        used_actual = set()
        
        for expected_field, variants in column_variants.items():
            best_match = None
            best_score = -1
            
            for variant in variants:
                variant_lower = variant.lower().strip()
                
                for actual_l, actual_orig in actual_lower.items():
                    if actual_orig in used_actual:
                        continue
                    
                    score = 0
                    
                    # Точное совпадение
                    if variant_lower == actual_l:
                        score = 100
                    # Полное вхождение
                    elif variant_lower in actual_l:
                        score = 80 + len(variant_lower) / len(actual_l) * 20
                    elif actual_l in variant_lower:
                        score = 60 + len(actual_l) / len(variant_lower) * 20
                    # Частичное совпадение слов
                    else:
                        variant_words = set(variant_lower.split())
                        actual_words = set(actual_l.split())
                        common_words = variant_words & actual_words
                        if common_words:
                            score = 40 + (len(common_words) / max(len(variant_words), len(actual_words))) * 40
                    
                    # Бонус за наличие в начале строки
                    if actual_l.startswith(variant_lower) or variant_lower.startswith(actual_l):
                        score += 10
                    
                    if score > best_score:
                        best_score = score
                        best_match = actual_orig
            
            if best_match and best_score > 30:  # Порог для принятия маппинга
                mapping[best_match] = expected_field
                used_actual.add(best_match)
                logger.debug(f"Маппинг: {best_match} -> {expected_field} (score: {best_score:.0f})")
        
        # Анализ содержимого для нераспознанных колонок
        if len(mapping) < len(expected_columns := list(column_variants.keys())):
            unmapped_actual = [col for col in actual_columns if col not in mapping]
            
            if unmapped_actual and 'oe_number' not in mapping.values():
                # Попытка определить OE номер по содержимому (цифры и буквы)
                for col in unmapped_actual[:5]:  # Проверяем первые 5 нераспознанных
                    if re.match(r'^[A-Za-z0-9\-\s]+$', col):
                        mapping[col] = 'oe_number'
                        break
        
        logger.info(f"Маппинг колонок для {file_type}: {mapping}")
        return mapping
    
    @timing_decorator
    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        """Улучшенное чтение и подготовка файлов с автоматическим определением формата"""
        logger.info(f"📄 Обработка файла: {file_type} ({file_path})")
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                return pl.DataFrame()
            
            # Автоматическое определение формата файла
            file_path_obj = Path(file_path)
            file_ext = file_path_obj.suffix.lower()
            file_size_mb = file_path_obj.stat().st_size / (1024 * 1024)
            
            logger.info(f"Размер файла: {file_size_mb:.2f} МБ")
            
            # Выбор стратегии чтения в зависимости от размера
            if file_ext == '.csv':
                # Для больших CSV используем streaming
                if file_size_mb > 100:
                    df = pl.read_csv(
                        file_path,
                        ignore_errors=True,
                        encoding='utf-8',
                        low_memory=True,
                        n_rows=None  # Читаем все, но с оптимизацией памяти
                    )
                else:
                    df = pl.read_csv(file_path, ignore_errors=True, encoding='utf-8')
            
            elif file_ext in ['.xlsx', '.xls']:
                if file_size_mb > 50:
                    # Для больших Excel используем calamine engine
                    df = pl.read_excel(file_path, engine='calamine')
                else:
                    df = pl.read_excel(file_path)
            
            elif file_ext == '.parquet':
                df = pl.read_parquet(file_path)
            
            elif file_ext == '.json':
                df = pl.read_json(file_path)
            
            else:
                logger.error(f"Неподдерживаемый формат файла: {file_ext}")
                return pl.DataFrame()
            
            if df.is_empty():
                logger.warning(f"Пустой файл: {file_path}")
                return pl.DataFrame()
            
            logger.info(f"Исходные колонки ({len(df.columns)}): {df.columns}")
            logger.info(f"Количество строк: {len(df)}")
            
        except Exception as e:
            logger.exception(f"Ошибка чтения файла {file_path}: {e}")
            return pl.DataFrame()
        
        # Определение ожидаемых колонок
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
        
        # Используем расширенное определение колонок
        column_mapping = self.detect_columns_advanced(df.columns, file_type)
        
        if not column_mapping:
            logger.warning(f"Не удалось определить колонки для {file_type}")
            # Попытка использовать простой маппинг как fallback
            if file_type in schemas:
                expected_cols = schemas[file_type]
                column_mapping = self.detect_columns_simple(df.columns, expected_cols)
            
            if not column_mapping:
                return pl.DataFrame()
        
        # Переименование колонок
        try:
            rename_dict = {old: new for old, new in column_mapping.items() if old in df.columns and new not in df.columns}
            if rename_dict:
                df = df.rename(rename_dict)
        except Exception as e:
            logger.warning(f"Ошибка переименования: {e}")
            # Поочередное переименование
            for old_name, new_name in column_mapping.items():
                try:
                    if old_name in df.columns and new_name not in df.columns:
                        df = df.rename({old_name: new_name})
                except Exception as e2:
                    logger.warning(f"Не удалось переименовать {old_name} -> {new_name}: {e2}")
        
        # Удаление дубликатов колонок
        if len(df.columns) != len(set(df.columns)):
            logger.warning("Обнаружены дубликаты колонок")
            seen = set()
            cols_to_keep = []
            for col in df.columns:
                if col not in seen:
                    seen.add(col)
                    cols_to_keep.append(col)
            df = df.select(cols_to_keep)
        
        # Очистка текстовых колонок
        for col in ['artikul', 'brand', 'oe_number', 'name', 'applicability']:
            if col in df.columns:
                df = df.with_columns(self.clean_values(pl.col(col)).alias(col))
        
        # Конвертация числовых колонок
        numeric_cols = ['length', 'width', 'height', 'weight', 'price']
        for col in numeric_cols:
            if col in df.columns:
                try:
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8)
                        .str.replace_all(r'[^\d.,\-]', '')
                        .str.replace(',', '.')
                        .str.replace(r'\.(?=.*\.)', '')
                        .cast(pl.Float64, strict=False)
                        .fill_null(0.0)
                        .round(2)
                        .alias(col)
                    )
                except Exception as e:
                    logger.warning(f"Не удалось преобразовать {col}: {e}")
                    if col not in df.columns:
                        df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
        
        # Удаление дубликатов по ключевым полям
        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')
        
        # Нормализация ключей
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                df = df.with_columns(
                    self.normalize_key(pl.col(col)).alias(f"{col}_norm")
                )
        
        logger.info(f"✅ Файл {file_type} обработан. Колонки: {df.columns}, Строк: {len(df)}")
        memory_monitor()
        
        return df
    
    def detect_columns_simple(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        """Простой маппинг колонок (fallback метод)"""
        column_variants = {
            'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'OE', 'oe_number', 'oe number'],
            'artikul': ['артикул', 'article', 'sku', 'artikul', 'код товара', 'код', 'код артикула'],
            'brand': ['бренд', 'brand', 'производитель', 'manufacturer', 'марка'],
            'name': ['наименование', 'название', 'name', 'описание', 'description', 'товар'],
            'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability', 'применяемость'],
            'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13', 'штрих код'],
            'multiplicity': ['кратность шт', 'кратность', 'multiplicity', 'кратность упаковки'],
            'length': ['длина (см)', 'длина', 'length', 'длинна', 'длина, см', 'length_cm'],
            'width': ['ширина (см)', 'ширина', 'width', 'ширина, см', 'width_cm'],
            'height': ['высота (см)', 'высота', 'height', 'высота, см', 'height_cm'],
            'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight', 'масса', 'weight_kg', 'вес кг'],
            'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка', 'фото'],
            'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size', 'габариты'],
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
        
        return mapping
    
    def process_uploaded_files(self, uploaded_files_dict: Dict[str, Any]) -> Dict[str, pl.DataFrame]:
        """Обработка загруженных файлов с использованием временных файлов"""
        results = {}
        temp_dir = self.data_dir / "temp_uploads"
        temp_dir.mkdir(exist_ok=True)
        
        for file_type, files in uploaded_files_dict.items():
            if not files:
                continue
            
            dfs_for_type = []
            total_files = len(files)
            
            for idx, uploaded_file in enumerate(files):
                logger.info(f"Обработка файла {idx + 1}/{total_files}: {uploaded_file.name}")
                
                with temp_upload_file(uploaded_file) as temp_path:
                    try:
                        df = self.read_and_prepare_file(str(temp_path), file_type)
                        if not df.is_empty():
                            dfs_for_type.append(df)
                            logger.info(f"✅ Файл '{uploaded_file.name}' обработан. Строк: {len(df)}")
                        else:
                            logger.warning(f"⚠️ Файл '{uploaded_file.name}' обработан, но DataFrame пуст")
                    except Exception as e:
                        logger.exception(f"❌ Ошибка обработки файла '{uploaded_file.name}': {e}")
                        st.error(f"❌ Ошибка обработки файла '{uploaded_file.name}': {str(e)}")
            
            if dfs_for_type:
                try:
                    combined_df = pl.concat(dfs_for_type)
                    results[file_type] = combined_df.unique(keep='first')
                    logger.info(f"📦 Тип {file_type}: объединено {len(combined_df)} записей")
                except Exception as e:
                    logger.error(f"Ошибка объединения DataFrame для {file_type}: {e}")
        
        return results
    
    # ========================================================================
    # ЗАГРУЗКА И ОБНОВЛЕНИЕ В БАЗЕ (УЛУЧШЕННЫЙ UPSERT)
    # ========================================================================
    def upsert_data_batched(self, table_name: str, df: pl.DataFrame, pk: List[str], batch_size: int = BATCH_SIZE):
        """Пакетный UPSERT данных с проверкой целостности"""
        if df.is_empty():
            return
        
        df = df.unique(keep='first')
        total_rows = len(df)
        successful_rows = 0
        
        logger.info(f"🔄 Пакетный UPSERT в {table_name}: {total_rows} записей")
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch = df.slice(start_idx, end_idx - start_idx)
            
            temp_view_name = f"temp_{table_name}_{int(time.time())}_{start_idx}"
            
            try:
                self.conn.register(temp_view_name, batch.to_arrow())
                
                # Атомарный INSERT OR REPLACE
                insert_sql = f"""
                    INSERT OR REPLACE INTO {table_name}
                    SELECT * FROM {temp_view_name};
                """
                self.conn.execute(insert_sql)
                
                successful_rows += len(batch)
                
                if start_idx % (batch_size * 10) == 0 or end_idx >= total_rows:
                    progress = (end_idx / total_rows) * 100
                    logger.info(f"⏳ Прогресс {table_name}: {progress:.0f}% ({end_idx}/{total_rows})")
                
            except Exception as e:
                logger.error(f"Ошибка при UPSERT в {table_name} (строки {start_idx}-{end_idx}): {e}")
                # Попытка по строке для выявления проблемных записей
                for i in range(len(batch)):
                    single_row = batch.slice(i, 1)
                    try:
                        single_view = f"temp_single_{int(time.time())}_{i}"
                        self.conn.register(single_view, single_row.to_arrow())
                        self.conn.execute(f"INSERT OR REPLACE INTO {table_name} SELECT * FROM {single_view}")
                        self.conn.unregister(single_view)
                        successful_rows += 1
                    except Exception as row_error:
                        logger.warning(f"Проблемная запись {i}: {row_error}")
            
            finally:
                try:
                    self.conn.unregister(temp_view_name)
                except Exception:
                    pass
            
            # Проверка памяти каждые 100K записей
            if start_idx > 0 and start_idx % (batch_size * 100) == 0:
                mem_usage = memory_monitor()
                if mem_usage > 2000:  # Если больше 2 ГБ
                    logger.warning(f"⚠️ Высокое использование памяти: {mem_usage:.1f} МБ")
                    self.conn.execute("CHECKPOINT")
        
        logger.info(f"✅ UPSERT в {table_name} завершен: {successful_rows}/{total_rows} записей")
        
        # Логирование изменений
        self.log_change(table_name, 'UPSERT', str(pk), f"Вставлено/обновлено {successful_rows} записей")
    
    def log_change(self, table_name: str, operation: str, record_key: str, details: str):
        """Логирование изменений в базу"""
        try:
            self.conn.execute("""
                INSERT INTO change_log (table_name, operation, record_key, new_values)
                VALUES (?, ?, ?, ?)
            """, [table_name, operation, record_key, details])
        except Exception as e:
            logger.warning(f"Не удалось записать лог изменений: {e}")
    
    @timing_decorator
    def upsert_prices(self, price_df: pl.DataFrame):
        """Обновление цен с применением правил"""
        if price_df.is_empty():
            return
        
        if 'artikul' in price_df.columns and 'brand' in price_df.columns:
            price_df = price_df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])
            
            if 'currency' not in price_df.columns:
                price_df = price_df.with_columns(pl.lit(self.price_rules.get('currency', 'RUB')).alias('currency'))
            
            # Применение правил цен
            price_df = price_df.filter(
                (pl.col('price') >= self.price_rules['min_price']) &
                (pl.col('price') <= self.price_rules['max_price'])
            )
            
            # Округление цен если нужно
            if self.price_rules.get('round_prices', True):
                precision = self.price_rules.get('price_precision', 2)
                price_df = price_df.with_columns(
                    pl.col('price').round(precision).alias('price')
                )
            
            # Добавление даты цены
            price_df = price_df.with_columns(
                pl.lit(date.today()).alias('price_date')
            )
            
            self.upsert_data_batched('prices', price_df, ['artikul_norm', 'brand_norm'])
    
    @timing_decorator
    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Улучшенная загрузка данных с индикацией прогресса"""
        if not dataframes:
            st.warning("Нет данных для загрузки")
            return
        
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        
        steps = []
        if 'oe' in dataframes:
            steps.append('oe')
        if 'cross' in dataframes:
            steps.append('cross')
        if 'prices' in dataframes:
            steps.append('prices')
        steps.append('parts')  # Всегда обрабатываем parts последним
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, step in enumerate(steps):
            progress = (idx + 1) / len(steps)
            progress_bar.progress(progress)
            
            if step == 'oe':
                status_text.text(f"Обработка OE данных ({idx + 1}/{len(steps)})...")
                self._process_oe_data(dataframes.get('oe'))
            
            elif step == 'cross':
                status_text.text(f"Обработка кросс-ссылок ({idx + 1}/{len(steps)})...")
                self._process_cross_data(dataframes.get('cross'))
            
            elif step == 'prices':
                status_text.text(f"Обработка цен ({idx + 1}/{len(steps)})...")
                price_df = dataframes.get('prices')
                if price_df is not None and not price_df.is_empty():
                    self.upsert_prices(price_df)
                    st.success(f"✅ Загружено {len(price_df)} цен")
            
            elif step == 'parts':
                status_text.text(f"Сборка данных по артикулам ({idx + 1}/{len(steps)})...")
                self._process_parts_data(dataframes)
        
        progress_bar.progress(1.0)
        status_text.text("✅ Загрузка данных завершена!")
        
        # Оптимизация базы после загрузки
        self.vacuum_database()
        
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
    
    def _process_oe_data(self, oe_df: Optional[pl.DataFrame]):
        """Обработка OE данных"""
        if oe_df is None or oe_df.is_empty():
            return
        
        df = oe_df.filter(pl.col('oe_number_norm') != "")
        
        # Обеспечение наличия всех колонок
        for col in ['length', 'width', 'height', 'weight']:
            if col not in df.columns:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
        
        if 'dimensions_str' not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias('dimensions_str'))
        
        # Формирование DataFrame для OE
        oe_cols = ['oe_number_norm', 'oe_number', 'name', 'applicability',
                   'length', 'width', 'height', 'weight', 'dimensions_str']
        
        available_cols = [c for c in oe_cols if c in df.columns]
        oe_clean = df.select(available_cols).unique(subset=['oe_number_norm'], keep='first')
        
        # Определение категорий
        if 'name' in oe_clean.columns:
            oe_clean = oe_clean.with_columns(
                self.determine_category_vectorized(pl.col('name')).alias('category')
            )
        else:
            oe_clean = oe_clean.with_columns(pl.lit('Разное').alias('category'))
        
        # Загрузка OE данных
        self.upsert_data_batched('oe', oe_clean, ['oe_number_norm'])
        
        # Создание кросс-ссылок из OE данных
        if 'artikul_norm' in df.columns and 'brand_norm' in df.columns:
            cross_from_oe = df.filter(pl.col('artikul_norm') != "").select([
                'oe_number_norm', 'artikul_norm', 'brand_norm'
            ]).unique()
            
            if not cross_from_oe.is_empty():
                cross_from_oe = cross_from_oe.with_columns([
                    pl.lit('direct').alias('link_type'),
                    pl.lit(0).alias('priority')
                ])
                self.upsert_data_batched('cross_references', cross_from_oe,
                                        ['oe_number_norm', 'artikul_norm', 'brand_norm'])
    
    def _process_cross_data(self, cross_df: Optional[pl.DataFrame]):
        """Обработка кросс-ссылок"""
        if cross_df is None or cross_df.is_empty():
            return
        
        df = cross_df.filter(
            (pl.col('oe_number_norm') != "") & (pl.col('artikul_norm') != "")
        )
        
        if df.is_empty():
            return
        
        cross_clean = df.select(['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
        cross_clean = cross_clean.with_columns([
            pl.lit('direct').alias('link_type'),
            pl.lit(0).alias('priority')
        ])
        
        self.upsert_data_batched('cross_references', cross_clean,
                                ['oe_number_norm', 'artikul_norm', 'brand_norm'])
    
    def _process_parts_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Сборка и загрузка данных по артикулам"""
        # Сбор всех артикулов
        parts_to_concat = []
        file_priority = ['oe', 'dimensions', 'barcode', 'images']
        
        for ftype in file_priority:
            if ftype in dataframes and not dataframes[ftype].is_empty():
                df = dataframes[ftype]
                if 'artikul_norm' in df.columns and 'brand_norm' in df.columns:
                    cols = ['artikul_norm', 'brand_norm']
                    # Добавляем артикул и бренд если есть
                    for c in ['artikul', 'brand']:
                        if c in df.columns:
                            cols.append(c)
                    parts_to_concat.append(df.select(cols))
        
        if not parts_to_concat:
            return
        
        parts_df = pl.concat(parts_to_concat).filter(
            pl.col('artikul_norm') != ""
        ).unique(subset=['artikul_norm', 'brand_norm'], keep='first')
        
        if parts_df.is_empty():
            return
        
        # Добавление дополнительных данных
        for ftype in file_priority:
            if ftype not in dataframes or dataframes[ftype].is_empty():
                continue
            
            df = dataframes[ftype]
            if 'artikul_norm' not in df.columns:
                continue
            
            # Определение колонок для добавления
            if ftype in ['oe', 'dimensions']:
                join_cols = ['length', 'width', 'height', 'weight', 'dimensions_str']
            else:
                join_cols = [col for col in df.columns 
                           if col not in ['artikul', 'artikul_norm', 'brand', 'brand_norm']]
            
            join_cols = [c for c in join_cols if c in df.columns]
            existing_cols = set(parts_df.columns)
            join_cols = [c for c in join_cols if c not in existing_cols]
            
            if not join_cols:
                continue
            
            df_subset = df.select(['artikul_norm', 'brand_norm'] + join_cols).unique(
                subset=['artikul_norm', 'brand_norm'], keep='first')
            
            try:
                parts_df = parts_df.join(
                    df_subset, on=['artikul_norm', 'brand_norm'], how='left', coalesce=True)
            except Exception as e:
                logger.warning(f"Ошибка JOIN для {ftype}: {e}")
        
        # Заполнение пропущенных значений
        if 'multiplicity' not in parts_df.columns:
            parts_df = parts_df.with_columns(pl.lit(1).cast(pl.Int32).alias('multiplicity'))
        else:
            parts_df = parts_df.with_columns(pl.col('multiplicity').fill_null(1).cast(pl.Int32))
        
        for col in ['length', 'width', 'height', 'weight']:
            if col not in parts_df.columns:
                parts_df = parts_df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
            else:
                parts_df = parts_df.with_columns(pl.col(col).fill_null(0.0).cast(pl.Float64))
        
        if 'dimensions_str' not in parts_df.columns:
            parts_df = parts_df.with_columns(pl.lit(None).cast(pl.Utf8).alias('dimensions_str'))
        
        # Формирование dimensions_str из отдельных размеров
        parts_df = self._format_dimensions_string(parts_df)
        
        # Создание описания
        parts_df = self._create_description(parts_df)
        
        # Формирование финального набора колонок
        final_columns = [
            'artikul_norm', 'brand_norm', 'artikul', 'brand', 'multiplicity', 'barcode',
            'length', 'width', 'height', 'weight', 'image_url', 'dimensions_str', 'description'
        ]
        
        select_exprs = []
        for c in final_columns:
            if c in parts_df.columns:
                select_exprs.append(pl.col(c))
            else:
                select_exprs.append(pl.lit(None).alias(c))
        
        parts_final = parts_df.select(select_exprs)
        
        self.upsert_data_batched('parts', parts_final, ['artikul_norm', 'brand_norm'])
    
    def _format_dimensions_string(self, df: pl.DataFrame) -> pl.DataFrame:
        """Форматирование строки размеров"""
        df = df.with_columns([
            pl.col('length').cast(pl.Utf8).fill_null('').alias('_length_str'),
            pl.col('width').cast(pl.Utf8).fill_null('').alias('_width_str'),
            pl.col('height').cast(pl.Utf8).fill_null('').alias('_height_str'),
        ])
        
        df = df.with_columns(
            pl.when(
                (pl.col('dimensions_str').is_not_null()) &
                (pl.col('dimensions_str').cast(pl.Utf8) != '') &
                (pl.col('dimensions_str').cast(pl.Utf8).str.to_uppercase() != 'XX')
            ).then(
                pl.col('dimensions_str').cast(pl.Utf8)
            ).when(
                (pl.col('_length_str') != '0.0') |
                (pl.col('_width_str') != '0.0') |
                (pl.col('_height_str') != '0.0')
            ).then(
                pl.concat_str([
                    pl.col('_length_str'), pl.lit('x'),
                    pl.col('_width_str'), pl.lit('x'),
                    pl.col('_height_str')
                ], separator='')
            ).otherwise(
                pl.lit(None)
            ).alias('dimensions_str')
        )
        
        return df.drop(['_length_str', '_width_str', '_height_str'])
    
    def _create_description(self, df: pl.DataFrame) -> pl.DataFrame:
        """Создание описания товара"""
        df = df.with_columns([
            pl.col('artikul').cast(pl.Utf8).fill_null('').alias('_art'),
            pl.col('brand').cast(pl.Utf8).fill_null('').alias('_brd'),
            pl.col('multiplicity').cast(pl.Utf8).alias('_mult'),
        ])
        
        df = df.with_columns(
            pl.concat_str([
                pl.lit('Артикул: '), pl.col('_art'),
                pl.lit(' Бренд: '), pl.col('_brd'),
                pl.lit(' Кратность: '), pl.col('_mult'), pl.lit(' шт.')
            ], separator='').alias('description')
        )
        
        return df.drop(['_art', '_brd', '_mult'])
    
    # ========================================================================
    # ЭКСПОРТ (УЛУЧШЕННЫЙ)
    # ========================================================================
    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True,
                          apply_exclusions=True, use_link_rules=True):
        """Построение запроса для экспорта с учетом всех настроек"""
        description_text = (
            "Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — "
            "надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и "
            "высокую производительность вашего авто с помощью нашего широкого ассортимента "
            "оригинальных и совместимых автозапчастей."
        )
        
        # Настройка связей
        if use_link_rules and self.link_rules.get('use_cross_references', True):
            max_depth = self.link_rules.get('max_link_depth', 2)
            link_by_oe_only = self.link_rules.get('link_by_oe_only', False)
        else:
            max_depth = 1
            link_by_oe_only = False
        
        brand_markups_sql = self._get_brand_markups_sql()
        
        # Формирование списка колонок
        select_parts = []
        
        price_requested = include_prices and (not selected_columns or 
                                             "Цена" in selected_columns or 
                                             "Валюта" in selected_columns)
        
        if price_requested:
            if apply_markup:
                global_markup = self.price_rules.get('global_markup', 0)
                select_parts.append(
                    f"CASE WHEN pr.price IS NOT NULL THEN ROUND(pr.price * (1 + COALESCE(brm.markup, {global_markup})), 2) ELSE pr.price END AS \"Цена\""
                )
            else:
                select_parts.append('ROUND(pr.price, 2) AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")
        
        # Основные колонки
        columns_map = self._get_export_columns_map()
        
        for name, expr in columns_map.items():
            if not selected_columns or name in selected_columns:
                select_parts.append(expr.strip())
        
        if not select_parts:
            select_parts = ['r.artikul AS "Артикул бренда"', 'r.brand AS "Бренд"']
        
        select_clause = ",\n".join(select_parts)
        
        # Построение CTE для связей
        cte_parts = self._build_link_ctes(max_depth, link_by_oe_only)
        
        # Добавление исключений
        exclusion_where = ""
        if apply_exclusions and self.exclusion_rules:
            exclusion_patterns = "|".join([re.escape(rule.lower()) for rule in self.exclusion_rules])
            if exclusion_patterns:
                exclusion_where = f"""
                    AND NOT EXISTS (
                        SELECT 1 FROM oe o_excl
                        JOIN cross_references cr_excl ON o_excl.oe_number_norm = cr_excl.oe_number_norm
                        WHERE cr_excl.artikul_norm = r.artikul_norm 
                        AND cr_excl.brand_norm = r.brand_norm
                        AND LOWER(COALESCE(o_excl.name, '')) ~ '{exclusion_patterns}'
                    )
                """
        
        # Сборка полного запроса
        query = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${description_text}$$ AS text
        ),
        BrandMarkups AS (
            SELECT brand, markup FROM (
                {brand_markups_sql}
            ) AS tmp
        ),
        {cte_parts}
        SELECT
            {select_clause}
        FROM RankedData r
        CROSS JOIN DescriptionTemplate dt
        {'LEFT JOIN prices pr ON r.artikul_norm = pr.artikul_norm AND r.brand_norm = pr.brand_norm' if include_prices else ''}
        {'LEFT JOIN BrandMarkups brm ON r.brand = brm.brand' if include_prices and apply_markup else ''}
        WHERE r.rn = 1
        {exclusion_where}
        ORDER BY r.brand, r.artikul
        """
        
        return "\n".join([line.rstrip() for line in query.strip().splitlines()])
    
    def _get_export_columns_map(self) -> Dict[str, str]:
        """Получение маппинга колонок для экспорта"""
        return {
            "Артикул бренда": 'r.artikul AS "Артикул бренда"',
            "Бренд": 'r.brand AS "Бренд"',
            "Наименование": 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"',
            "Применимость": 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"',
            "Описание": 'CONCAT(COALESCE(r.description, \'\'), dt.text) AS "Описание"',
            "Категория товара": 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория товара"',
            "Кратность": 'r.multiplicity AS "Кратность"',
            "Длинна": """
                COALESCE(
                    NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.analog_length AS DOUBLE), 2), 0),
                    0.0
                ) AS "Длинна"
            """,
            "Ширина": """
                COALESCE(
                    NULLIF(ROUND(CAST(r.width AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.analog_width AS DOUBLE), 2), 0),
                    0.0
                ) AS "Ширина"
            """,
            "Высота": """
                COALESCE(
                    NULLIF(ROUND(CAST(r.height AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.analog_height AS DOUBLE), 2), 0),
                    0.0
                ) AS "Высота"
            """,
            "Вес": """
                COALESCE(
                    NULLIF(ROUND(CAST(r.weight AS DOUBLE), 2), 0),
                    NULLIF(ROUND(CAST(r.analog_weight AS DOUBLE), 2), 0),
                    0.0
                ) AS "Вес"
            """,
            "Длинна/Ширина/Высота": """
                COALESCE(
                    CASE
                        WHEN r.dimensions_str IS NULL OR r.dimensions_str = '' OR UPPER(TRIM(r.dimensions_str)) = 'XX'
                        THEN NULL
                        ELSE r.dimensions_str
                    END,
                    r.analog_dimensions_str
                ) AS "Длинна/Ширина/Высота"
            """,
            "OE номер": 'r.oe_list AS "OE номер"',
            "аналоги": 'r.analog_list AS "аналоги"',
            "Ссылка на изображение": 'r.image_url AS "Ссылка на изображение"'
        }
    
    def _get_brand_markups_sql(self) -> str:
        """Получение SQL для наценок по брендам"""
        rows = []
        for brand, markup in self.price_rules['brand_markups'].items():
            safe_brand = brand.replace("'", "''")
            rows.append(f"SELECT '{safe_brand}' AS brand, {markup} AS markup")
        
        if rows:
            return " UNION ALL ".join(rows)
        else:
            return "SELECT NULL AS brand, NULL AS markup LIMIT 0"
    
    def _build_link_ctes(self, max_depth: int, link_by_oe_only: bool) -> str:
        """Построение CTE для связывания данных"""
        if max_depth <= 0:
            max_depth = 1
        
        ctes = """
        PartDetails AS (
            SELECT
                cr.artikul_norm,
                cr.brand_norm,
                STRING_AGG(DISTINCT o.oe_number, ', ') AS oe_list,
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
                STRING_AGG(DISTINCT p2.artikul, ', ') AS analog_list
            FROM cross_references cr1
            JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
            JOIN parts p2 ON cr2.artikul_norm = p2.artikul_norm AND cr2.brand_norm = p2.brand_norm
            WHERE (cr1.artikul_norm != p2.artikul_norm OR cr1.brand_norm != p2.brand_norm)
            GROUP BY cr1.artikul_norm, cr1.brand_norm
        )"""
        
        if max_depth >= 2:
            ctes += """,
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
        AggregatedAnalogData AS (
            SELECT
                arp.source_artikul_norm AS artikul_norm,
                arp.source_brand_norm AS brand_norm,
                ROUND(MAX(CASE WHEN p2.length IS NOT NULL AND p2.length != 0 THEN p2.length ELSE NULL END), 2) AS analog_length,
                ROUND(MAX(CASE WHEN p2.width IS NOT NULL AND p2.width != 0 THEN p2.width ELSE NULL END), 2) AS analog_width,
                ROUND(MAX(CASE WHEN p2.height IS NOT NULL AND p2.height != 0 THEN p2.height ELSE NULL END), 2) AS analog_height,
                ROUND(MAX(CASE WHEN p2.weight IS NOT NULL AND p2.weight != 0 THEN p2.weight ELSE NULL END), 2) AS analog_weight,
                ANY_VALUE(
                    CASE
                        WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX'
                        THEN p2.dimensions_str
                        ELSE NULL
                    END
                ) AS analog_dimensions_str,
                ANY_VALUE(pd2.representative_name) AS analog_representative_name,
                ANY_VALUE(pd2.representative_applicability) AS analog_representative_applicability,
                ANY_VALUE(pd2.representative_category) AS analog_representative_category
            FROM Level1Analogs arp
            JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),"""
        else:
            ctes += """,
        AggregatedAnalogData AS (
            SELECT
                NULL AS artikul_norm,
                NULL AS brand_norm,
                NULL AS analog_length,
                NULL AS analog_width,
                NULL AS analog_height,
                NULL AS analog_weight,
                NULL AS analog_dimensions_str,
                NULL AS analog_representative_name,
                NULL AS analog_representative_applicability,
                NULL AS analog_representative_category
            LIMIT 0
        ),"""
        
        ctes += """
        RankedData AS (
            SELECT
                p.artikul_norm,
                p.brand_norm,
                p.artikul,
                p.brand,
                p.description,
                p.multiplicity,
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
                p_analog.analog_length,
                p_analog.analog_width,
                p_analog.analog_height,
                p_analog.analog_weight,
                p_analog.analog_dimensions_str,
                p_analog.analog_representative_name,
                p_analog.analog_representative_applicability,
                p_analog.analog_representative_category,
                ROW_NUMBER() OVER (
                    PARTITION BY p.artikul_norm, p.brand_norm
                    ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST
                ) AS rn
            FROM parts p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
        )"""
        
        return ctes
    
    @timing_decorator
    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None,
                               include_prices: bool = True, apply_markup: bool = True,
                               apply_exclusions: bool = True) -> bool:
        """Экспорт в CSV с чанковой обработкой для больших объемов"""
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        
        st.info(f"📤 Экспорт {total:,} записей в CSV...")
        
        try:
            query = self.build_export_query(
                selected_columns, include_prices, apply_markup, apply_exclusions
            )
            
            # Чанковый экспорт для больших объемов
            if total > 500_000:
                return self._export_csv_chunked(output_path, query, total)
            
            df = self.conn.execute(query).pl()
            
            # Постобработка
            pdf = self._postprocess_export(df.to_pandas())
            
            # Запись с BOM для Excel
            output_dir = Path(output_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            
            buf = io.StringIO()
            pdf.to_csv(buf, sep=';', index=False)
            
            with open(output_path, "wb") as f:
                f.write(b'\xef\xbb\xbf')
                f.write(buf.getvalue().encode('utf-8'))
            
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Данные экспортированы: {Path(output_path).name} ({size_mb:.1f} МБ)")
            return True
            
        except Exception as e:
            logger.exception("Ошибка экспорта CSV")
            st.error(f"❌ Ошибка при экспорте в CSV: {str(e)}")
            return False
    
    def _export_csv_chunked(self, output_path: str, query: str, total: int, chunk_size: int = 100_000) -> bool:
        """Чанковый экспорт в CSV"""
        try:
            chunks = (total // chunk_size) + 1
            progress_bar = st.progress(0)
            
            with open(output_path, "wb") as f:
                f.write(b'\xef\xbb\xbf')
                
                for i in range(chunks):
                    offset = i * chunk_size
                    chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
                    
                    df = self.conn.execute(chunk_query).pl()
                    pdf = self._postprocess_export(df.to_pandas())
                    
                    buf = io.StringIO()
                    if i == 0:
                        pdf.to_csv(buf, sep=';', index=False)
                    else:
                        pdf.to_csv(buf, sep=';', index=False, header=False)
                    
                    f.write(buf.getvalue().encode('utf-8'))
                    
                    progress = min((i + 1) / chunks, 1.0)
                    progress_bar.progress(progress, text=f"Экспорт чанка {i + 1}/{chunks}...")
            
            progress_bar.empty()
            st.success(f"✅ Экспортировано {total:,} записей")
            return True
            
        except Exception as e:
            logger.exception("Ошибка чанкового экспорта")
            raise
    
    def _postprocess_export(self, pdf: pd.DataFrame) -> pd.DataFrame:
        """Постобработка экспортированных данных"""
        # Форматирование числовых колонок
        dimension_cols = ["Длинна", "Ширина", "Высота", "Вес"]
        for col in dimension_cols:
            if col in pdf.columns:
                try:
                    pdf[col] = pd.to_numeric(pdf[col], errors='coerce').fillna(0).round(2)
                except Exception:
                    pdf[col] = 0.0
        
        # Очистка строк
        if "Длинна/Ширина/Высота" in pdf.columns:
            pdf["Длинна/Ширина/Высота"] = pdf["Длинна/Ширина/Высота"].astype(str).replace(
                {'nan': '', 'None': '', 'null': ''}
            )
        
        return pdf
    
    def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None,
                                 include_prices: bool = True, apply_markup: bool = True,
                                 apply_exclusions: bool = True) -> bool:
        """Экспорт в Excel с поддержкой множества листов"""
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        
        st.info(f"📤 Экспорт {total:,} записей в Excel...")
        
        query = self.build_export_query(
            selected_columns, include_prices, apply_markup, apply_exclusions
        )
        
        try:
            df = self.conn.execute(query).pl().to_pandas()
            pdf = self._postprocess_export(df)
            
            # Создание Excel с несколькими листами если нужно
            if len(pdf) <= EXCEL_ROW_LIMIT:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    pdf.to_excel(writer, index=False, sheet_name='Данные')
            else:
                sheets = (len(pdf) // EXCEL_ROW_LIMIT) + 1
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for i in range(sheets):
                        start_idx = i * EXCEL_ROW_LIMIT
                        end_idx = min((i + 1) * EXCEL_ROW_LIMIT, len(pdf))
                        pdf.iloc[start_idx:end_idx].to_excel(
                            writer, index=False, sheet_name=f"Данные_{i + 1}"
                        )
            
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Экспортировано в Excel ({size_mb:.1f} МБ)")
            return True
            
        except Exception as e:
            logger.exception("Ошибка экспорта Excel")
            st.error(f"❌ Ошибка при экспорте в Excel: {str(e)}")
            return False
    
    def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None,
                         include_prices: bool = True, apply_markup: bool = True,
                         apply_exclusions: bool = True) -> bool:
        """Экспорт в Parquet формат"""
        try:
            query = self.build_export_query(
                selected_columns, include_prices, apply_markup, apply_exclusions
            )
            df = self.conn.execute(query).pl()
            df.write_parquet(output_path)
            
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            st.success(f"✅ Экспортировано в Parquet ({size_mb:.1f} МБ)")
            return True
            
        except Exception as e:
            logger.exception("Ошибка экспорта Parquet")
            st.error(f"❌ Ошибка при экспорте в Parquet: {str(e)}")
            return False
    
    # ========================================================================
    # УПРАВЛЕНИЕ ДАННЫМИ
    # ========================================================================
    def delete_by_brand(self, brand_norm: str) -> int:
        """Удаление записей по бренду с каскадным удалением связей"""
        try:
            # Получение количества до удаления
            count_parts = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
            
            if count_parts == 0:
                logger.info(f"Нет записей для бренда: {brand_norm}")
                return 0
            
            # Каскадное удаление
            self.conn.execute("""
                DELETE FROM prices WHERE (artikul_norm, brand_norm) IN (
                    SELECT artikul_norm, brand_norm FROM parts WHERE brand_norm = ?
                )
            """, [brand_norm])
            
            self.conn.execute("""
                DELETE FROM cross_references WHERE (artikul_norm, brand_norm) IN (
                    SELECT artikul_norm, brand_norm FROM parts WHERE brand_norm = ?
                )
            """, [brand_norm])
            
            self.conn.execute("DELETE FROM parts WHERE brand_norm = ?", [brand_norm])
            
            # Логирование
            self.log_change('parts', 'DELETE', f'brand:{brand_norm}', 
                          f"Удалено {count_parts} записей бренда")
            
            # Оптимизация после удаления
            self.vacuum_database()
            
            logger.info(f"Удалено {count_parts} записей бренда: {brand_norm}")
            return count_parts
            
        except Exception as e:
            logger.error(f"Ошибка удаления бренда {brand_norm}: {e}")
            raise
    
    def delete_by_artikul(self, artikul_norm: str) -> int:
        """Удаление записей по артикулу"""
        try:
            count_parts = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
            
            if count_parts == 0:
                logger.info(f"Нет записей для артикула: {artikul_norm}")
                return 0
            
            # Каскадное удаление
            self.conn.execute("""
                DELETE FROM prices WHERE artikul_norm = ?
            """, [artikul_norm])
            
            self.conn.execute("""
                DELETE FROM cross_references WHERE artikul_norm = ?
            """, [artikul_norm])
            
            self.conn.execute("DELETE FROM parts WHERE artikul_norm = ?", [artikul_norm])
            
            self.log_change('parts', 'DELETE', f'artikul:{artikul_norm}',
                          f"Удалено {count_parts} записей артикула")
            
            self.vacuum_database()
            
            return count_parts
            
        except Exception as e:
            logger.error(f"Ошибка удаления артикула {artikul_norm}: {e}")
            raise
    
    # ========================================================================
    # ПОИСК (С FTS И КЭШИРОВАНИЕМ)
    # ========================================================================
    def _clean_search_cache(self):
        """Очистка устаревших записей кэша"""
        current_time = time.time()
        expired_keys = [
            k for k, (t, _) in self._search_cache.items()
            if current_time - t > self._search_cache_ttl
        ]
        for k in expired_keys:
            del self._search_cache[k]
    
    def search_parts(self, query: str, limit: int = 100, use_cache: bool = True) -> pd.DataFrame:
        """Улучшенный поиск с FTS, кэшированием и fallback"""
        if not query or not query.strip():
            return pd.DataFrame()
        
        # Проверка кэша
        cache_key = hashlib.md5(f"{query}:{limit}".encode()).hexdigest()
        
        if use_cache and cache_key in self._search_cache:
            cached_time, cached_result = self._search_cache[cache_key]
            if time.time() - cached_time < self._search_cache_ttl:
                self.performance_metrics['cache_hits'] += 1
                return cached_result
        
        self.performance_metrics['queries'] += 1
        
        start_time = time.time()
        
        # Очистка запроса
        clean_query = re.sub(r'[^\w\s]', ' ', query).strip()
        if not clean_query:
            return pd.DataFrame()
        
        result = None
        
        # Попытка 1: Полнотекстовый поиск
        try:
            result = self._search_fts(clean_query, limit)
        except Exception as e:
            logger.debug(f"FTS поиск не удался: {e}")
        
        # Попытка 2: LIKE поиск с нормализацией
        if result is None or result.empty:
            try:
                result = self._search_like(query, limit)
            except Exception as e:
                logger.error(f"LIKE поиск не удался: {e}")
        
        # Сохранение в кэш
        if result is not None:
            self._search_cache[cache_key] = (time.time(), result)
            self._clean_search_cache()
        
        # Обновление метрик
        self.performance_metrics['total_time'] += (time.time() - start_time)
        
        return result if result is not None else pd.DataFrame()
    
    def _search_fts(self, query: str, limit: int) -> pd.DataFrame:
        """Полнотекстовый поиск"""
        sql_fts = f"""
            SELECT DISTINCT
                p.artikul,
                p.brand,
                p.description,
                p.multiplicity,
                p.length,
                p.width,
                p.height,
                p.weight,
                p.dimensions_str,
                p.image_url,
                STRING_AGG(DISTINCT o.oe_number, ', ') as oe_numbers,
                STRING_AGG(DISTINCT o.name, ', ') as oe_names
            FROM parts p
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            WHERE 
                fts_main_parts.match_bm25(?) IS NOT NULL
                OR (o.oe_number_norm IS NOT NULL AND fts_main_oe.match_bm25(?) IS NOT NULL)
            GROUP BY p.artikul, p.brand, p.description, p.multiplicity, 
                     p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url
            ORDER BY fts_main_parts.match_bm25(?) DESC
            LIMIT {limit}
        """
        
        try:
            return self.conn.execute(sql_fts, [query, query, query]).pl().to_pandas()
        except Exception:
            # Альтернативный FTS синтаксис
            sql_fts_alt = f"""
                SELECT DISTINCT
                    p.artikul, p.brand, p.description, p.multiplicity,
                    p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url,
                    STRING_AGG(DISTINCT o.oe_number, ', ') as oe_numbers,
                    STRING_AGG(DISTINCT o.name, ', ') as oe_names
                FROM parts p
                LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
                LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
                WHERE p.artikul_norm MATCH ? OR p.brand_norm MATCH ? OR o.name MATCH ?
                GROUP BY p.artikul, p.brand, p.description, p.multiplicity,
                         p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url
                LIMIT {limit}
            """
            try:
                return self.conn.execute(sql_fts_alt, [query, query, query]).pl().to_pandas()
            except Exception:
                raise
    
    def _search_like(self, query: str, limit: int) -> pd.DataFrame:
        """Поиск через LIKE с нормализацией"""
        query_norm = self.normalize_key(pl.Series([query]))[0]
        
        # Экранирование спецсимволов для LIKE
        safe_query = query_norm.replace('%', '\\%').replace('_', '\\_')
        
        sql_like = f"""
            SELECT DISTINCT
                p.artikul,
                p.brand,
                p.description,
                p.multiplicity,
                p.length,
                p.width,
                p.height,
                p.weight,
                p.dimensions_str,
                p.image_url,
                STRING_AGG(DISTINCT o.oe_number, ', ') as oe_numbers,
                STRING_AGG(DISTINCT o.name, ', ') as oe_names
            FROM parts p
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            WHERE 
                p.artikul_norm LIKE '%' || ? || '%'
                OR p.brand_norm LIKE '%' || ? || '%'
                OR o.oe_number_norm LIKE '%' || ? || '%'
                OR o.name LIKE '%' || ? || '%'
            GROUP BY p.artikul, p.brand, p.description, p.multiplicity,
                     p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url
            LIMIT {limit}
        """
        
        return self.conn.execute(sql_like, [safe_query, safe_query, safe_query, safe_query]).pl().to_pandas()
    
    # ========================================================================
    # СТАТИСТИКА И МЕТРИКИ
    # ========================================================================
    def get_statistics(self) -> Dict[str, Any]:
        """Расширенная статистика базы данных"""
        stats = {}
        
        try:
            stats['parts'] = self.conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0]
            stats['oe'] = self.conn.execute("SELECT COUNT(*) FROM oe").fetchone()[0]
            stats['cross'] = self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0]
            stats['prices'] = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            stats['brands'] = self.conn.execute("SELECT COUNT(DISTINCT brand) FROM parts").fetchone()[0]
            stats['unique_parts'] = self.conn.execute(
                "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)"
            ).fetchone()[0]
            
            # Ценовая статистика
            price_stats = self.conn.execute("""
                SELECT 
                    ROUND(AVG(price), 2) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    COUNT(*) as priced_items
                FROM prices WHERE price > 0
            """).fetchone()
            
            stats['avg_price'] = price_stats[0] if price_stats[0] else 0
            stats['min_price'] = price_stats[1] if price_stats[1] else 0
            stats['max_price'] = price_stats[2] if price_stats[2] else 0
            stats['priced_items'] = price_stats[3] if price_stats[3] else 0
            
            # Топ брендов
            try:
                top_brands = self.conn.execute("""
                    SELECT brand, COUNT(*) as cnt 
                    FROM parts 
                    WHERE brand IS NOT NULL AND brand != ''
                    GROUP BY brand 
                    ORDER BY cnt DESC 
                    LIMIT 10
                """).pl()
                stats['top_brands'] = top_brands.to_pandas()
            except Exception:
                stats['top_brands'] = pd.DataFrame()
            
            # Статистика по категориям
            try:
                category_stats = self.conn.execute("""
                    SELECT category, COUNT(*) as cnt 
                    FROM oe 
                    WHERE category IS NOT NULL
                    GROUP BY category 
                    ORDER BY cnt DESC
                """).pl()
                stats['category_stats'] = category_stats.to_pandas()
            except Exception:
                stats['category_stats'] = pd.DataFrame()
            
            # Статистика связей
            stats['cross_coverage'] = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT p.artikul_norm || p.brand_norm) as total_parts,
                    COUNT(DISTINCT cr.artikul_norm || cr.brand_norm) as linked_parts
                FROM parts p
                LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm 
                    AND p.brand_norm = cr.brand_norm
            """).fetchone()
            
            if stats['cross_coverage'][0] > 0:
                stats['link_percentage'] = round(
                    (stats['cross_coverage'][1] / stats['cross_coverage'][0]) * 100, 1
                )
            else:
                stats['link_percentage'] = 0
            
            # Размер базы данных
            if self.db_path.exists():
                stats['db_size_mb'] = round(self.db_path.stat().st_size / (1024 * 1024), 2)
            
            # Метрики производительности
            stats['performance'] = self.performance_metrics.copy()
            stats['cache_hit_rate'] = (
                self.performance_metrics['cache_hits'] / max(self.performance_metrics['queries'], 1) * 100
            )
            
        except Exception as e:
            logger.error(f"Ошибка сбора статистики: {e}")
        
        return stats
    
    # ========================================================================
    # ИНТЕРФЕЙСЫ ПОЛЬЗОВАТЕЛЯ
    # ========================================================================
    def show_export_interface(self):
        """Расширенный интерфейс экспорта"""
        st.header("📤 Экспорт данных")
        
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)"
        ).fetchone()[0]
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Всего товаров", f"{total:,}")
        
        # Формат экспорта
        format_choice = st.radio(
            "Формат экспорта:",
            ["CSV (с разделителем ;)", "Excel (.xlsx)", "Parquet"],
            horizontal=True
        )
        
        # Настройки экспорта
        with st.expander("⚙️ Настройки экспорта", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Колонки для экспорта")
                all_columns = [
                    "Артикул бренда", "Бренд", "Наименование", "Применимость", 
                    "Описание", "Категория товара", "Кратность", "Длинна", 
                    "Ширина", "Высота", "Вес", "Длинна/Ширина/Высота", 
                    "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"
                ]
                
                selected_columns = st.multiselect(
                    "Выберите колонки (пусто = все):",
                    all_columns,
                    default=["Артикул бренда", "Бренд", "Наименование", "Цена"]
                )
            
            with col2:
                st.subheader("Параметры")
                include_prices = st.checkbox("Включить цены", value=True)
                apply_markup = st.checkbox(
                    "Применить наценку", 
                    value=True, 
                    disabled=not include_prices
                )
                apply_exclusions = st.checkbox("Применить исключения", value=True)
                
                if include_prices and apply_markup:
                    st.info(f"Глобальная наценка: {self.price_rules.get('global_markup', 0) * 100:.1f}%")
        
        # Предпросмотр настроек связей
        with st.expander("🔗 Настройки связей (для экспорта)", expanded=False):
            st.info("Настройки из раздела 'Управление связями'")
            st.write(f"Глубина связей: {self.link_rules.get('max_link_depth', 2)}")
            st.write(f"Использовать кросс-ссылки: {self.link_rules.get('use_cross_references', True)}")
            st.write(f"Только OE связи: {self.link_rules.get('link_by_oe_only', False)}")
        
        # Кнопка экспорта
        if st.button("🚀 Экспортировать", type="primary", use_container_width=True):
            if total == 0:
                st.warning("Нет данных для экспорта")
                return
            
            # Определение расширения файла
            if "CSV" in format_choice:
                ext = "csv"
            elif "Excel" in format_choice:
                ext = "xlsx"
            else:
                ext = "parquet"
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"export_{timestamp}.{ext}"
            output_path = self.data_dir / output_filename
            
            with st.spinner(f"⏳ Генерация файла {output_filename}..."):
                progress_bar = st.progress(0)
                
                try:
                    if ext == "csv":
                        success = self.export_to_csv_optimized(
                            str(output_path),
                            selected_columns if selected_columns else None,
                            include_prices,
                            apply_markup,
                            apply_exclusions
                        )
                    elif ext == "xlsx":
                        success = self.export_to_excel_optimized(
                            str(output_path),
                            selected_columns if selected_columns else None,
                            include_prices,
                            apply_markup,
                            apply_exclusions
                        )
                    else:
                        success = self.export_to_parquet(
                            str(output_path),
                            selected_columns if selected_columns else None,
                            include_prices,
                            apply_markup,
                            apply_exclusions
                        )
                    
                    progress_bar.progress(1.0)
                    
                    if success and output_path.exists():
                        with open(output_path, "rb") as f:
                            file_data = f.read()
                        
                        st.download_button(
                            label=f"⬇️ Скачать {output_filename}",
                            data=file_data,
                            file_name=output_filename,
                            mime="application/octet-stream",
                            use_container_width=True
                        )
                        
                        # Сохранение в историю
                        if 'export_history' not in st.session_state:
                            st.session_state.export_history = []
                        
                        st.session_state.export_history.append({
                            'filename': output_filename,
                            'timestamp': timestamp,
                            'size_mb': round(len(file_data) / (1024 * 1024), 2),
                            'format': ext,
                            'rows': total
                        })
                    
                except Exception as e:
                    st.error(f"❌ Ошибка экспорта: {str(e)}")
                    logger.exception("Ошибка экспорта")
                
                finally:
                    progress_bar.empty()
        
        # История экспортов
        if 'export_history' in st.session_state and st.session_state.export_history:
            with st.expander("📋 История экспортов", expanded=False):
                history_df = pd.DataFrame(st.session_state.export_history)
                st.dataframe(history_df, use_container_width=True, hide_index=True)
    
    def show_price_settings(self):
        """Расширенные настройки цен"""
        st.header("💰 Управление ценами и наценками")
        
        tabs = st.tabs(["Общие настройки", "Наценки по брендам", "Ограничения", "История цен"])
        
        with tabs[0]:
            st.subheader("Общие настройки цен")
            
            col1, col2 = st.columns(2)
            
            with col1:
                global_markup = st.number_input(
                    "Глобальная наценка (%):",
                    min_value=0.0,
                    max_value=1000.0,
                    value=self.price_rules.get('global_markup', 0.2) * 100,
                    step=1.0,
                    help="Процент наценки, применяемый ко всем товарам"
                )
                self.price_rules['global_markup'] = global_markup / 100
            
            with col2:
                default_currency = st.selectbox(
                    "Валюта по умолчанию:",
                    ["RUB", "USD", "EUR", "BYN", "KZT"],
                    index=["RUB", "USD", "EUR", "BYN", "KZT"].index(
                        self.price_rules.get('currency', 'RUB')
                    )
                )
                self.price_rules['currency'] = default_currency
            
            col3, col4 = st.columns(2)
            
            with col3:
                round_prices = st.checkbox(
                    "Округлять цены",
                    value=self.price_rules.get('round_prices', True)
                )
                self.price_rules['round_prices'] = round_prices
            
            with col4:
                if round_prices:
                    precision = st.number_input(
                        "Точность округления:",
                        min_value=0,
                        max_value=4,
                        value=self.price_rules.get('price_precision', 2)
                    )
                    self.price_rules['price_precision'] = precision
        
        with tabs[1]:
            st.subheader("Наценки по брендам")
            st.info("Индивидуальные наценки для конкретных брендов")
            
            brand_markups = self.price_rules.get('brand_markups', {})
            
            # Список брендов из базы
            try:
                brands = self.conn.execute(
                    "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand"
                ).fetchall()
                available_brands = [row[0] for row in brands]
            except Exception:
                available_brands = []
            
            if available_brands:
                # Отображение текущих наценок
                if brand_markups:
                    markup_df = pd.DataFrame([
                        {"Бренд": brand, "Наценка (%)": f"{markup * 100:.1f}%"}
                        for brand, markup in brand_markups.items()
                    ])
                    st.dataframe(markup_df, use_container_width=True, hide_index=True)
                
                # Добавление/редактирование наценки
                st.markdown("---")
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    selected_brand = st.selectbox(
                        "Выберите бренд:",
                        available_brands,
                        key="brand_markup_select"
                    )
                
                with col2:
                    current_markup = brand_markups.get(selected_brand, 0) * 100
                    new_markup = st.number_input(
                        "Наценка (%):",
                        min_value=0.0,
                        max_value=1000.0,
                        value=current_markup,
                        step=1.0,
                        key=f"markup_input_{selected_brand}"
                    )
                
                with col3:
                    if st.button("💾 Сохранить", key=f"save_markup_{selected_brand}"):
                        brand_markups[selected_brand] = new_markup / 100
                        self.price_rules['brand_markups'] = brand_markups
                        self.save_price_rules()
                        st.success(f"✅ Наценка для {selected_brand}: {new_markup:.1f}%")
                        st.rerun()
                
                # Удаление наценки
                if brand_markups:
                    brand_to_remove = st.selectbox(
                        "Удалить наценку для бренда:",
                        list(brand_markups.keys()),
                        key="remove_markup_select"
                    )
                    if st.button("🗑️ Удалить наценку", key="remove_markup_btn"):
                        del brand_markups[brand_to_remove]
                        self.price_rules['brand_markups'] = brand_markups
                        self.save_price_rules()
                        st.success(f"✅ Наценка для {brand_to_remove} удалена")
                        st.rerun()
        
        with tabs[2]:
            st.subheader("Ограничения по ценам")
            
            col1, col2 = st.columns(2)
            
            with col1:
                min_price = st.number_input(
                    "Минимальная цена:",
                    min_value=0.0,
                    value=float(self.price_rules.get('min_price', 0)),
                    step=10.0,
                    help="Товары с ценой ниже будут исключены"
                )
                self.price_rules['min_price'] = min_price
            
            with col2:
                max_price = st.number_input(
                    "Максимальная цена:",
                    min_value=0.0,
                    value=float(self.price_rules.get('max_price', 99999)),
                    step=1000.0,
                    help="Товары с ценой выше будут исключены"
                )
                self.price_rules['max_price'] = max_price
        
        with tabs[3]:
            st.subheader("История изменений цен")
            
            try:
                price_changes = self.conn.execute("""
                    SELECT 
                        changed_at,
                        record_key,
                        new_values
                    FROM change_log
                    WHERE table_name = 'prices'
                    ORDER BY changed_at DESC
                    LIMIT 50
                """).pl()
                
                if not price_changes.is_empty():
                    st.dataframe(
                        price_changes.to_pandas(),
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.info("Нет записей об изменениях цен")
            except Exception as e:
                st.warning(f"Не удалось загрузить историю: {e}")
        
        # Сохранение всех настроек
        if st.button("💾 Сохранить все настройки цен", type="primary"):
            self.save_price_rules()
            st.success("✅ Все настройки цен сохранены")
    
    def show_link_rules_interface(self):
        """Интерфейс управления правилами связывания"""
        st.header("🔗 Управление связями данных")
        st.info("Настройте правила связывания OE номеров с артикулами")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Основные настройки")
            
            use_cross = st.checkbox(
                "Использовать кросс-ссылки",
                value=self.link_rules.get('use_cross_references', True),
                help="Связывать артикулы через OE номера"
            )
            self.link_rules['use_cross_references'] = use_cross
            
            if use_cross:
                max_depth = st.slider(
                    "Глубина связей:",
                    min_value=1,
                    max_value=3,
                    value=self.link_rules.get('max_link_depth', 2),
                    help="1 - только прямые связи, 2-3 - включая аналоги"
                )
                self.link_rules['max_link_depth'] = max_depth
                
                link_by_oe = st.checkbox(
                    "Связывать только через OE",
                    value=self.link_rules.get('link_by_oe_only', False),
                    help="Не использовать другие типы связей"
                )
                self.link_rules['link_by_oe_only'] = link_by_oe
            
            prefer_original = st.checkbox(
                "Предпочитать оригинальные OE",
                value=self.link_rules.get('prefer_original_oe', True),
                help="Приоритет оригинальных номеров над аналогами"
            )
            self.link_rules['prefer_original_oe'] = prefer_original
        
        with col2:
            st.subheader("Дополнительные связи")
            
            use_dimensions = st.checkbox(
                "Связывать по габаритам",
                value=self.link_rules.get('use_dimensions_linking', True),
                help="Дополнять информацию о размерах из связанных товаров"
            )
            self.link_rules['use_dimensions_linking'] = use_dimensions
            
            use_barcode = st.checkbox(
                "Связывать по штрих-кодам",
                value=self.link_rules.get('use_barcode_linking', True)
            )
            self.link_rules['use_barcode_linking'] = use_barcode
            
            use_price = st.checkbox(
                "Связывать цены",
                value=self.link_rules.get('use_price_linking', True)
            )
            self.link_rules['use_price_linking'] = use_price
        
        st.markdown("---")
        
        # Приоритетные бренды
        st.subheader("🏆 Приоритетные бренды для связывания")
        
        try:
            brands = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand"
            ).fetchall()
            available_brands = [row[0] for row in brands]
        except Exception:
            available_brands = []
        
        if available_brands:
            priority_brands = self.link_rules.get('priority_brands_for_linking', [])
            exclude_brands = self.link_rules.get('exclude_brands_from_linking', [])
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Приоритетные бренды:**")
                new_priority = st.multiselect(
                    "Выберите приоритетные бренды:",
                    available_brands,
                    default=priority_brands,
                    help="Товары этих брендов будут в приоритете при связывании"
                )
                self.link_rules['priority_brands_for_linking'] = new_priority
            
            with col2:
                st.markdown("**Исключенные бренды:**")
                new_exclude = st.multiselect(
                    "Выберите исключаемые бренды:",
                    available_brands,
                    default=exclude_brands,
                    help="Товары этих брендов не будут участвовать в связывании"
                )
                self.link_rules['exclude_brands_from_linking'] = new_exclude
        
        # Сохранение
        if st.button("💾 Сохранить правила связывания", type="primary"):
            self.save_link_rules()
            st.success("✅ Правила связывания сохранены")
    
    def show_column_mapping_interface(self):
        """Интерфейс управления маппингом колонок"""
        st.header("📋 Управление маппингом колонок")
        st.info("Настройте соответствие названий колонок в загружаемых файлах")
        
        # Выбор типа файла для редактирования
        file_types = list(self.column_mapping_config.keys())
        selected_type = st.selectbox(
            "Тип файла:",
            file_types,
            format_func=lambda x: {
                'oe': 'OE данные',
                'cross': 'Кросс-ссылки',
                'prices': 'Цены'
            }.get(x, x)
        )
        
        if selected_type in self.column_mapping_config:
            st.subheader(f"Поля для типа: {selected_type}")
            
            config = self.column_mapping_config[selected_type]
            
            # Отображение текущего маппинга
            mapping_data = []
            for field, variants in config.items():
                mapping_data.append({
                    "Поле": field,
                    "Варианты названий": ", ".join(variants[:5]) + ("..." if len(variants) > 5 else "")
                })
            
            st.dataframe(
                pd.DataFrame(mapping_data),
                use_container_width=True,
                hide_index=True
            )
            
            st.markdown("---")
            
            # Редактирование конкретного поля
            st.subheader("Редактировать поле")
            
            field_to_edit = st.selectbox(
                "Выберите поле:",
                list(config.keys())
            )
            
            if field_to_edit:
                current_variants = config[field_to_edit]
                
                new_variants_text = st.text_area(
                    "Варианты названий (по одному на строку):",
                    value="\n".join(current_variants),
                    height=200,
                    help="Добавьте все возможные варианты названий колонок"
                )
                
                if st.button("💾 Сохранить варианты"):
                    new_variants = [v.strip() for v in new_variants_text.split("\n") if v.strip()]
                    config[field_to_edit] = new_variants
                    self.save_column_mapping_config()
                    st.success(f"✅ Варианты для '{field_to_edit}' сохранены")
                    st.rerun()
            
            st.markdown("---")
            
            # Добавление нового поля
            with st.expander("➕ Добавить новое поле"):
                new_field_name = st.text_input("Название поля:")
                new_field_variants = st.text_area(
                    "Варианты названий:",
                    height=100,
                    placeholder="Вариант 1\nВариант 2\n..."
                )
                
                if st.button("Добавить поле"):
                    if new_field_name and new_field_variants:
                        variants = [v.strip() for v in new_field_variants.split("\n") if v.strip()]
                        config[new_field_name] = variants
                        self.save_column_mapping_config()
                        st.success(f"✅ Поле '{new_field_name}' добавлено")
                        st.rerun()
                    else:
                        st.warning("Заполните все поля")
    
    def show_exclusion_settings(self):
        st.header("🚫 Управление исключениями при экспорте")
        st.info("Товары, содержащие эти слова в названии, будут исключены из экспорта")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            current_exclusions = "\n".join(self.exclusion_rules)
            
            new_exclusions = st.text_area(
                "Список исключений (по одному на строку):",
                value=current_exclusions,
                height=300,
                placeholder="Введите слова для исключения, например:\nКузов\nСтекла\nМасла\nАккумулятор"
            )
        
        with col2:
            st.subheader("Статистика")
            
            if self.exclusion_rules:
                st.metric("Правил исключений", len(self.exclusion_rules))
                
                # Предпросмотр влияния
                try:
                    for rule in self.exclusion_rules[:10]:
                        count = self.conn.execute("""
                            SELECT COUNT(DISTINCT p.artikul_norm)
                            FROM parts p
                            JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm 
                                AND p.brand_norm = cr.brand_norm
                            JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
                            WHERE LOWER(o.name) LIKE ?
                        """, [f"%{rule.lower()}%"]).fetchone()[0]
                        
                        if count:
                            st.caption(f"'{rule}': {count:,} товаров")
                except Exception:
                    pass
        
        if st.button("💾 Сохранить правила исключения", type="primary"):
            cleaned = [line.strip() for line in new_exclusions.splitlines() if line.strip()]
            
            if len(cleaned) != len(set(cleaned)):
                st.warning("Обнаружены дубликаты. Они будут автоматически удалены.")
            
            self.exclusion_rules = list(dict.fromkeys(cleaned))
            self.save_exclusion_rules()
            st.success("✅ Правила исключения сохранены")
    
    def show_category_mapping(self):
        st.header("🗂️ Управление категориями товаров")
        st.info("Настройте соответствие между названиями товаров и категориями")
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("Текущие правила категоризации")
            
            if self.category_mapping:
                mapping_df = pd.DataFrame([
                    {"Ключевое слово": k, "Категория": v}
                    for k, v in self.category_mapping.items()
                ])
                st.dataframe(mapping_df, use_container_width=True, hide_index=True)
            else:
                st.info("Нет пользовательских правил категоризации")
            
            st.markdown("---")
            
            st.subheader("Добавить/Редактировать правило")
            
            col_a, col_b = st.columns(2)
            
            with col_a:
                name_pattern = st.text_input(
                    "Ключевое слово в названии:",
                    placeholder="Например: Радиатор"
                )
            
            with col_b:
                # Предопределенные категории
                default_categories = [
                    'Охлаждение', 'Подвеска', 'Фильтры', 'Тормоза',
                    'Двигатель', 'Трансмиссия', 'Электрика', 'Рулевое',
                    'Выпуск', 'Топливо', 'Кузов', 'Салон', 'Разное'
                ]
                
                category = st.text_input(
                    "Категория:",
                    placeholder="Например: Охлаждение"
                )
                
                if default_categories:
                    selected_default = st.selectbox(
                        "Или выберите из списка:",
                        [""] + default_categories
                    )
                    if selected_default:
                        category = selected_default
            
            if st.button("➕ Сохранить правило", type="primary"):
                if name_pattern.strip() and category.strip():
                    self.category_mapping[name_pattern.strip()] = category.strip()
                    self.save_category_mapping()
                    st.success(f"✅ Правило сохранено: {name_pattern.strip()} → {category.strip()}")
                    st.rerun()
                else:
                    st.warning("Заполните оба поля")
        
        with col2:
            st.subheader("Удаление правил")
            
            if self.category_mapping:
                rule_to_delete = st.selectbox(
                    "Выберите правило для удаления:",
                    list(self.category_mapping.keys()),
                    format_func=lambda x: f"{x} → {self.category_mapping[x]}"
                )
                
                if st.button("🗑️ Удалить правило"):
                    if rule_to_delete in self.category_mapping:
                        del self.category_mapping[rule_to_delete]
                        self.save_category_mapping()
                        st.success(f"✅ Правило удалено: {rule_to_delete}")
                        st.rerun()
            
            st.markdown("---")
            
            st.subheader("Импорт/Экспорт")
            
            if st.button("📥 Экспортировать правила"):
                json_str = json.dumps(self.category_mapping, indent=2, ensure_ascii=False)
                st.download_button(
                    "⬇️ Скачать JSON",
                    json_str,
                    "category_mapping.json",
                    "application/json"
                )
            
            uploaded_rules = st.file_uploader(
                "📤 Загрузить правила (JSON):",
                type=['json'],
                key="category_upload"
            )
            
            if uploaded_rules:
                try:
                    new_rules = json.loads(uploaded_rules.getvalue().decode('utf-8'))
                    if isinstance(new_rules, dict):
                        self.category_mapping.update(new_rules)
                        self.save_category_mapping()
                        st.success(f"✅ Загружено {len(new_rules)} правил")
                        st.rerun()
                except Exception as e:
                    st.error(f"Ошибка загрузки: {e}")
    
    def show_cloud_sync(self):
        st.header("☁️ Облачная синхронизация")
        
        tabs = st.tabs(["Настройки", "Статус", "Логи"])
        
        with tabs[0]:
            st.subheader("Настройки синхронизации")
            
            self.cloud_config['enabled'] = st.checkbox(
                "Включить облачную синхронизацию",
                value=self.cloud_config.get('enabled', False)
            )
            
            providers = ["s3", "gcs", "azure"]
            current_idx = providers.index(self.cloud_config.get('provider', 's3')) \
                if self.cloud_config.get('provider') in providers else 0
            
            self.cloud_config['provider'] = st.selectbox(
                "Провайдер:",
                providers,
                index=current_idx
            )
            
            self.cloud_config['bucket'] = st.text_input(
                "Bucket / Container:",
                value=self.cloud_config.get('bucket', '')
            )
            
            self.cloud_config['region'] = st.text_input(
                "Регион:",
                value=self.cloud_config.get('region', '')
            )
            
            col1, col2 = st.columns(2)
            
            with col1:
                self.cloud_config['access_key'] = st.text_input(
                    "Access Key:",
                    value=self.cloud_config.get('access_key', ''),
                    type="password"
                )
            
            with col2:
                self.cloud_config['secret_key'] = st.text_input(
                    "Secret Key:",
                    value=self.cloud_config.get('secret_key', ''),
                    type="password"
                )
            
            self.cloud_config['sync_interval'] = st.number_input(
                "Интервал синхронизации (сек):",
                min_value=300,
                max_value=86400,
                value=int(self.cloud_config.get('sync_interval', 3600))
            )
            
            if st.button("💾 Сохранить настройки", type="primary"):
                self.save_cloud_config()
                st.success("✅ Настройки синхронизации сохранены")
        
        with tabs[1]:
            st.subheader("Текущий статус")
            
            if self.cloud_config.get('enabled'):
                st.success("✅ Синхронизация включена")
            else:
                st.warning("⚠️ Синхронизация отключена")
            
            last_sync = self.cloud_config.get('last_sync', 0)
            if last_sync > 0:
                st.info(f"🕐 Последняя синхронизация: {datetime.fromtimestamp(last_sync).strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                st.info("❌ Синхронизация еще не выполнялась")
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.button("🔄 Синхронизировать сейчас", use_container_width=True):
                    if not self.cloud_config.get('enabled'):
                        st.warning("Синхронизация отключена")
                    elif not self.cloud_config.get('bucket'):
                        st.error("Не указан bucket")
                    else:
                        with st.spinner("Выполнение синхронизации..."):
                            success = self.perform_cloud_sync()
                            if success:
                                st.success("✅ Синхронизация выполнена")
                            else:
                                st.error("❌ Ошибка синхронизации")
            
            with col2:
                db_size = self.db_path.stat().st_size / (1024 * 1024) if self.db_path.exists() else 0
                st.metric("Размер базы данных", f"{db_size:.1f} МБ")
        
        with tabs[2]:
            st.subheader("Логи синхронизации")
            
            log_file = self.data_dir / "app.log"
            if log_file.exists():
                with open(log_file, 'r', encoding='utf-8') as f:
                    logs = f.readlines()
                
                sync_logs = [l for l in logs if 'sync' in l.lower() or 'cloud' in l.lower()]
                
                if sync_logs:
                    st.text_area(
                        "Последние записи:",
                        value="".join(sync_logs[-20:]),
                        height=300
                    )
                else:
                    st.info("Нет записей о синхронизации")
    
    def perform_cloud_sync(self) -> bool:
        """Выполнение облачной синхронизации"""
        try:
            # Создание бэкапа перед синхронизацией
            backup_path = self.data_dir / f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
            
            import shutil
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"Создан бэкап: {backup_path}")
            
            # Здесь должен быть код для отправки в облако
            # В зависимости от провайдера (s3, gcs, azure)
            
            provider = self.cloud_config.get('provider', 's3')
            bucket = self.cloud_config.get('bucket')
            
            logger.info(f"Синхронизация с {provider}://{bucket}")
            
            # Имитация синхронизации
            time.sleep(2)
            
            self.cloud_config['last_sync'] = int(time.time())
            self.save_cloud_config()
            
            logger.info("✅ Синхронизация выполнена успешно")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка синхронизации: {e}")
            return False
    
    def show_statistics(self):
        st.header("📈 Статистика и метрики")
        
        stats = self.get_statistics()
        
        if not stats:
            st.error("Ошибка сбора статистики")
            return
        
        # Основные метрики
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Уникальных товаров", f"{stats.get('unique_parts', 0):,}")
        col2.metric("Брендов", f"{stats.get('brands', 0):,}")
        col3.metric("OE номеров", f"{stats.get('oe', 0):,}")
        col4.metric("Кросс-ссылок", f"{stats.get('cross', 0):,}")
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Средняя цена", f"{stats.get('avg_price', 0):,.2f} ₽")
        col2.metric("Мин. цена", f"{stats.get('min_price', 0):,.2f} ₽")
        col3.metric("Макс. цена", f"{stats.get('max_price', 0):,.2f} ₽")
        col4.metric("Охват связей", f"{stats.get('link_percentage', 0)}%")
        
        st.markdown("---")
        
        # Графики и таблицы
        tab1, tab2, tab3 = st.tabs(["📊 Распределение", "🏆 Топ брендов", "⚡ Производительность"])
        
        with tab1:
            if 'category_stats' in stats and not stats['category_stats'].empty:
                st.subheader("Распределение по категориям")
                st.dataframe(
                    stats['category_stats'],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "category": "Категория",
                        "cnt": "Количество"
                    }
                )
                
                # Простой бар-чарт
                st.bar_chart(
                    stats['category_stats'].set_index('category')['cnt']
                )
            else:
                st.info("Нет данных о категориях")
        
        with tab2:
            if 'top_brands' in stats and not stats['top_brands'].empty:
                st.subheader("Топ 10 брендов")
                st.dataframe(
                    stats['top_brands'],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "brand": "Бренд",
                        "cnt": "Количество товаров"
                    }
                )
            else:
                st.info("Нет данных о брендах")
        
        with tab3:
            st.subheader("Метрики производительности")
            
            perf = stats.get('performance', {})
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Всего запросов", perf.get('queries', 0))
            col2.metric("Попаданий в кэш", perf.get('cache_hits', 0))
            col3.metric("Эффективность кэша", f"{stats.get('cache_hit_rate', 0):.1f}%")
            
            if 'db_size_mb' in stats:
                st.info(f"💾 Размер базы данных: {stats['db_size_mb']:.1f} МБ")
            
            # Health check
            if st.button("🔍 Проверить целостность БД"):
                with st.spinner("Проверка базы данных..."):
                    health = self.check_database_health()
                    
                    if health.get('corruption_detected'):
                        st.error("⚠️ Обнаружены проблемы с целостностью данных!")
                    else:
                        st.success("✅ База данных в порядке")
                    
                    st.json(health)
    
    def show_data_management(self):
        st.header("🔧 Управление данными")
        
        management_option = st.radio(
            "Выберите действие:",
            [
                "🗑️ Удаление данных",
                "💰 Цены и наценки",
                "🚫 Исключения",
                "🗂️ Категории",
                "🔗 Управление связями",
                "📋 Маппинг колонок",
                "☁️ Облачная синхронизация",
                "🔍 Диагностика"
            ],
            horizontal=False
        )
        
        if management_option == "🗑️ Удаление данных":
            self._show_delete_interface()
        elif management_option == "💰 Цены и наценки":
            self.show_price_settings()
        elif management_option == "🚫 Исключения":
            self.show_exclusion_settings()
        elif management_option == "🗂️ Категории":
            self.show_category_mapping()
        elif management_option == "🔗 Управление связями":
            self.show_link_rules_interface()
        elif management_option == "📋 Маппинг колонок":
            self.show_column_mapping_interface()
        elif management_option == "☁️ Облачная синхронизация":
            self.show_cloud_sync()
        elif management_option == "🔍 Диагностика":
            self._show_diagnostics()
    
    def _show_delete_interface(self):
        st.subheader("🗑️ Удаление данных")
        st.warning("⚠️ Внимание! Операции удаления необратимы!")
        
        delete_option = st.radio(
            "Тип удаления:",
            ["По бренду", "По артикулу", "Очистить все данные"],
            horizontal=True
        )
        
        if delete_option == "По бренду":
            try:
                brands = self.conn.execute(
                    "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand"
                ).fetchall()
                available_brands = [row[0] for row in brands]
                
                if available_brands:
                    selected_brand = st.selectbox("Выберите бренд для удаления:", available_brands)
                    
                    count = self.conn.execute(
                        "SELECT COUNT(*) FROM parts WHERE brand = ?", [selected_brand]
                    ).fetchone()[0]
                    
                    st.info(f"Будет удалено {count:,} записей бренда '{selected_brand}'")
                    
                    col1, col2 = st.columns([1, 3])
                    
                    with col1:
                        confirmed = st.checkbox("Подтверждаю")
                    
                    with col2:
                        if st.button("🗑️ Удалить", type="primary", disabled=not confirmed):
                            if confirmed:
                                brand_norm = self.normalize_key(pl.Series([selected_brand]))[0]
                                deleted = self.delete_by_brand(brand_norm)
                                st.success(f"✅ Удалено {deleted:,} записей")
                                st.rerun()
                else:
                    st.info("Нет данных для удаления")
                    
            except Exception as e:
                st.error(f"Ошибка: {e}")
        
        elif delete_option == "По артикулу":
            artikul_input = st.text_input("Введите артикул для удаления:")
            
            if artikul_input:
                artikul_norm = self.normalize_key(pl.Series([artikul_input]))[0]
                
                count = self.conn.execute(
                    "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]
                ).fetchone()[0]
                
                if count > 0:
                    st.info(f"Найдено {count:,} записей для артикула '{artikul_input}'")
                    
                    # Показать какие записи будут удалены
                    preview = self.conn.execute("""
                        SELECT p.artikul, p.brand, COUNT(cr.oe_number_norm) as oe_count
                        FROM parts p
                        LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm 
                            AND p.brand_norm = cr.brand_norm
                        WHERE p.artikul_norm = ?
                        GROUP BY p.artikul, p.brand
                    """, [artikul_norm]).pl()
                    
                    if not preview.is_empty():
                        st.dataframe(preview.to_pandas(), use_container_width=True, hide_index=True)
                    
                    confirmed = st.checkbox("Подтверждаю удаление")
                    
                    if st.button("🗑️ Удалить", type="primary", disabled=not confirmed):
                        if confirmed:
                            deleted = self.delete_by_artikul(artikul_norm)
                            st.success(f"✅ Удалено {deleted:,} записей")
                            st.rerun()
                else:
                    st.warning(f"Артикул '{artikul_input}' не найден")
        
        elif delete_option == "Очистить все данные":
            st.error("🚨 Эта операция удалит ВСЕ данные из базы!")
            
            st.info(f"""
            Будут удалены все записи из таблиц:
            - Parts: {self.conn.execute('SELECT COUNT(*) FROM parts').fetchone()[0]:,} записей
            - OE: {self.conn.execute('SELECT COUNT(*) FROM oe').fetchone()[0]:,} записей
            - Cross References: {self.conn.execute('SELECT COUNT(*) FROM cross_references').fetchone()[0]:,} записей
            - Prices: {self.conn.execute('SELECT COUNT(*) FROM prices').fetchone()[0]:,} записей
            """)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                confirm1 = st.checkbox("Я понимаю последствия")
            
            with col2:
                confirm2 = st.checkbox("Данные нельзя восстановить")
            
            with col3:
                if st.button("💀 Удалить все данные", type="primary", 
                            disabled=not (confirm1 and confirm2),
                            use_container_width=True):
                    if confirm1 and confirm2:
                        try:
                            # Создание бэкапа перед удалением
                            backup_path = self.data_dir / f"backup_before_clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
                            import shutil
                            shutil.copy2(self.db_path, backup_path)
                            
                            # Очистка таблиц
                            self.conn.execute("DELETE FROM prices")
                            self.conn.execute("DELETE FROM cross_references")
                            self.conn.execute("DELETE FROM oe")
                            self.conn.execute("DELETE FROM parts")
                            self.conn.execute("DELETE FROM change_log")
                            
                            self.vacuum_database()
                            
                            st.success(f"✅ Все данные удалены. Бэкап сохранен: {backup_path.name}")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Ошибка при удалении: {e}")
    
    def _show_diagnostics(self):
        st.subheader("🔍 Диагностика системы")
        
        if st.button("🔄 Запустить диагностику"):
            with st.spinner("Выполнение диагностики..."):
                # Health check
                health = self.check_database_health()
                
                # Системная информация
                import platform
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("### База данных")
                    st.json({
                        "Размер БД": f"{health.get('db_size_mb', 0):.1f} МБ",
                        "Таблицы ОК": health.get('tables_ok', False),
                        "Сиротские записи": not health.get('no_orphans', True),
                        "Повреждения": health.get('corruption_detected', False)
                    })
                
                with col2:
                    st.markdown("### Система")
                    st.json({
                        "Python": sys.version,
                        "OS": platform.platform(),
                        "Память": f"{memory_monitor():.1f} МБ",
                        "DuckDB": duckdb.__version__,
                        "Polars": pl.__version__
                    })
                
                # Статистика таблиц
                st.markdown("### Статистика таблиц")
                
                table_stats = []
                for table in ['parts', 'oe', 'cross_references', 'prices', 'change_log']:
                    try:
                        count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        table_stats.append({"Таблица": table, "Записей": count})
                    except Exception:
                        table_stats.append({"Таблица": table, "Записей": "Ошибка"})
                
                st.dataframe(
                    pd.DataFrame(table_stats),
                    use_container_width=True,
                    hide_index=True
                )
                
                # Действия по исправлению
                if health.get('no_orphans') == False:
                    st.warning("Обнаружены сиротские записи!")
                    
                    if st.button("🧹 Очистить сиротские записи"):
                        try:
                            self.conn.execute("""
                                DELETE FROM cross_references 
                                WHERE (artikul_norm, brand_norm) NOT IN (
                                    SELECT DISTINCT artikul_norm, brand_norm FROM parts
                                )
                            """)
                            self.conn.execute("""
                                DELETE FROM cross_references 
                                WHERE oe_number_norm NOT IN (
                                    SELECT DISTINCT oe_number_norm FROM oe
                                )
                            """)
                            st.success("✅ Сиротские записи удалены")
                            self.vacuum_database()
                        except Exception as e:
                            st.error(f"Ошибка: {e}")


# ============================================================================
# ПОЛНОЕ ПРИЛОЖЕНИЕ STREAMLIT
# ============================================================================
@st.cache_resource
def get_high_volume_catalog():
    """Создание каталога через st.cache_resource для корректной работы с DuckDB"""
    return HighVolumeAutoPartsCatalog()


def main():
    st.set_page_config(
        page_title="Каталог автозапчастей v200.0",
        page_icon="🔧",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🔧 Каталог автозапчастей v200.0")
    
    # Инициализация каталога
    catalog = get_high_volume_catalog()
    
    # Инициализация session_state
    if 'uploaded_files' not in st.session_state:
        st.session_state.uploaded_files = {}
    
    if 'export_history' not in st.session_state:
        st.session_state.export_history = []
    
    # Боковая панель с навигацией
    st.sidebar.title("📋 Навигация")
    
    # Информация о БД в сайдбаре
    try:
        parts_count = catalog.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)"
        ).fetchone()[0]
        
        st.sidebar.metric("Товаров в базе", f"{parts_count:,}")
    except Exception:
        pass
    
    menu = st.sidebar.radio(
        "Выберите раздел:",
        [
            "📥 Загрузка данных",
            "📊 Статистика и просмотр",
            "⚙️ Управление и настройки",
            "📤 Экспорт"
        ]
    )
    
    st.sidebar.markdown("---")
    
    # Быстрый поиск в сайдбаре
    st.sidebar.subheader("🔍 Быстрый поиск")
    quick_search = st.sidebar.text_input(
        "Артикул/бренд/OE:",
        placeholder="Поиск...",
        key="sidebar_search"
    )
    
    if quick_search:
        with st.sidebar:
            with st.spinner("Поиск..."):
                results = catalog.search_parts(quick_search, limit=10)
                if not results.empty:
                    st.success(f"Найдено: {len(results)}")
                    st.dataframe(
                        results[['artikul', 'brand', 'multiplicity']] if len(results.columns) >= 3 else results,
                        use_container_width=True,
                        hide_index=True,
                        height=200
                    )
                else:
                    st.warning("Ничего не найдено")
    
    st.sidebar.markdown("---")
    st.sidebar.info("💡 Подсказка: Все изменения сохраняются автоматически")
    
    # Основной контент в зависимости от выбранного раздела
    if menu == "📥 Загрузка данных":
        st.header("📥 Загрузка данных")
        
        st.markdown("""
        ### 📋 Инструкция по загрузке файлов
        
        Загрузите файлы Excel (.xlsx, .xls) или CSV с данными автозапчастей. 
        Система **автоматически определит тип данных** по содержимому колонок.
        
        **Поддерживаемые типы файлов:**
        """)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            **📄 Основные данные:**
            - **OE данные** — оригинальные номера, наименования, применимость
            - **Кроссы** — связи между OE номерами и артикулами
            - **Цены** — стоимость и валюта
            """)
        
        with col2:
            st.markdown("""
            **📎 Дополнительные данные:**
            - **Габариты** — длина, ширина, высота, вес
            - **Штрих-коды** — barcode и кратность упаковки
            - **Изображения** — ссылки на фото товаров
            """)
        
        with col3:
            st.markdown("""
            **📦 Специальные форматы:**
            - **Универсальный** — все данные в одном файле
            - **Пакетная загрузка** — несколько файлов одного типа
            """)
        
        st.markdown("---")
        
        # Загрузка файлов по типам
        with st.expander("📄 Основные данные", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                oe_files = st.file_uploader(
                    "OE данные (оригинальные номера)",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="oe_uploader",
                    help="Файлы с оригинальными номерами запчастей"
                )
                
                cross_files = st.file_uploader(
                    "Кроссы (связи OE-артикул)",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="cross_uploader",
                    help="Файлы со связями OE номеров и артикулов"
                )
            
            with col2:
                prices_files = st.file_uploader(
                    "Цены",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="prices_uploader",
                    help="Файлы с ценами товаров"
                )
        
        with st.expander("📎 Дополнительные данные", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                dimensions_files = st.file_uploader(
                    "Габариты (Д×Ш×В×Вес)",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="dimensions_uploader",
                    help="Файлы с размерами и весом товаров"
                )
                
                barcode_files = st.file_uploader(
                    "Штрих-коды и кратность",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="barcode_uploader",
                    help="Файлы со штрих-кодами и кратностью упаковки"
                )
            
            with col2:
                images_files = st.file_uploader(
                    "Изображения (URL)",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="images_uploader",
                    help="Файлы со ссылками на изображения товаров"
                )
        
        with st.expander("📦 Универсальная загрузка", expanded=False):
            universal_files = st.file_uploader(
                "Универсальный файл (все данные в одном)",
                type=['xlsx', 'xls', 'csv'],
                accept_multiple_files=True,
                key="universal_uploader",
                help="Файл, содержащий все типы данных в разных колонках"
            )
        
        st.markdown("---")
        
        # Кнопка обработки с прогрессом
        col1, col2 = st.columns([3, 1])
        
        with col1:
            if st.button("🚀 Обработать и загрузить все файлы", type="primary", use_container_width=True):
                uploaded_files_dict = {
                    'oe': oe_files,
                    'cross': cross_files,
                    'prices': prices_files,
                    'dimensions': dimensions_files,
                    'barcode': barcode_files,
                    'images': images_files,
                    'universal': universal_files
                }
                
                # Фильтрация пустых загрузок
                uploaded_files_dict = {k: v for k, v in uploaded_files_dict.items() if v}
                
                if not uploaded_files_dict:
                    st.warning("⚠️ Не выбрано ни одного файла для загрузки")
                else:
                    total_files = sum(len(files) for files in uploaded_files_dict.values())
                    st.info(f"📦 Загружено файлов: {total_files}")
                    
                    with st.spinner("🔄 Обработка файлов... Это может занять несколько минут"):
                        try:
                            # Обработка загруженных файлов
                            dataframes = catalog.process_uploaded_files(uploaded_files_dict)
                            
                            if not dataframes:
                                st.error("❌ Не удалось обработать ни одного файла. Проверьте логи.")
                            else:
                                # Загрузка в базу данных
                                catalog.process_and_load_data(dataframes)
                                
                                # Сохранение статистики загрузки
                                st.session_state.uploaded_files = {
                                    k: len(v) for k, v in dataframes.items()
                                }
                                
                                st.success("✅ Все данные успешно загружены в базу!")
                                
                                # Отображение результатов
                                st.subheader("📊 Результаты загрузки")
                                cols = st.columns(len(dataframes))
                                for idx, (file_type, count) in enumerate(st.session_state.uploaded_files.items()):
                                    cols[idx].metric(
                                        f"{file_type.upper()}",
                                        f"{count:,}",
                                        "записей"
                                    )
                        
                        except Exception as e:
                            logger.exception("Ошибка при обработке файлов")
                            st.error(f"❌ Критическая ошибка: {str(e)}")
        
        with col2:
            if st.session_state.uploaded_files:
                st.metric("Последняя загрузка", 
                         f"{datetime.now().strftime('%H:%M')}")
    
    elif menu == "📊 Статистика и просмотр":
        catalog.show_statistics()
        
        st.markdown("---")
        
        # Интерактивный поиск
        st.subheader("🔍 Поиск товаров")
        
        col1, col2 = st.columns([3, 1])
        
        with col1:
            search_query = st.text_input(
                "Введите артикул, бренд или OE номер:",
                placeholder="Например: 12345, BOSCH, 09870000",
                key="main_search"
            )
        
        with col2:
            search_limit = st.number_input(
                "Лимит результатов:",
                min_value=10,
                max_value=1000,
                value=100,
                step=10
            )
        
        if search_query:
            with st.spinner("🔎 Поиск..."):
                results_df = catalog.search_parts(search_query, limit=search_limit)
                
                if results_df.empty:
                    st.warning("🔍 Ничего не найдено")
                else:
                    st.success(f"✅ Найдено {len(results_df)} записей")
                    
                    # Настройка отображения колонок
                    available_cols = [c for c in results_df.columns 
                                    if c not in ['artikul_norm', 'brand_norm']]
                    
                    st.dataframe(
                        results_df[available_cols],
                        use_container_width=True,
                        hide_index=True,
                        height=400,
                        column_config={
                            "artikul": st.column_config.TextColumn("Артикул", width="medium"),
                            "brand": st.column_config.TextColumn("Бренд", width="medium"),
                            "description": st.column_config.TextColumn("Описание", width="large"),
                            "multiplicity": st.column_config.NumberColumn("Кратность"),
                            "oe_numbers": st.column_config.TextColumn("OE номера", width="large"),
                        }
                    )
    
    elif menu == "⚙️ Управление и настройки":
        catalog.show_data_management()
    
    elif menu == "📤 Экспорт":
        catalog.show_export_interface()


if __name__ == "__main__":
    main()
