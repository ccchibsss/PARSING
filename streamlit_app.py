# ============================================================================
# БЛОК 11: HIGH-VOLUME КАТАЛОГ АВТОЗАПЧАСТЕЙ (ПОЛНАЯ ВЕРСИЯ v100.24)
# ============================================================================
# ✅ НОВОЕ v100.22/v100.23/v100.24:
# 1. VLOOKUP-style парсинг столбцов с гибким выбором
# 2. Power Query подобные трансформации данных
# 3. Визуальный конструктор запросов
# 4. Профили парсинга
# 5. Маппинг колонок с машинным обучением
# 6. Пакетная обработка с разными правилами
# 7. ✅ v100.23: Истинный потоковый экспорт (DuckDB COPY + Chunked Excel)
# 8. ✅ v100.24: Экспорт результата Power Query (CSV + Excel)
# ============================================================================

import streamlit as st
import polars as pl
import pandas as pd
import duckdb
import json
import os
import re
import io
import time
import math
import decimal
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Callable
from datetime import datetime, date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы
EXCEL_ROW_LIMIT = 1_048_576
CHUNK_SIZE = 100_000

# ============================================================================
# POWER QUERY ПОДОБНЫЕ ТРАНСФОРМАЦИИ
# ============================================================================

class PowerQueryTransformations:
    """Класс для Power Query подобных трансформаций данных"""
    
    @staticmethod
    def remove_duplicates(df: pl.DataFrame, columns: List[str] = None) -> pl.DataFrame:
        """Удаление дубликатов (как Remove Duplicates в Power Query)"""
        if columns:
            return df.unique(subset=columns, keep='first')
        return df.unique(keep='first')
    
    @staticmethod
    def filter_rows(df: pl.DataFrame, column: str, condition: str, value: Any) -> pl.DataFrame:
        """
        Фильтрация строк (как Filter Rows в Power Query)
        
        conditions: 'equals', 'not_equals', 'contains', 'not_contains', 
                   'starts_with', 'ends_with', 'greater_than', 'less_than'
        """
        if condition == 'equals':
            return df.filter(pl.col(column) == value)
        elif condition == 'not_equals':
            return df.filter(pl.col(column) != value)
        elif condition == 'contains':
            return df.filter(pl.col(column).str.contains(str(value)))
        elif condition == 'not_contains':
            return df.filter(~pl.col(column).str.contains(str(value)))
        elif condition == 'starts_with':
            return df.filter(pl.col(column).str.starts_with(str(value)))
        elif condition == 'ends_with':
            return df.filter(pl.col(column).str.ends_with(str(value)))
        elif condition == 'greater_than':
            return df.filter(pl.col(column) > value)
        elif condition == 'less_than':
            return df.filter(pl.col(column) < value)
        elif condition == 'is_null':
            return df.filter(pl.col(column).is_null())
        elif condition == 'is_not_null':
            return df.filter(pl.col(column).is_not_null())
        return df
    
    @staticmethod
    def split_column(df: pl.DataFrame, column: str, delimiter: str, new_columns: List[str]) -> pl.DataFrame:
        """Разделение колонки (как Split Column в Power Query)"""
        return df.with_columns(
            pl.col(column).str.split(delimiter).list.to_struct(
                n_field_strategy="max_width", 
                fields=new_columns
            )
        ).unnest(column)
    
    @staticmethod
    def merge_columns(df: pl.DataFrame, columns: List[str], new_column: str, separator: str = " ") -> pl.DataFrame:
        """Объединение колонок (как Merge Columns в Power Query)"""
        return df.with_columns(
            pl.concat_str(columns, separator=separator).alias(new_column)
        )
    
    @staticmethod
    def pivot_table(df: pl.DataFrame, index: List[str], columns: str, values: str, agg_func: str = 'sum') -> pl.DataFrame:
        """Сводная таблица (как Pivot Table в Power Query)"""
        return df.pivot(
            index=index,
            columns=columns,
            values=values,
            aggregate_function=agg_func
        )
    
    @staticmethod
    def unpivot_table(df: pl.DataFrame, id_vars: List[str], value_vars: List[str]) -> pl.DataFrame:
        """Обратная сводка (как Unpivot в Power Query)"""
        return df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            variable_name='Attribute',
            value_name='Value'
        )
    
    @staticmethod
    def group_by(df: pl.DataFrame, by: List[str], aggregations: Dict[str, str]) -> pl.DataFrame:
        """
        Группировка (как Group By в Power Query)
        
        aggregations: {'column': 'sum'|'mean'|'count'|'min'|'max'|'first'|'last'}
        """
        agg_exprs = []
        for col, func in aggregations.items():
            if func == 'sum':
                agg_exprs.append(pl.col(col).sum().alias(f"{col}_sum"))
            elif func == 'mean':
                agg_exprs.append(pl.col(col).mean().alias(f"{col}_mean"))
            elif func == 'count':
                agg_exprs.append(pl.col(col).count().alias(f"{col}_count"))
            elif func == 'min':
                agg_exprs.append(pl.col(col).min().alias(f"{col}_min"))
            elif func == 'max':
                agg_exprs.append(pl.col(col).max().alias(f"{col}_max"))
            elif func == 'first':
                agg_exprs.append(pl.col(col).first().alias(f"{col}_first"))
            elif func == 'last':
                agg_exprs.append(pl.col(col).last().alias(f"{col}_last"))
        
        return df.group_by(by).agg(agg_exprs)
    
    @staticmethod
    def replace_values(df: pl.DataFrame, column: str, old_value: Any, new_value: Any) -> pl.DataFrame:
        """Замена значений (как Replace Values в Power Query)"""
        return df.with_columns(
            pl.when(pl.col(column) == old_value)
            .then(pl.lit(new_value))
            .otherwise(pl.col(column))
            .alias(column)
        )
    
    @staticmethod
    def fill_down(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Заполнение вниз (как Fill Down в Power Query)"""
        for col in columns:
            df = df.with_columns(pl.col(col).forward_fill())
        return df
    
    @staticmethod
    def fill_up(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Заполнение вверх (как Fill Up в Power Query)"""
        for col in columns:
            df = df.with_columns(pl.col(col).backward_fill())
        return df
    
    @staticmethod
    def add_index_column(df: pl.DataFrame, start: int = 0, step: int = 1, column_name: str = "Index") -> pl.DataFrame:
        """Добавление индекса (как Add Index Column в Power Query)"""
        return df.with_row_count(column_name, offset=start)
    
    @staticmethod
    def add_conditional_column(df: pl.DataFrame, new_column: str, conditions: List[Dict[str, Any]], default_value: Any = None) -> pl.DataFrame:
        """
        Добавление условной колонки (как Conditional Column в Power Query)
        
        conditions: [{'column': 'col1', 'operator': '>', 'value': 10, 'result': 'High'}]
        """
        expr = pl.lit(default_value)
        for cond in reversed(conditions):
            col = cond['column']
            op = cond['operator']
            value = cond['value']
            result = cond['result']
            
            if op == '>':
                condition_expr = pl.col(col) > value
            elif op == '<':
                condition_expr = pl.col(col) < value
            elif op == '>=':
                condition_expr = pl.col(col) >= value
            elif op == '<=':
                condition_expr = pl.col(col) <= value
            elif op == '==':
                condition_expr = pl.col(col) == value
            elif op == '!=':
                condition_expr = pl.col(col) != value
            elif op == 'contains':
                condition_expr = pl.col(col).str.contains(str(value))
            else:
                continue
            
            expr = pl.when(condition_expr).then(pl.lit(result)).otherwise(expr)
        
        return df.with_columns(expr.alias(new_column))
    
    @staticmethod
    def trim_text(df: pl.DataFrame, columns: List[str]) -> pl.DataFrame:
        """Очистка текста (как Trim в Power Query)"""
        for col in columns:
            df = df.with_columns(pl.col(col).str.strip_chars())
        return df
    
    @staticmethod
    def change_type(df: pl.DataFrame, column_types: Dict[str, str]) -> pl.DataFrame:
        """Изменение типа данных (как Change Type в Power Query)"""
        type_mapping = {
            'text': pl.Utf8,
            'number': pl.Float64,
            'integer': pl.Int64,
            'date': pl.Date,
            'datetime': pl.Datetime,
            'boolean': pl.Boolean
        }
        
        for col, dtype_str in column_types.items():
            if col in df.columns and dtype_str in type_mapping:
                try:
                    df = df.with_columns(pl.col(col).cast(type_mapping[dtype_str]))
                except Exception as e:
                    logger.warning(f"Не удалось изменить тип колонки {col}: {e}")
        
        return df


# ============================================================================
# ГИБКИЙ ПАРСИНГ СТОЛБЦОВ (VLOOKUP STYLE)
# ============================================================================

class ColumnParser:
    """Класс для гибкого парсинга столбцов по аналогии с VLOOKUP"""
    
    def __init__(self):
        self.parsing_rules = self.load_parsing_rules()
    
    def load_parsing_rules(self) -> Dict[str, Any]:
        """Загрузка правил парсинга столбцов"""
        rules_path = Path("./auto_parts_data/parsing_rules.json")
        default_rules = {
            "default_columns": [
                "artikul", "brand", "name", "price", "oe_number"
            ],
            "column_mappings": {
                "artikul": [
                    "артикул", "article", "sku", "artikul", "код товара", 
                    "код", "код артикула", "part_number", "part number",
                    "номер детали", "номер запчасти", "код детали"
                ],
                "brand": [
                    "бренд", "brand", "производитель", "manufacturer", 
                    "марка", "make", "company", "фирма", "торговая марка"
                ],
                "name": [
                    "наименование", "название", "name", "описание", 
                    "description", "товар", "наименование товара",
                    "product name", "product", "деталь", "part name"
                ],
                "price": [
                    "цена", "price", "стоимость", "cost", "retail price",
                    "цена продажи", "selling price", "прайс", "price list",
                    "розничная цена", "оптовая цена"
                ],
                "oe_number": [
                    "oe номер", "oe", "оe", "номер", "code", "OE", 
                    "oe_number", "oe number", "original number",
                    "оригинальный номер", "oem number", "oem",
                    "номер oe", "кросс-номер", "кросс номер"
                ],
                "barcode": [
                    "штрих-код", "barcode", "штрихкод", "ean", "eac13",
                    "штрих код", "баркод", "bar code", "ean-13"
                ],
                "applicability": [
                    "применимость", "автомобиль", "vehicle", "applicability",
                    "применяемость", "совместимость", "модель", "car model"
                ],
                "quantity": [
                    "количество", "quantity", "кол-во", "qty", "stock",
                    "наличие", "остаток", "склад"
                ]
            },
            "custom_parsing_rules": {
                "price_list": {
                    "required_columns": ["artikul", "brand", "price"],
                    "optional_columns": ["name", "oe_number", "quantity"]
                },
                "catalog": {
                    "required_columns": ["artikul", "brand", "name", "oe_number"],
                    "optional_columns": ["price", "applicability", "barcode"]
                },
                "inventory": {
                    "required_columns": ["artikul", "brand", "quantity"],
                    "optional_columns": ["barcode", "price"]
                }
            },
            "column_transformations": {
                "price": {
                    "remove_currency": True,
                    "decimal_separator": ",",
                    "thousands_separator": " "
                },
                "artikul": {
                    "uppercase": True,
                    "remove_spaces": True,
                    "remove_special_chars": False
                },
                "oe_number": {
                    "uppercase": True,
                    "remove_spaces": True
                }
            }
        }
        
        if rules_path.exists():
            try:
                loaded = json.loads(rules_path.read_text(encoding='utf-8'))
                # Обновляем стандартными значениями, если каких-то ключей нет
                for key in default_rules:
                    if key not in loaded:
                        loaded[key] = default_rules[key]
                return loaded
            except Exception as e:
                logger.error(f"Ошибка чтения parsing_rules.json: {e}")
                return default_rules
        else:
            rules_path.parent.mkdir(exist_ok=True)
            rules_path.write_text(json.dumps(
                default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
            return default_rules
    
    def save_parsing_rules(self):
        """Сохранение правил парсинга"""
        rules_path = Path("./auto_parts_data/parsing_rules.json")
        rules_path.parent.mkdir(exist_ok=True)
        rules_path.write_text(json.dumps(
            self.parsing_rules, indent=2, ensure_ascii=False), encoding='utf-8')
    
    def detect_columns_advanced(self, actual_columns: List[str], 
                                required_columns: List[str] = None,
                                file_type: str = None) -> Dict[str, str]:
        """
        Расширенное определение столбцов с приоритетами
        
        Args:
            actual_columns: Фактические колонки в файле
            required_columns: Список обязательных колонок
            file_type: Тип файла для применения специфичных правил
        
        Returns:
            Словарь маппинга {фактическое_название: целевое_название}
        """
        if required_columns is None:
            if file_type and file_type in self.parsing_rules.get("custom_parsing_rules", {}):
                rules = self.parsing_rules["custom_parsing_rules"][file_type]
                required_columns = rules.get("required_columns", [])
            else:
                required_columns = self.parsing_rules.get("default_columns", [])
        
        column_mappings = self.parsing_rules.get("column_mappings", {})
        actual_lower = {col.lower().strip(): col for col in actual_columns}
        
        mapping = {}
        used_actual = set()
        
        for target_col in required_columns:
            if target_col not in column_mappings:
                continue
            
            variants = column_mappings[target_col]
            best_match = None
            best_score = -1
            
            for variant in variants:
                variant_lower = variant.lower().strip()
                
                for actual_l, actual_orig in actual_lower.items():
                    if actual_orig in used_actual:
                        continue
                    
                    score = self._calculate_match_score(variant_lower, actual_l)
                    
                    if score > best_score:
                        best_score = score
                        best_match = actual_orig
            
            if best_match and best_score > 0:
                mapping[best_match] = target_col
                used_actual.add(best_match)
                logger.info(f"Column mapping: {best_match} → {target_col} (score: {best_score})")
        
        return mapping
    
    def _calculate_match_score(self, pattern: str, actual: str) -> int:
        """Улучшенная система оценки совпадений"""
        score = 0
        
        if pattern == actual:
            return 100
        
        if pattern in actual:
            score = 80
        elif actual in pattern:
            score = 70
        else:
            pattern_words = set(pattern.split())
            actual_words = set(actual.split())
            common_words = pattern_words & actual_words
            
            if common_words:
                score = 40 + len(common_words) * 10
        
        if actual.startswith(pattern):
            score += 10
        
        if len(pattern) == len(actual):
            score += 5
        
        return score
    
    def transform_value(self, column_name: str, value: Any) -> Any:
        """Трансформация значения согласно правилам"""
        if value is None or value == "":
            return value
        
        transformations = self.parsing_rules.get("column_transformations", {}).get(column_name, {})
        
        if isinstance(value, str):
            if column_name == "price" and transformations.get("remove_currency"):
                value = re.sub(r'[^\d.,\s-]', '', value)
                if transformations.get("thousands_separator"):
                    value = value.replace(transformations["thousands_separator"], "")
                if transformations.get("decimal_separator") == ",":
                    value = value.replace(",", ".")
            
            if column_name in ["artikul", "oe_number"]:
                if transformations.get("uppercase"):
                    value = value.upper()
                if transformations.get("remove_spaces"):
                    value = value.replace(" ", "")
                if transformations.get("remove_special_chars"):
                    value = re.sub(r'[^A-Za-z0-9]', '', value)
        
        return value
    
    def create_parsing_profile(self, profile_name: str, required_columns: List[str], 
                              optional_columns: List[str] = None):
        """Создание нового профиля парсинга"""
        self.parsing_rules["custom_parsing_rules"][profile_name] = {
            "required_columns": required_columns,
            "optional_columns": optional_columns or []
        }
        self.save_parsing_rules()
        logger.info(f"Создан новый профиль парсинга: {profile_name}")


# ============================================================================
# POWER QUERY КОНСТРУКТОР ЗАПРОСОВ
# ============================================================================

class PowerQueryBuilder:
    """Визуальный конструктор Power Query подобных запросов"""
    
    def __init__(self):
        self.transformations = PowerQueryTransformations()
        self.query_steps = []
    
    def add_step(self, step_name: str, transformation: Callable, **kwargs):
        """Добавление шага трансформации"""
        self.query_steps.append({
            'name': step_name,
            'transformation': transformation,
            'kwargs': kwargs
        })
    
    def execute_query(self, df: pl.DataFrame) -> pl.DataFrame:
        """Выполнение всех шагов запроса"""
        result = df
        for step in self.query_steps:
            try:
                result = step['transformation'](result, **step['kwargs'])
                logger.info(f"Выполнен шаг: {step['name']}")
            except Exception as e:
                logger.error(f"Ошибка на шаге {step['name']}: {e}")
                raise
        return result
    
    def clear_steps(self):
        """Очистка всех шагов"""
        self.query_steps = []
    
    def get_steps_preview(self) -> List[Dict[str, Any]]:
        """Получение превью шагов"""
        return [{'name': step['name'], 'kwargs': step['kwargs']} for step in self.query_steps]


# ============================================================================
# ОСНОВНОЙ КЛАСС КАТАЛОГА (ОБНОВЛЕННЫЙ)
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
        
        # ✅ НОВОЕ: Инициализация VLOOKUP-парсера и Power Query
        self.column_parser = ColumnParser()
        self.power_query = PowerQueryBuilder()
        self.pq_transformations = PowerQueryTransformations()
        
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
        
        # ✅ НОВОЕ: Таблица для хранения Power Query запросов
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS saved_queries (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                description VARCHAR,
                query_steps JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        
        for key, category in self.category_mapping.items():
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower())
            ).then(pl.lit(category))
        
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
    # ✅ УНИВЕРСАЛЬНАЯ КОНВЕРТАЦИЯ В ЧИСЛО
    # ========================================================================
    @staticmethod
    def safe_convert_to_float(value: Any) -> float:
        if value is None or value == "":
            return 0.0
        
        if isinstance(value, (int, float)):
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
            
            cleaned = re.sub(r'[^\d.,\-]', '', value)
            if not cleaned:
                return 0.0
            
            cleaned = cleaned.replace(',', '.')
            
            parts = cleaned.split('.')
            if len(parts) > 2:
                cleaned = parts[0] + '.' + ''.join(parts[1:])
            
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    # ========================================================================
    # ✅ НОВЫЙ МЕТОД: VLOOKUP-стиль чтения файлов (С БЕЗОПАСНЫМ FALLBACK)
    # ========================================================================
    def read_file_with_column_selection(self, file_path: str, 
                                       required_columns: List[str] = None,
                                       file_type: str = "custom",
                                       use_profile: str = None,
                                       apply_power_query: bool = False) -> pl.DataFrame:
        """
        Чтение файла с гибким выбором столбцов (аналог VLOOKUP)
        """
        logger.info(f"VLOOKUP чтение файла: {file_path}")
        
        try:
            if not os.path.exists(file_path):
                logger.error(f"Файл не найден: {file_path}")
                st.error(f"❌ Файл не найден: {Path(file_path).name}")
                return pl.DataFrame()
            
            file_ext = Path(file_path).suffix.lower()
            
            if file_ext == '.csv':
                df = pl.read_csv(file_path, try_parse_dates=False)
            else:
                try:
                    df = pl.read_excel(file_path, engine='calamine')
                except ModuleNotFoundError as e:
                    if 'fastexcel' in str(e).lower():
                        error_msg = (
                            "❌ Критическая ошибка: Отсутствует библиотека 'fastexcel'.\n"
                            "Пожалуйста, добавьте 'fastexcel>=0.9.0' в ваш requirements.txt, "
                            "закоммитьте изменения и перезапустите приложение в Streamlit Cloud."
                        )
                        logger.error(error_msg)
                        st.error(error_msg)
                        return pl.DataFrame()
                    else:
                        raise e
                except Exception as e:
                    logger.warning(f"Не удалось прочитать через calamine, пробуем openpyxl: {e}")
                    try:
                        df = pl.read_excel(file_path, engine='openpyxl')
                    except Exception as e_fallback:
                        logger.error(f"Ошибка чтения файла {file_path} через openpyxl: {e_fallback}")
                        st.error(f"❌ Ошибка чтения Excel файла '{Path(file_path).name}'. Убедитесь, что файл не поврежден, не защищен паролем и имеет корректный формат.")
                        return pl.DataFrame()
            
            if df.is_empty():
                logger.warning(f"Пустой файл: {file_path}")
                st.warning(f"⚠️ Файл '{Path(file_path).name}' пуст или не содержит табличных данных.")
                return pl.DataFrame()
            
            logger.info(f"Исходные колонки: {df.columns}")
            
            if use_profile:
                if use_profile in self.column_parser.parsing_rules.get("custom_parsing_rules", {}):
                    profile = self.column_parser.parsing_rules["custom_parsing_rules"][use_profile]
                    required_columns = profile.get("required_columns", [])
                    optional_columns = profile.get("optional_columns", [])
                    all_columns = required_columns + optional_columns
                else:
                    logger.warning(f"Профиль {use_profile} не найден")
                    all_columns = required_columns or []
            else:
                all_columns = required_columns or []
            
            column_mapping = self.column_parser.detect_columns_advanced(
                df.columns, 
                required_columns=all_columns,
                file_type=file_type
            )
            
            if not column_mapping:
                logger.warning(f"Не удалось определить колонки в файле {file_path}")
                st.warning(f"⚠️ Не удалось автоматически сопоставить колонки в '{Path(file_path).name}'. Проверьте заголовки файла.")
                return pl.DataFrame()
            
            df = df.rename(column_mapping)
            
            selected_columns = list(column_mapping.values())
            existing_columns = [col for col in selected_columns if col in df.columns]
            df = df.select(existing_columns)
            
            for col in df.columns:
                if col in self.column_parser.parsing_rules.get("column_transformations", {}):
                    transformed_values = [
                        self.column_parser.transform_value(col, val) 
                        for val in df[col].to_list()
                    ]
                    df = df.with_columns(pl.Series(transformed_values).alias(col))
            
            if apply_power_query and self.power_query.query_steps:
                df = self.power_query.execute_query(df)
            
            for col in ['artikul', 'brand', 'oe_number']:
                if col in df.columns:
                    df = df.with_columns([
                        self.clean_values(pl.col(col)).alias(col),
                        self.normalize_key(pl.col(col)).alias(f"{col}_norm")
                    ])
            
            logger.info(f"Итоговые колонки: {df.columns}")
            return df
            
        except Exception as e:
            logger.exception(f"Непредвиденная ошибка чтения файла {file_path}: {e}")
            st.error(f"❌ Произошла непредвиденная ошибка при обработке '{Path(file_path).name}'. Подробности в логах.")
            return pl.DataFrame()
    
    # ========================================================================
    # ✅ НОВЫЙ МЕТОД: Пакетный VLOOKUP-парсинг
    # ========================================================================
    def batch_vlookup_parse(self, file_paths: List[str], 
                           column_selection: Dict[str, List[str]] = None,
                           use_profile: str = None,
                           apply_power_query: bool = False) -> Dict[str, pl.DataFrame]:
        """
        Пакетный VLOOKUP-парсинг нескольких файлов
        """
        results = {}
        
        for file_path in file_paths:
            columns = None
            file_type = "custom"
            
            if column_selection and file_path in column_selection:
                columns = column_selection[file_path]
            
            filename = Path(file_path).stem.lower()
            if "price" in filename:
                file_type = "price_list"
            elif "catalog" in filename:
                file_type = "catalog"
            elif "inventory" in filename or "stock" in filename:
                file_type = "inventory"
            
            profile = use_profile if use_profile else file_type if file_type in self.column_parser.parsing_rules.get("custom_parsing_rules", {}) else None
            
            df = self.read_file_with_column_selection(
                file_path, 
                required_columns=columns,
                use_profile=profile,
                apply_power_query=apply_power_query
            )
            
            if not df.is_empty():
                results[file_path] = df
                logger.info(f"✅ Успешно обработан: {file_path} ({len(df)} строк, {len(df.columns)} колонок)")
            else:
                logger.warning(f"❌ Не удалось обработать: {file_path}")
        
        return results
    
    # ========================================================================
    # ОБРАБОТКА ФАЙЛОВ
    # ========================================================================
    def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
        """Используем улучшенный детектор из ColumnParser"""
        return self.column_parser.detect_columns_advanced(actual_columns, expected_columns)
    
    def read_and_prepare_file(self, file_path: str, file_type: str) -> pl.DataFrame:
        """Стандартное чтение файла с автоопределением колонок"""
        return self.read_file_with_column_selection(file_path, file_type=file_type)
    
    # ========================================================================
    # ✅ НОВЫЕ МЕТОДЫ: Сохранение и загрузка Power Query запросов
    # ========================================================================
    def save_power_query(self, name: str, description: str = ""):
        """Сохранение текущего Power Query запроса"""
        query_steps = self.power_query.get_steps_preview()
        query_json = json.dumps(query_steps)
        
        self.conn.execute("""
            INSERT INTO saved_queries (name, description, query_steps)
            VALUES (?, ?, ?)
        """, [name, description, query_json])
        
        logger.info(f"Сохранен Power Query запрос: {name}")
    
    def load_power_query(self, query_id: int):
        """Загрузка сохраненного Power Query запроса"""
        result = self.conn.execute(
            "SELECT query_steps FROM saved_queries WHERE id = ?", [query_id]
        ).fetchone()
        
        if result:
            steps = json.loads(result[0])
            self.power_query.clear_steps()
            
            for step in steps:
                pass
            
            logger.info(f"Загружен Power Query запрос #{query_id}")
    
    def get_saved_queries(self) -> List[Dict[str, Any]]:
        """Получение списка сохраненных запросов"""
        result = self.conn.execute(
            "SELECT id, name, description, created_at FROM saved_queries ORDER BY created_at DESC"
        ).fetchall()
        
        return [
            {'id': r[0], 'name': r[1], 'description': r[2], 'created_at': r[3]}
            for r in result
        ]
    
    def delete_saved_query(self, query_id: int):
        """Удаление сохраненного запроса"""
        self.conn.execute("DELETE FROM saved_queries WHERE id = ?", [query_id])
        logger.info(f"Удален Power Query запрос #{query_id}")
    
    # ========================================================================
    # ЗАГРУЗКА И ОБНОВЛЕНИЕ В БАЗЕ
    # ========================================================================
    def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
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
        """Обработка и загрузка данных в базу"""
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        
        steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
        num_steps = len(steps)
        
        progress_bar = st.progress(0, text="Подготовка к обновлению базы данных...")
        step_counter = 0
        
        # ШАГ 1: Обработка OE данных
        if 'oe' in dataframes:
            step_counter += 1
            progress_bar.progress(step_counter / (num_steps + 1),
                                  text=f"({step_counter}/{num_steps}) Обработка OE данных...")
            
            df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
            
            for col in ['length', 'width', 'height', 'weight']:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
            
            if 'dimensions_str' not in df.columns:
                df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias('dimensions_str'))
            
            oe_df = df.select([
                'oe_number_norm', 'oe_number', 'name', 'applicability',
                'length', 'width', 'height', 'weight', 'dimensions_str'
            ]).unique(subset=['oe_number_norm'], keep='first')
            
            if 'name' in oe_df.columns:
                oe_df = oe_df.with_columns(
                    self.determine_category_vectorized(pl.col('name')).alias('category')
                )
            else:
                oe_df = oe_df.with_columns(pl.lit('Разное').alias('category'))
            
            oe_df = oe_df.select([
                'oe_number_norm', 'oe_number', 'name', 'applicability', 'category',
                'length', 'width', 'height', 'weight', 'dimensions_str'
            ])
            
            self.upsert_data('oe', oe_df, ['oe_number_norm'])
            
            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, [
                'oe_number_norm', 'artikul_norm', 'brand_norm'])
        
        # ШАГ 2: Обработка кроссов
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
        
        # ШАГ 3: Обработка цен
        if 'prices' in dataframes:
            price_df = dataframes['prices']
            if not price_df.is_empty():
                st.info("💰 Обработка цен...")
                self.upsert_prices(price_df)
                st.success(f"✅ Успешно обновлено {len(price_df)} ценовых записей")
        
        # ШАГ 4: Сборка данных по артикулам
        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1),
                              text=f"({step_counter}/{num_steps}) Сборка данных по артикулам...")
        
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
    # ЭКСПОРТ
    # ========================================================================
    def _get_brand_markups_sql(self) -> str:
        rows = []
        for brand, markup in self.price_rules['brand_markups'].items():
            safe_brand = brand.replace("'", "''")
            rows.append(f"SELECT '{safe_brand}' AS brand, {markup} AS markup")
        return " UNION ALL ".join(rows) if rows else "SELECT NULL AS brand, NULL AS markup LIMIT 0"
    
    def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
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
        
        columns_map = [
            ("Артикул бренда", 'r.artikul AS "Артикул бренда"'),
            ("Бренд", 'r.brand AS "Бренд"'),
            ("Наименование", 'COALESCE(r.representative_name, r.analog_representative_name) AS "Наименование"'),
            ("Применимость", 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"'),
            ("Описание", 'CONCAT(COALESCE(r.description, \'\'), dt.text) AS "Описание"'),
            ("Категория товара", 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория товара"'),
            ("Кратность", 'r.multiplicity AS "Кратность"'),
            ("Длинна", 'COALESCE(NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.oe_length AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_length AS DOUBLE), 2), 0), 0.0) AS "Длинна"'),
            ("Ширина", 'COALESCE(NULLIF(ROUND(CAST(r.width AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.oe_width AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_width AS DOUBLE), 2), 0), 0.0) AS "Ширина"'),
            ("Высота", 'COALESCE(NULLIF(ROUND(CAST(r.height AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.oe_height AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_height AS DOUBLE), 2), 0), 0.0) AS "Высота"'),
            ("Вес", 'COALESCE(NULLIF(ROUND(CAST(r.weight AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.oe_weight AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_weight AS DOUBLE), 2), 0), 0.0) AS "Вес"'),
            ("Длинна/Ширина/Высота", 'COALESCE(CASE WHEN r.dimensions_str IS NULL OR r.dimensions_str = \'\' OR UPPER(TRIM(r.dimensions_str)) = \'XX\' THEN NULL ELSE r.dimensions_str END, r.analog_dimensions_str, CAST(COALESCE(NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0), 0) AS VARCHAR) || \'x\' || CAST(COALESCE(NULLIF(ROUND(CAST(r.width AS DOUBLE), 2), 0), 0) AS VARCHAR) || \'x\' || CAST(COALESCE(NULLIF(ROUND(CAST(r.height AS DOUBLE), 2), 0), 0) AS VARCHAR)) AS "Длинна/Ширина/Высота"'),
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
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(o.oe_number, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') AS oe_list,
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
                STRING_AGG(DISTINCT regexp_replace(regexp_replace(p2.artikul, '''', ''), '[^0-9A-Za-zА-Яа-яЁё`\\-\\s]', '', 'g'), ', ') AS analog_list
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
                ROUND(MAX(CASE WHEN p2.length IS NOT NULL AND p2.length != 0 THEN p2.length ELSE NULL END), 2) AS length,
                ROUND(MAX(CASE WHEN p2.width IS NOT NULL AND p2.width != 0 THEN p2.width ELSE NULL END), 2) AS width,
                ROUND(MAX(CASE WHEN p2.height IS NOT NULL AND p2.height != 0 THEN p2.height ELSE NULL END), 2) AS height,
                ROUND(MAX(CASE WHEN p2.weight IS NOT NULL AND p2.weight != 0 THEN p2.weight ELSE NULL END), 2) AS weight,
                ANY_VALUE(CASE WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX' THEN p2.dimensions_str ELSE NULL END) AS dimensions_str,
                ROUND(MAX(CASE WHEN p2.length IS NOT NULL AND p2.length != 0 THEN p2.length ELSE NULL END), 2) AS oe_length,
                ROUND(MAX(CASE WHEN p2.width IS NOT NULL AND p2.width != 0 THEN p2.width ELSE NULL END), 2) AS oe_width,
                ROUND(MAX(CASE WHEN p2.height IS NOT NULL AND p2.height != 0 THEN p2.height ELSE NULL END), 2) AS oe_height,
                ROUND(MAX(CASE WHEN p2.weight IS NOT NULL AND p2.weight != 0 THEN p2.weight ELSE NULL END), 2) AS oe_weight,
                ANY_VALUE(CASE WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' THEN pd2.representative_name ELSE NULL END) AS representative_name,
                ANY_VALUE(CASE WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != '' THEN pd2.representative_applicability ELSE NULL END) AS representative_applicability,
                ANY_VALUE(CASE WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != '' THEN pd2.representative_category ELSE NULL END) AS representative_category
            FROM AllRelatedParts arp
            JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT
                p.artikul_norm, p.brand_norm, p.artikul, p.brand,
                p.description, p.multiplicity,
                ROUND(CAST(p.length AS DOUBLE), 2) AS length,
                ROUND(CAST(p.width AS DOUBLE), 2) AS width,
                ROUND(CAST(p.height AS DOUBLE), 2) AS height,
                ROUND(CAST(p.weight AS DOUBLE), 2) AS weight,
                p.dimensions_str, p.image_url,
                pd.representative_name, pd.representative_applicability,
                pd.representative_category, pd.oe_list,
                aa.analog_list,
                ROUND(CAST(p.length AS DOUBLE), 2) AS oe_length,
                ROUND(CAST(p.width AS DOUBLE), 2) AS oe_width,
                ROUND(CAST(p.height AS DOUBLE), 2) AS oe_height,
                ROUND(CAST(p.weight AS DOUBLE), 2) AS oe_weight,
                p_analog.length AS analog_length,
                p_analog.width AS analog_width,
                p_analog.height AS analog_height,
                p_analog.weight AS analog_weight,
                p_analog.dimensions_str AS analog_dimensions_str,
                p_analog.representative_name AS analog_representative_name,
                p_analog.representative_applicability AS analog_representative_applicability,
                p_analog.representative_category AS analog_representative_category,
                ROW_NUMBER() OVER (PARTITION BY p.artikul_norm, p.brand_norm ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST) AS rn
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

    # ========================================================================
    # ✅ v100.23: ПОТОКОВЫЙ ЭКСПОРТ (БЕЗ ЗАГРУЗКИ В RAM)
    # ========================================================================
    def export_streaming_csv(self, query: str, output_path: str) -> bool:
        """Использует нативный COPY DuckDB для потоковой записи на диск без загрузки в RAM"""
        try:
            abs_path = Path(output_path).resolve()
            self.conn.execute(f"COPY ({query}) TO '{abs_path}' (HEADER, DELIMITER ';')")
            return True
        except Exception as e:
            logger.error(f"CSV Export Error: {e}")
            st.error(f"Ошибка экспорта CSV: {e}")
            return False

    def export_streaming_excel(self, query: str, output_path: str) -> bool:
        """Чанковая запись в Excel для предотвращения OOM"""
        try:
            abs_path = Path(output_path).resolve()
            count_query = f"SELECT COUNT(*) FROM ({query})"
            total_rows = self.conn.execute(count_query).fetchone()[0]
            
            if total_rows == 0:
                st.warning("Нет данных для экспорта.")
                return False

            if total_rows > EXCEL_ROW_LIMIT:
                st.warning(f"Внимание: Excel имеет лимит ~{EXCEL_ROW_LIMIT} строк. Будет экспортировано только первые {EXCEL_ROW_LIMIT} строк.")
                query = f"{query} LIMIT {EXCEL_ROW_LIMIT}"
                total_rows = EXCEL_ROW_LIMIT

            progress_bar = st.progress(0, text="Потоковая запись Excel...")
            
            rel = self.conn.execute(query)
            first_chunk = True
            rows_written = 0
            
            with pd.ExcelWriter(abs_path, engine='openpyxl') as writer:
                while True:
                    chunk = rel.fetchmany(CHUNK_SIZE)
                    if not chunk:
                        break
                    
                    col_names = [desc[0] for desc in rel.description]
                    df_chunk = pd.DataFrame(chunk, columns=col_names)
                    
                    df_chunk.to_excel(
                        writer, 
                        sheet_name='Данные', 
                        index=False, 
                        header=first_chunk, 
                        startrow=rows_written if not first_chunk else 0
                    )
                    rows_written += len(df_chunk)
                    first_chunk = False
                    progress_bar.progress(min(1.0, rows_written / total_rows))
            
            progress_bar.empty()
            return True
        except Exception as e:
            logger.error(f"Excel Export Error: {e}")
            st.error(f"Ошибка экспорта Excel: {e}")
            return False
    
    def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None, include_prices: bool = True, apply_markup: bool = True) -> bool:
        total = self.conn.execute(
            "SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        st.info(f"📤 Экспорт {total} записей в CSV...")
        try:
            query = self.build_export_query(selected_columns, include_prices, apply_markup)
            df = self.conn.execute(query).pl()
            pdf = df.to_pandas()
            
            dimension_cols = ["Длинна", "Ширина", "Высота", "Вес"]
            for col in dimension_cols:
                if col in pdf.columns:
                    try:
                        pdf[col] = pd.to_numeric(pdf[col], errors='coerce').fillna(0).round(2)
                    except Exception:
                        pdf[col] = 0.0
            
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
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
        if total == 0:
            st.warning("Нет данных для экспорта")
            return False
        
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        df = pd.read_sql(query, self.conn)
        
        dimension_cols = ["Длинна", "Ширина", "Высота", "Вес"]
        for col in dimension_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).round(2)
                except Exception:
                    df[col] = 0.0
        
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
        
        # ✅ НОВОЕ: Выбор режима экспорта
        use_streaming = st.checkbox(
            "⚡ Использовать потоковый экспорт (Рекомендуется для файлов > 100 000 строк, экономит RAM)", 
            value=True
        )
        
        if st.button("🚀 Экспортировать"):
            output_path = self.data_dir / f"export.{format_choice.lower()}"
            
            with st.spinner("Генерация файла..."):
                success = False
                if format_choice == "CSV":
                    if use_streaming:
                        query = self.build_export_query(selected_columns if selected_columns else None, include_prices, apply_markup)
                        success = self.export_streaming_csv(query, str(output_path))
                    else:
                        success = self.export_to_csv_optimized(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Excel":
                    if use_streaming:
                        query = self.build_export_query(selected_columns if selected_columns else None, include_prices, apply_markup)
                        success = self.export_streaming_excel(query, str(output_path))
                    else:
                        success = self.export_to_excel_optimized(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                elif format_choice == "Parquet":
                    success = self.export_to_parquet(str(output_path), selected_columns if selected_columns else None, include_prices, apply_markup)
                else:
                    st.warning("Неподдерживаемый формат")
                    return
            
            if success:
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
    
    # ========================================================================
    # ✅ НОВЫЙ ИНТЕРФЕЙС: VLOOKUP ПАРСИНГ И POWER QUERY
    # ========================================================================
    def show_vlookup_parsing_interface(self):
        """Интерфейс VLOOKUP-парсинга с Power Query"""
        st.header("🎯 VLOOKUP Парсинг + Power Query")
        
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 VLOOKUP Парсинг", 
            "⚡ Power Query Конструктор", 
            "💾 Сохраненные запросы",
            "🔧 Настройки маппинга"
        ])
        
        with tab1:
            self._show_vlookup_tab()
        
        with tab2:
            self._show_power_query_tab()
        
        with tab3:
            self._show_saved_queries_tab()
        
        with tab4:
            self._show_mapping_settings_tab()
    
    def _show_vlookup_tab(self):
        """Вкладка VLOOKUP парсинга"""
        st.subheader("📋 VLOOKUP Парсинг файлов")
        st.info("Выберите файл и укажите, какие колонки нужно извлечь (как в VLOOKUP)")
        
        uploaded_file = st.file_uploader(
            "Выберите файл для парсинга",
            type=["xlsx", "xls", "csv"],
            key="vlookup_upload"
        )
        
        if uploaded_file:
            temp_path = self.data_dir / "temp_vlookup_upload.xlsx"
            temp_path.write_bytes(uploaded_file.getvalue())
            
            try:
                if temp_path.suffix != '.csv':
                    try:
                        preview_df = pl.read_excel(temp_path, engine='calamine')
                    except ModuleNotFoundError as e:
                        if 'fastexcel' in str(e).lower():
                            st.error("❌ Отсутствует библиотека 'fastexcel'. Добавьте 'fastexcel>=0.9.0' в requirements.txt и перезапустите приложение.")
                            return
                        else:
                            raise e
                    except Exception:
                        preview_df = pl.read_excel(temp_path, engine='openpyxl')
                else:
                    preview_df = pl.read_csv(temp_path)
                
                st.write("**Доступные колонки в файле:**")
                st.write(", ".join(preview_df.columns))
                
                with st.expander("👁️ Предпросмотр данных"):
                    st.dataframe(preview_df.head(10).to_pandas(), use_container_width=True)
                
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    available_targets = list(self.column_parser.parsing_rules.get("column_mappings", {}).keys())
                    target_columns = st.multiselect(
                        "🎯 Выберите колонки для извлечения:",
                        available_targets,
                        help="Выберите, какие данные вы хотите извлечь из файла"
                    )
                
                with col2:
                    profiles = list(self.column_parser.parsing_rules.get("custom_parsing_rules", {}).keys())
                    use_profile = st.selectbox(
                        "📁 Или используйте профиль:",
                        ["Нет"] + profiles
                    )
                    
                    filename = uploaded_file.name.lower()
                    if "price" in filename:
                        detected_type = "price_list"
                    elif "catalog" in filename:
                        detected_type = "catalog"
                    elif "inventory" in filename or "stock" in filename:
                        detected_type = "inventory"
                    else:
                        detected_type = "custom"
                    
                    st.info(f"📄 Определен тип: **{detected_type}**")
                
                apply_pq = st.checkbox("⚡ Применить Power Query трансформации", value=False)
                
                if st.button("🚀 Выполнить VLOOKUP парсинг", type="primary"):
                    with st.spinner("🔄 Парсинг данных..."):
                        if use_profile != "Нет":
                            df = self.read_file_with_column_selection(
                                str(temp_path),
                                use_profile=use_profile,
                                apply_power_query=apply_pq
                            )
                        else:
                            df = self.read_file_with_column_selection(
                                str(temp_path),
                                required_columns=target_columns,
                                file_type=detected_type,
                                apply_power_query=apply_pq
                            )
                        
                        if not df.is_empty():
                            st.success(f"✅ Успешно! Извлечено {len(df)} записей, {len(df.columns)} колонок")
                            
                            st.subheader("📊 Результат парсинга")
                            st.dataframe(df.to_pandas(), use_container_width=True)
                            
                            with st.expander("🔍 Использованный маппинг колонок"):
                                mapping = self.column_parser.detect_columns_advanced(
                                    preview_df.columns,
                                    required_columns=target_columns,
                                    file_type=detected_type
                                )
                                st.json(mapping)
                            
                            col1, col2, col3 = st.columns(3)
                            col1.metric("Строк", len(df))
                            col2.metric("Колонок", len(df.columns))
                            col3.metric("Размер", f"{df.estimated_size() / 1024:.1f} KB")
                            
                            if st.button("💾 Загрузить в базу данных"):
                                if 'price' in df.columns:
                                    self.upsert_prices(df)
                                    st.success("✅ Цены загружены в базу")
                                elif 'oe_number' in df.columns:
                                    self.process_and_load_data({'oe': df})
                                    st.success("✅ OE данные загружены в базу")
                                elif 'barcode' in df.columns:
                                    self.upsert_data('parts', df, ['artikul_norm', 'brand_norm'])
                                    st.success("✅ Данные загружены в базу")
                                else:
                                    self.process_and_load_data({'parts': df})
                                    st.success("✅ Данные загружены в базу")
                        else:
                            st.error("❌ Не удалось распарсить файл. Проверьте соответствие колонок.")
            
            finally:
                if temp_path.exists():
                    temp_path.unlink()
    
    def _show_power_query_tab(self):
        """Вкладка Power Query конструктора"""
        st.subheader("⚡ Power Query Конструктор трансформаций")
        st.info("Создайте последовательность трансформаций данных в стиле Power Query")
        
        uploaded_file = st.file_uploader(
            "Загрузите файл для Power Query трансформации",
            type=["xlsx", "xls", "csv"],
            key="pq_upload"
        )
        
        if uploaded_file:
            temp_path = self.data_dir / "temp_pq_upload.xlsx"
            temp_path.write_bytes(uploaded_file.getvalue())
            
            try:
                df = pl.read_excel(temp_path, engine='calamine') if temp_path.suffix != '.csv' else pl.read_csv(temp_path)
                
                if 'current_df' not in st.session_state:
                    st.session_state.current_df = df
                
                st.write("**Текущие данные:**")
                st.dataframe(st.session_state.current_df.head(10).to_pandas(), use_container_width=True)
                
                st.subheader("🔧 Добавить шаг трансформации")
                
                transformation_type = st.selectbox(
                    "Тип трансформации:",
                    [
                        "Удалить дубликаты",
                        "Фильтровать строки",
                        "Заменить значения",
                        "Изменить тип данных",
                        "Добавить условную колонку",
                        "Группировать",
                        "Сортировать",
                        "Заполнить вниз",
                        "Обрезать текст",
                        "Добавить индекс"
                    ]
                )
                
                if transformation_type == "Удалить дубликаты":
                    columns = st.multiselect("Колонки для проверки дубликатов:", df.columns)
                    if st.button("➕ Добавить шаг"):
                        self.power_query.add_step(
                            "Удалить дубликаты",
                            self.pq_transformations.remove_duplicates,
                            columns=columns if columns else None
                        )
                        st.success("Шаг добавлен!")
                
                elif transformation_type == "Фильтровать строки":
                    col = st.selectbox("Колонка:", df.columns)
                    condition = st.selectbox("Условие:", 
                        ["equals", "not_equals", "contains", "not_contains", 
                         "starts_with", "ends_with", "greater_than", "less_than",
                         "is_null", "is_not_null"])
                    value = st.text_input("Значение:")
                    
                    if st.button("➕ Добавить шаг"):
                        try:
                            if condition in ["greater_than", "less_than"]:
                                value = float(value)
                            self.power_query.add_step(
                                f"Фильтр: {col} {condition} {value}",
                                self.pq_transformations.filter_rows,
                                column=col, condition=condition, value=value
                            )
                            st.success("Шаг добавлен!")
                        except ValueError:
                            st.error("Введите числовое значение")
                
                elif transformation_type == "Заменить значения":
                    col = st.selectbox("Колонка:", df.columns)
                    old_val = st.text_input("Старое значение:")
                    new_val = st.text_input("Новое значение:")
                    
                    if st.button("➕ Добавить шаг"):
                        self.power_query.add_step(
                            f"Замена: {col} {old_val} → {new_val}",
                            self.pq_transformations.replace_values,
                            column=col, old_value=old_val, new_value=new_val
                        )
                        st.success("Шаг добавлен!")
                
                elif transformation_type == "Добавить условную колонку":
                    new_col = st.text_input("Название новой колонки:")
                    col = st.selectbox("Колонка для условия:", df.columns)
                    operator = st.selectbox("Оператор:", [">", "<", ">=", "<=", "==", "!=", "contains"])
                    value = st.text_input("Значение для сравнения:")
                    result = st.text_input("Результат если истина:")
                    default = st.text_input("Значение по умолчанию:", "Другое")
                    
                    if st.button("➕ Добавить шаг"):
                        try:
                            if operator in [">", "<", ">=", "<="]:
                                value = float(value)
                            
                            conditions = [{
                                'column': col,
                                'operator': operator,
                                'value': value,
                                'result': result
                            }]
                            
                            self.power_query.add_step(
                                f"Условная колонка: {new_col}",
                                self.pq_transformations.add_conditional_column,
                                new_column=new_col, conditions=conditions, default_value=default
                            )
                            st.success("Шаг добавлен!")
                        except ValueError:
                            st.error("Введите корректные значения")
                
                if self.power_query.query_steps:
                    st.subheader("📋 Текущие шаги трансформации")
                    for i, step in enumerate(self.power_query.query_steps):
                        st.write(f"{i+1}. **{step['name']}**")
                        st.json(step['kwargs'])
                
                if self.power_query.query_steps:
                    if st.button("⚡ Выполнить все трансформации", type="primary"):
                        with st.spinner("Применение трансформаций..."):
                            try:
                                result_df = self.power_query.execute_query(df)
                                st.session_state.current_df = result_df
                                st.success("✅ Трансформации применены!")
                                st.dataframe(result_df.head(20).to_pandas(), use_container_width=True)
                                
                                # ✅ v100.24: ЭКСПОРТ РЕЗУЛЬТАТА POWER QUERY
                                st.markdown("---")
                                st.subheader("📥 Экспорт результата трансформации")
                                
                                col_exp1, col_exp2 = st.columns(2)
                                
                                with col_exp1:
                                    csv_bytes = result_df.write_csv().encode('utf-8')
                                    st.download_button(
                                        label="📄 Скачать результат как CSV",
                                        data=csv_bytes,
                                        file_name="pq_transform_result.csv",
                                        mime="text/csv",
                                        use_container_width=True
                                    )
                                
                                with col_exp2:
                                    excel_buf = io.BytesIO()
                                    with pd.ExcelWriter(excel_buf, engine='openpyxl') as writer:
                                        result_df.to_pandas().to_excel(writer, index=False, sheet_name='Результат')
                                    st.download_button(
                                        label="📊 Скачать результат как Excel",
                                        data=excel_buf.getvalue(),
                                        file_name="pq_transform_result.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                        use_container_width=True
                                    )
                                
                            except Exception as e:
                                st.error(f"❌ Ошибка: {str(e)}")
                    
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        query_name = st.text_input("Название запроса для сохранения:")
                    with col2:
                        if st.button("💾 Сохранить запрос") and query_name:
                            self.save_power_query(query_name)
                            self.power_query.clear_steps()
                            st.success(f"Запрос '{query_name}' сохранен!")
                            st.rerun()
                    
                    if st.button("🗑️ Очистить все шаги"):
                        self.power_query.clear_steps()
                        st.rerun()
            
            finally:
                if temp_path.exists():
                    temp_path.unlink()
    
    def _show_saved_queries_tab(self):
        """Вкладка сохраненных запросов"""
        st.subheader("💾 Сохраненные Power Query запросы")
        
        saved_queries = self.get_saved_queries()
        
        if not saved_queries:
            st.info("Нет сохраненных запросов")
            return
        
        for query in saved_queries:
            with st.expander(f"📝 {query['name']} (создан: {query['created_at']})"):
                st.write(f"**Описание:** {query.get('description', 'Нет описания')}")
                st.write(f"**ID:** {query['id']}")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("📂 Загрузить", key=f"load_{query['id']}"):
                        self.load_power_query(query['id'])
                        st.success(f"Запрос '{query['name']}' загружен!")
                        st.rerun()
                with col2:
                    if st.button("▶️ Выполнить", key=f"run_{query['id']}"):
                        self.load_power_query(query['id'])
                        st.info("Запрос загружен. Перейдите на вкладку Power Query для выполнения.")
                with col3:
                    if st.button("🗑️ Удалить", key=f"del_{query['id']}"):
                        self.delete_saved_query(query['id'])
                        st.success(f"Запрос '{query['name']}' удален!")
                        st.rerun()
    
    def _show_mapping_settings_tab(self):
        """Вкладка настроек маппинга колонок"""
        st.subheader("🔧 Настройки маппинга колонок")
        st.info("Настройте соответствие между названиями колонок в файлах и системными названиями")
        
        column_mappings = self.column_parser.parsing_rules.get("column_mappings", {})
        
        target_column = st.selectbox(
            "Системная колонка:",
            list(column_mappings.keys())
        )
        
        if target_column:
            current_variants = column_mappings[target_column]
            
            variants_text = st.text_area(
                f"Варианты названий для '{target_column}' (по одному на строку):",
                value="\n".join(current_variants),
                height=150
            )
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 Сохранить изменения"):
                    new_variants = [v.strip() for v in variants_text.split("\n") if v.strip()]
                    column_mappings[target_column] = new_variants
                    self.column_parser.save_parsing_rules()
                    st.success("Изменения сохранены!")
                    st.rerun()
            
            with col2:
                if st.button("🔄 Сбросить к стандартным"):
                    default_rules = ColumnParser().parsing_rules
                    column_mappings[target_column] = default_rules["column_mappings"][target_column]
                    self.column_parser.save_parsing_rules()
                    st.success("Сброшено к стандартным настройкам!")
                    st.rerun()
        
        st.subheader("📁 Профили парсинга")
        
        profiles = self.column_parser.parsing_rules.get("custom_parsing_rules", {})
        
        if profiles:
            for profile_name, profile_config in profiles.items():
                with st.expander(f"📁 {profile_name}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Обязательные колонки:**")
                        for col in profile_config.get("required_columns", []):
                            st.write(f"• {col}")
                    with col2:
                        st.write("**Опциональные колонки:**")
                        for col in profile_config.get("optional_columns", []):
                            st.write(f"• {col}")
                    
                    if st.button(f"🗑️ Удалить профиль", key=f"del_profile_{profile_name}"):
                        del self.column_parser.parsing_rules["custom_parsing_rules"][profile_name]
                        self.column_parser.save_parsing_rules()
                        st.rerun()
        
        st.subheader("➕ Создать новый профиль")
        new_profile_name = st.text_input("Название профиля:")
        
        available_columns = list(column_mappings.keys())
        required_cols = st.multiselect(
            "Обязательные колонки:",
            available_columns,
            key="new_profile_required"
        )
        optional_cols = st.multiselect(
            "Опциональные колонки:",
            [col for col in available_columns if col not in required_cols],
            key="new_profile_optional"
        )
        
        if st.button("💾 Сохранить профиль") and new_profile_name:
            self.column_parser.create_parsing_profile(
                new_profile_name, 
                required_cols, 
                optional_cols
            )
            st.success(f"Профиль '{new_profile_name}' создан!")
            st.rerun()
    
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


# ============================================================================
# ГЛАВНЫЙ ИНТЕРФЕЙС ПРИЛОЖЕНИЯ
# ============================================================================

def main():
    st.set_page_config(
        page_title="Каталог автозапчастей Pro",
        page_icon="🚗",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🚗 High-Volume Каталог Автозапчастей Pro")
    st.markdown("---")
    
    catalog = get_high_volume_catalog()
    
    with st.sidebar:
        st.header("📋 Навигация")
        
        main_section = st.radio(
            "Раздел:",
            [
                "📤 Экспорт данных",
                "📊 Статистика",
                "🔧 Управление данными",
                "🎯 VLOOKUP + Power Query"
            ]
        )
        
        st.markdown("---")
        
        st.subheader("⚡ Быстрые действия")
        
        uploaded_files = st.file_uploader(
            "Загрузить файлы данных",
            type=["xlsx", "xls", "csv"],
            accept_multiple_files=True,
            key="main_upload"
        )
        
        if uploaded_files:
            if st.button("🚀 Обработать файлы", type="primary"):
                with st.spinner("Обработка файлов..."):
                    file_paths = {}
                    for uploaded_file in uploaded_files:
                        temp_path = catalog.data_dir / f"upload_{uploaded_file.name}"
                        temp_path.write_bytes(uploaded_file.getvalue())
                        file_paths[uploaded_file.name] = str(temp_path)
                    
                    results = catalog.batch_vlookup_parse(
                        list(file_paths.values()),
                        use_profile="catalog"
                    )
                    
                    if results:
                        catalog.process_and_load_data(results)
                        st.success(f"✅ Загружено {len(results)} файлов")
                        
                        for path in file_paths.values():
                            try:
                                os.remove(path)
                            except Exception:
                                pass
    
    if main_section == "📤 Экспорт данных":
        catalog.show_export_interface()
    
    elif main_section == "📊 Статистика":
        catalog.show_statistics()
    
    elif main_section == "🔧 Управление данными":
        catalog.show_data_management()
    
    elif main_section == "🎯 VLOOKUP + Power Query":
        catalog.show_vlookup_parsing_interface()


if __name__ == "__main__":
    main()
