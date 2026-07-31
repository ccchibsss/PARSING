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
import shutil
import platform
import zipfile
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
import itertools

import streamlit as st
import duckdb
import polars as pl
import pandas as pd
import yaml
from jinja2 import Template, Environment, BaseLoader

# ============================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ============================================================================
log_dir = Path("./auto_parts_data")
log_dir.mkdir(exist_ok=True)

import logging.handlers

log_file = log_dir / "app.log"
file_handler = logging.handlers.RotatingFileHandler(
    log_file,
    maxBytes=10 * 1024 * 1024,
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
# КОНФИГУРАЦИЯ КОЛОНОК ДЛЯ РАЗНЫХ ФОРМАТОВ
# ============================================================================
class ExportColumnConfig:
    """Конфигурация колонок для разных форматов выгрузки"""
    
    # Стандартные колонки для всех форматов
    STANDARD_COLUMNS = [
        "Артикул", "Артикул (SKU)", "Артикул производителя", "Артикул товара (SKU)",
        "Название товара", "Название", "Бренд", "Производитель",
        "Описание товара", "Описание", "Категория", "Категория на Маркете",
        "Цена", "Цена со скидкой", "Зачёркнутая цена", "Себестоимость",
        "Длина, см", "Ширина, см", "Высота, см", "Вес, кг",
        "Применимость", "OE номер", "Кроссы", "Аналоги",
        "Ссылка на изображение", "Дополнительные изображения",
        "Штрихкод", "EAN", "Штрих-код",
        "Наличие", "Кратность", "Мин. партия",
        "Страна производства", "Гарантийный срок", "Срок годности",
        "Артикул Маркета", "Категория Yandex", "Категория Ozon",
        "Теги", "Ссылка на видео", "Инструкции",
        "Состояние", "Описание состояния", "Тип уценки",
        "Дата добавления", "Дата обновления"
    ]
    
    # Специфичные для Яндекс.Маркет
    YANDEX_COLUMNS = [
        "offer id", "Название", "Ссылка на изображение", "Описание",
        "Категория Yandex", "Бренд", "EAN", "Вес", "Цена",
        "Мин. партия", "Наличие", "Длина", "Ширина", "Высота",
        "Применимость", "Кроссы", "Дополнительные изображения"
    ]
    
    # Специфичные для Ozon
    OZON_COLUMNS = [
        "Артикул товара (SKU)", "Название товара", "Ссылка на изображение",
        "Описание товара", "Категория Ozon", "Бренд", "Штрихкод",
        "Вес, кг", "Длина, см", "Ширина, см", "Высота, см",
        "Цена", "Зачёркнутая цена", "Артикул производителя",
        "Страна производства", "Гарантийный срок", "Срок годности"
    ]
    
    # Специфичные для Avito
    AVITO_COLUMNS = [
        "Артикул", "Название", "Описание", "Цена",
        "Бренд", "Категория", "Состояние",
        "Длина, см", "Ширина, см", "Высота, см", "Вес, кг",
        "Ссылка на изображение", "Применимость", "Гарантийный срок"
    ]
    
    # Специфичные для СберМегаМаркет
    SBER_COLUMNS = [
        "Артикул товара (SKU)", "Название товара", "Описание товара",
        "Цена", "Бренд", "Категория на Маркете", "Ссылка на изображение",
        "Штрихкод", "Длина, см", "Ширина, см", "Высота, см", "Вес, кг",
        "Страна производства", "Гарантийный срок", "Артикул производителя"
    ]
    
    # Все колонки для пользовательского выбора
    ALL_COLUMNS = {
        "Основные": [
            "Артикул", "Артикул (SKU)", "Артикул производителя", "Артикул товара (SKU)",
            "Название товара", "Название", "Описание товара", "Описание"
        ],
        "Бренды и категории": [
            "Бренд", "Производитель", "Категория", "Категория на Маркете", 
            "Категория Yandex", "Категория Ozon"
        ],
        "Цены": [
            "Цена", "Цена со скидкой", "Зачёркнутая цена", "Себестоимость",
            "Дополнительные расходы", "Валюта"
        ],
        "Размеры": [
            "Длина, см", "Ширина, см", "Высота, см", "Вес, кг",
            "Длина", "Ширина", "Высота", "Вес"
        ],
        "Идентификация": [
            "Штрихкод", "EAN", "Штрих-код", "OE номер", "Кроссы", "Аналоги",
            "Артикул Маркета", "offer id"
        ],
        "Изображения": [
            "Ссылка на изображение", "Дополнительные изображения", "Ссылка на видео"
        ],
        "Склады и наличие": [
            "Наличие", "Кратность", "Мин. партия", "Товар доставляется в нескольких упаковках"
        ],
        "Дополнительная информация": [
            "Применимость", "Страна производства", "Гарантийный срок", "Срок годности",
            "Срок службы", "Комментарий к гарантийному сроку", "Комментарий к сроку годности",
            "Комментарий к сроку службы", "Инструкции", "Теги", "Состояние",
            "Описание состояния", "Тип уценки", "Внешний вид товара",
            "Особый тип товара", "С какого возраста пользоваться", "Товар для взрослых",
            "Цифровой товар", "Характеристики товара", "В архиве",
            "Дата дополнения карточки", "Дата добавления", "Дата обновления",
            "Номер документа на товар", "Код ТН ВЭД", "Буду маркировать"
        ]
    }

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

def get_memory_usage() -> float:
    """Получение использования памяти без psutil"""
    try:
        import resource
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == 'darwin':
            return usage / (1024 * 1024)
        else:
            return usage / 1024
    except (ImportError, AttributeError):
        pass
    
    try:
        if sys.platform == 'linux':
            with open('/proc/self/status', 'r') as f:
                for line in f:
                    if 'VmRSS' in line:
                        return float(line.split()[1]) / 1024
    except Exception:
        pass
    
    return 0.0

def memory_monitor():
    """Мониторинг использования памяти"""
    mem_mb = get_memory_usage()
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
# ДВИЖОК ШАБЛОНОВ
# ============================================================================
class TemplateEngine:
    """Движок для генерации контента по шаблонам с поддержкой Liquid-синтаксиса"""
    
    def __init__(self):
        self.env = Environment(loader=BaseLoader())
    
    def render(self, template_str: str, context: Dict[str, Any]) -> str:
        """Рендеринг шаблона с контекстом"""
        if not template_str:
            return ""
        
        try:
            template = self.env.from_string(template_str)
            return template.render(**context)
        except Exception as e:
            logger.error(f"Ошибка рендеринга шаблона: {e}")
            return template_str
    
    def get_default_name_template(self) -> str:
        """Шаблон названия по умолчанию"""
        return "{{detail_name}} Артикул - {{oem}} / Бренд - {{make_name}} / комплект - {{min_qnt}} шт."
    
    def get_default_description_template(self) -> str:
        """Шаблон описания по умолчанию"""
        return """{{detail_name}}
Характеристики:
Артикул - {{oem}}
Бренд - {{make_name}}
Комплект - {{min_qnt}} шт.
Состояние товара: новый (в упаковке)
{{product_description}}"""
    
    def build_context(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Построение контекста из строки данных"""
        context = {
            # Основные поля
            'oem': row.get('Артикул', '') or row.get('Артикул (SKU)', '') or row.get('offer id', ''),
            'source_oem': row.get('Артикул производителя', '') or row.get('Артикул товара (SKU)', ''),
            'make_name': row.get('Бренд', '') or row.get('Производитель', ''),
            'detail_name': row.get('Название товара', '') or row.get('Название', ''),
            'description': row.get('Описание товара', '') or row.get('Описание', ''),
            
            # Кроссы и коды
            'cross': row.get('Кроссы', '') or row.get('Аналоги', ''),
            'barcode': row.get('Штрихкод', '') or row.get('EAN', '') or row.get('Штрих-код', ''),
            
            # Количество
            'qnt': row.get('Наличие', 0) or row.get('Количество', 0),
            'min_qnt': row.get('Кратность', 1) or row.get('Мин. партия', 1),
            
            # Цены
            'price': row.get('Цена', 0),
            'currency': row.get('Валюта', 'RUB'),
            
            # Размеры
            'length': row.get('Длина, см', 0) or row.get('Длина', 0),
            'width': row.get('Ширина, см', 0) or row.get('Ширина', 0),
            'height': row.get('Высота, см', 0) or row.get('Высота', 0),
            'weight': row.get('Вес, кг', 0) or row.get('Вес', 0),
            
            # Изображения
            'image_url': row.get('Ссылка на изображение', ''),
            'additional_images': row.get('Дополнительные изображения', ''),
            
            # Категории
            'category': row.get('Категория', '') or row.get('Категория на Маркете', ''),
            'category_yandex': row.get('Категория Yandex', ''),
            'category_ozon': row.get('Категория Ozon', ''),
            
            # Применимость
            'applicability': row.get('Применимость', ''),
            
            # Прочее
            'manufacturer': row.get('Производитель', ''),
            'country': row.get('Страна производства', ''),
            'warranty': row.get('Гарантийный срок', ''),
            'shelf_life': row.get('Срок годности', ''),
            'service_life': row.get('Срок службы', ''),
            'sku': row.get('Артикул (SKU)', ''),
            
            # Описания для разных площадок
            'product_description': row.get('Описание товара', ''),
            'ymarket_description': row.get('Описание товара', ''),
            'ozon_description': row.get('Описание товара', ''),
            'avito_description': row.get('Описание товара', ''),
            'sbermegamarket_description': row.get('Описание товара', ''),
            'google_description': row.get('Описание товара', ''),
            'aliexpress_description': row.get('Описание товара', ''),
            
            # Свойства (1-6)
            'properties_1': row.get('Свойство 1', ''),
            'properties_2': row.get('Свойство 2', ''),
            'properties_3': row.get('Свойство 3', ''),
            'properties_4': row.get('Свойство 4', ''),
            'properties_5': row.get('Свойство 5', ''),
            'properties_6': row.get('Свойство 6', ''),
        }
        
        return context

# ============================================================================
# ОБРАБОТЧИК ЭКСПОРТА
# ============================================================================
class ExportHandler:
    """Обработчик экспорта с поддержкой всех форматов и настроек"""
    
    def __init__(self, catalog):
        self.catalog = catalog
        self.template_engine = TemplateEngine()
        self.column_config = ExportColumnConfig()
    
    def get_columns_for_format(self, format_type: str) -> List[str]:
        """Получение списка колонок для формата"""
        format_map = {
            'yandex': self.column_config.YANDEX_COLUMNS,
            'ozon': self.column_config.OZON_COLUMNS,
            'avito': self.column_config.AVITO_COLUMNS,
            'sber': self.column_config.SBER_COLUMNS,
        }
        return format_map.get(format_type, self.column_config.STANDARD_COLUMNS)
    
    @timing_decorator
    def export_data(
        self,
        output_path: str,
        selected_columns: Optional[List[str]] = None,
        include_prices: bool = True,
        apply_markup: bool = True,
        apply_exclusions: bool = True,
        format_type: str = 'csv',
        market_format: Optional[str] = None,
        template_name: Optional[str] = None,
        template_description: Optional[str] = None,
        csv_separator: str = ';',
        decimal_separator: str = '.',
        encoding: str = 'windows-1251',
        add_apostrophe: bool = True,
        use_crlf: bool = True,
        remove_semicolon: bool = False,
        split_size: int = 500000,
        zip_archive: bool = False,
        **kwargs
    ) -> bool:
        """
        Основной метод экспорта с поддержкой всех настроек
        """
        # Получение данных
        query = self.catalog.build_export_query(
            selected_columns, include_prices, apply_markup, apply_exclusions
        )
        
        total = self.catalog.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)"
        ).fetchone()[0]
        
        if total == 0:
            logger.warning("Нет данных для экспорта")
            return False
        
        logger.info(f"📤 Экспорт {total:,} записей в {format_type.upper()}...")
        
        try:
            # Получение данных чанками если нужно
            if split_size > 0 and total > split_size:
                return self._export_chunked(
                    query, output_path, total, split_size,
                    selected_columns, market_format,
                    template_name, template_description,
                    csv_separator, decimal_separator, encoding,
                    add_apostrophe, use_crlf, remove_semicolon,
                    zip_archive, format_type
                )
            else:
                # Обычный экспорт
                df = self.catalog.conn.execute(query).pl()
                
                # Применение шаблонов
                if template_name or template_description:
                    df = self._apply_templates(df, template_name, template_description)
                
                # Преобразование в нужный формат
                if market_format:
                    df = self._format_for_marketplace(df, market_format)
                
                # Применение настроек форматирования
                df = self._apply_formatting(
                    df, csv_separator, decimal_separator,
                    add_apostrophe, remove_semicolon
                )
                
                # Сохранение
                if format_type == 'csv':
                    return self._save_csv(df, output_path, csv_separator, encoding, use_crlf)
                elif format_type == 'xlsx':
                    return self._save_excel(df, output_path)
                elif format_type == 'json':
                    return self._save_json(df, output_path)
                else:
                    return self._save_csv(df, output_path, csv_separator, encoding, use_crlf)
        
        except Exception as e:
            logger.exception("Ошибка экспорта")
            return False
    
    def _apply_templates(self, df: pl.DataFrame, 
                        name_template: Optional[str],
                        desc_template: Optional[str]) -> pl.DataFrame:
        """Применение шаблонов к данным"""
        if not name_template and not desc_template:
            return df
        
        # Конвертация в список словарей для рендеринга
        rows = df.rows(named=True)
        processed_rows = []
        
        for row in rows:
            context = self.template_engine.build_context(row)
            
            if name_template:
                row['Сгенерированное название'] = self.template_engine.render(name_template, context)
            
            if desc_template:
                row['Сгенерированное описание'] = self.template_engine.render(desc_template, context)
            
            processed_rows.append(row)
        
        return pl.DataFrame(processed_rows)
    
    def _format_for_marketplace(self, df: pl.DataFrame, market_format: str) -> pl.DataFrame:
        """Форматирование данных для конкретного маркетплейса"""
        rows = df.rows(named=True)
        formatted_rows = []
        
        for row in rows:
            formatted = self._format_row_for_marketplace(row, market_format)
            formatted_rows.append(formatted)
        
        return pl.DataFrame(formatted_rows)
    
    def _format_row_for_marketplace(self, row: Dict[str, Any], market_format: str) -> Dict[str, Any]:
        """Форматирование одной строки для маркетплейса"""
        if market_format == 'yandex':
            return {
                'offer id': row.get('Артикул', ''),
                'Название': row.get('Сгенерированное название', row.get('Название товара', '')),
                'Ссылка на изображение': row.get('Ссылка на изображение', ''),
                'Описание': row.get('Сгенерированное описание', row.get('Описание товара', '')),
                'Категория Yandex': row.get('Категория Yandex', row.get('Категория', '')),
                'Бренд': row.get('Бренд', ''),
                'EAN': row.get('Штрихкод', ''),
                'Вес': row.get('Вес, кг', 0),
                'Цена': row.get('Цена', 0),
                'Мин. партия': row.get('Кратность', 1),
                'Наличие': row.get('Наличие', 0),
                'Длина': row.get('Длина, см', 0),
                'Ширина': row.get('Ширина, см', 0),
                'Высота': row.get('Высота, см', 0),
                'Применимость': row.get('Применимость', ''),
                'Кроссы': row.get('Кроссы', ''),
                'Дополнительные изображения': row.get('Дополнительные изображения', '')
            }
        
        elif market_format == 'ozon':
            return {
                'Артикул товара (SKU)': row.get('Артикул (SKU)', ''),
                'Название товара': row.get('Сгенерированное название', row.get('Название товара', '')),
                'Ссылка на изображение': row.get('Ссылка на изображение', ''),
                'Описание товара': row.get('Сгенерированное описание', row.get('Описание товара', '')),
                'Категория Ozon': row.get('Категория Ozon', row.get('Категория', '')),
                'Бренд': row.get('Бренд', ''),
                'Штрихкод': row.get('Штрихкод', ''),
                'Вес, кг': row.get('Вес, кг', 0),
                'Длина, см': row.get('Длина, см', 0),
                'Ширина, см': row.get('Ширина, см', 0),
                'Высота, см': row.get('Высота, см', 0),
                'Цена': row.get('Цена', 0),
                'Зачёркнутая цена': row.get('Зачёркнутая цена', 0),
                'Артикул производителя': row.get('Артикул производителя', ''),
                'Страна производства': row.get('Страна производства', ''),
                'Гарантийный срок': row.get('Гарантийный срок', ''),
                'Срок годности': row.get('Срок годности', '')
            }
        
        elif market_format == 'avito':
            return {
                'Артикул': row.get('Артикул', ''),
                'Название': row.get('Сгенерированное название', row.get('Название товара', '')),
                'Описание': row.get('Сгенерированное описание', row.get('Описание товара', '')),
                'Цена': row.get('Цена', 0),
                'Бренд': row.get('Бренд', ''),
                'Категория': row.get('Категория', ''),
                'Состояние': row.get('Состояние', 'Новый'),
                'Длина, см': row.get('Длина, см', 0),
                'Ширина, см': row.get('Ширина, см', 0),
                'Высота, см': row.get('Высота, см', 0),
                'Вес, кг': row.get('Вес, кг', 0),
                'Ссылка на изображение': row.get('Ссылка на изображение', ''),
                'Применимость': row.get('Применимость', ''),
                'Гарантийный срок': row.get('Гарантийный срок', '')
            }
        
        elif market_format == 'sber':
            return {
                'Артикул товара (SKU)': row.get('Артикул (SKU)', ''),
                'Название товара': row.get('Сгенерированное название', row.get('Название товара', '')),
                'Описание товара': row.get('Сгенерированное описание', row.get('Описание товара', '')),
                'Цена': row.get('Цена', 0),
                'Бренд': row.get('Бренд', ''),
                'Категория на Маркете': row.get('Категория', ''),
                'Ссылка на изображение': row.get('Ссылка на изображение', ''),
                'Штрихкод': row.get('Штрихкод', ''),
                'Длина, см': row.get('Длина, см', 0),
                'Ширина, см': row.get('Ширина, см', 0),
                'Высота, см': row.get('Высота, см', 0),
                'Вес, кг': row.get('Вес, кг', 0),
                'Страна производства': row.get('Страна производства', ''),
                'Гарантийный срок': row.get('Гарантийный срок', ''),
                'Артикул производителя': row.get('Артикул производителя', '')
            }
        
        else:
            return row
    
    def _apply_formatting(
        self,
        df: pl.DataFrame,
        csv_separator: str,
        decimal_separator: str,
        add_apostrophe: bool,
        remove_semicolon: bool
    ) -> pl.DataFrame:
        """Применение форматирования к данным"""
        # Конвертация в Pandas для более гибкой работы
        pdf = df.to_pandas()
        
        # Обработка колонок с артикулами
        artikel_cols = ['Артикул', 'Артикул (SKU)', 'Артикул производителя', 'Артикул товара (SKU)', 'offer id']
        for col in artikel_cols:
            if col in pdf.columns:
                pdf[col] = pdf[col].astype(str)
                if add_apostrophe:
                    pdf[col] = pdf[col].apply(lambda x: f"'{x}" if x and x != 'nan' else x)
                else:
                    # Добавление разделителя для номеров с E
                    pdf[col] = pdf[col].apply(self._safe_format_artikul)
        
        # Удаление точек с запятой из текста
        if remove_semicolon:
            for col in pdf.select_dtypes(include=['object']).columns:
                pdf[col] = pdf[col].astype(str).str.replace(';', ' ', regex=False)
        
        # Замена десятичного разделителя
        if decimal_separator == ',':
            for col in pdf.select_dtypes(include=['float', 'int']).columns:
                pdf[col] = pdf[col].astype(str).str.replace('.', ',')
        
        return pl.from_pandas(pdf)
    
    def _safe_format_artikul(self, value: str) -> str:
        """Безопасное форматирование артикула"""
        if not value or value == 'nan':
            return ''
        
        # Если номер содержит E и цифры, добавляем разделитель
        if re.match(r'^\d+E\d+$', value, re.IGNORECASE):
            parts = re.split(r'(E)', value, flags=re.IGNORECASE)
            if len(parts) >= 3:
                return f"{parts[0]}-{parts[1]}{parts[2]}"
        
        return value
    
    def _export_chunked(
        self,
        query: str,
        output_path: str,
        total: int,
        chunk_size: int,
        selected_columns: Optional[List[str]],
        market_format: Optional[str],
        template_name: Optional[str],
        template_description: Optional[str],
        csv_separator: str,
        decimal_separator: str,
        encoding: str,
        add_apostrophe: bool,
        use_crlf: bool,
        remove_semicolon: bool,
        zip_archive: bool,
        format_type: str
    ) -> bool:
        """Чанковый экспорт с разбиением на части"""
        output_path = Path(output_path)
        base_name = output_path.stem
        output_dir = output_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        chunks = (total // chunk_size) + 1
        files_created = []
        
        progress_bar = st.progress(0) if 'st' in globals() else None
        status_text = st.empty() if 'st' in globals() else None
        
        logger.info(f"Разбиение экспорта на {chunks} частей по {chunk_size:,} записей")
        
        for i in range(chunks):
            offset = i * chunk_size
            chunk_query = f"{query} LIMIT {chunk_size} OFFSET {offset}"
            
            # Проверка, есть ли данные в чанке
            count_query = f"SELECT COUNT(*) FROM ({chunk_query})"
            count = self.catalog.conn.execute(count_query).fetchone()[0]
            
            if count == 0:
                break
            
            df = self.catalog.conn.execute(chunk_query).pl()
            
            # Применение шаблонов
            if template_name or template_description:
                df = self._apply_templates(df, template_name, template_description)
            
            # Форматирование для маркетплейса
            if market_format:
                df = self._format_for_marketplace(df, market_format)
            
            # Применение форматирования
            df = self._apply_formatting(
                df, csv_separator, decimal_separator,
                add_apostrophe, remove_semicolon
            )
            
            # Имя файла
            part_name = f"{base_name}_part_{i+1:03d}"
            if format_type == 'csv':
                part_path = output_dir / f"{part_name}.csv"
                self._save_csv(df, str(part_path), csv_separator, encoding, use_crlf)
            elif format_type == 'xlsx':
                part_path = output_dir / f"{part_name}.xlsx"
                self._save_excel(df, str(part_path))
            elif format_type == 'json':
                part_path = output_dir / f"{part_name}.json"
                self._save_json(df, str(part_path))
            else:
                part_path = output_dir / f"{part_name}.csv"
                self._save_csv(df, str(part_path), csv_separator, encoding, use_crlf)
            
            files_created.append(part_path)
            
            # Обновление прогресса
            progress = (i + 1) / chunks
            if progress_bar:
                progress_bar.progress(progress)
            if status_text:
                status_text.text(f"📤 Экспорт чанка {i + 1}/{chunks}...")
            
            logger.info(f"✅ Чанк {i+1}/{chunks} создан: {part_path.name} ({count:,} записей)")
        
        # Если нужен архив
        if zip_archive and len(files_created) > 1:
            archive_path = output_dir / f"{base_name}.zip"
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                for file_path in files_created:
                    zipf.write(file_path, file_path.name)
                    file_path.unlink()  # Удаляем оригинальный файл
            
            logger.info(f"📦 Создан архив: {archive_path.name}")
            
            if 'st' in globals():
                st.success(f"✅ Экспорт завершен! Архив: {archive_path.name}")
            
            return True
        
        elif len(files_created) == 1:
            # Перемещаем единственный файл в выходной путь
            if str(files_created[0]) != str(output_path):
                shutil.move(str(files_created[0]), str(output_path))
            
            if 'st' in globals():
                st.success(f"✅ Экспорт завершен: {output_path.name}")
            
            return True
        
        if 'st' in globals():
            st.success(f"✅ Экспорт завершен! Создано {len(files_created)} файлов")
        
        return True
    
    def _save_csv(self, df: pl.DataFrame, output_path: str, separator: str, 
                 encoding: str, use_crlf: bool) -> bool:
        """Сохранение в CSV с правильными параметрами"""
        try:
            pdf = df.to_pandas()
            
            # Запись с BOM для Windows
            buf = io.StringIO()
            pdf.to_csv(buf, sep=separator, index=False, 
                      line_terminator='\r\n' if use_crlf else '\n')
            
            with open(output_path, "wb") as f:
                if encoding.lower() == 'windows-1251':
                    f.write(b'\xef\xbb\xbf')
                f.write(buf.getvalue().encode(encoding))
            
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"✅ CSV сохранен: {Path(output_path).name} ({size_mb:.1f} МБ)")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения CSV: {e}")
            return False
    
    def _save_excel(self, df: pl.DataFrame, output_path: str) -> bool:
        """Сохранение в Excel"""
        try:
            pdf = df.to_pandas()
            
            if len(pdf) <= EXCEL_ROW_LIMIT:
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    pdf.to_excel(writer, index=False, sheet_name='Данные')
            else:
                # Разбиение на несколько листов
                sheets = (len(pdf) // EXCEL_ROW_LIMIT) + 1
                with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                    for i in range(sheets):
                        start_idx = i * EXCEL_ROW_LIMIT
                        end_idx = min((i + 1) * EXCEL_ROW_LIMIT, len(pdf))
                        pdf.iloc[start_idx:end_idx].to_excel(
                            writer, index=False, sheet_name=f"Данные_{i+1}"
                        )
            
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"✅ Excel сохранен: {Path(output_path).name} ({size_mb:.1f} МБ)")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения Excel: {e}")
            return False
    
    def _save_json(self, df: pl.DataFrame, output_path: str) -> bool:
        """Сохранение в JSON"""
        try:
            data = df.to_pandas().to_dict(orient='records')
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            logger.info(f"✅ JSON сохранен: {Path(output_path).name} ({size_mb:.1f} МБ)")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка сохранения JSON: {e}")
            return False

# ============================================================================
# HIGH-VOLUME КАТАЛОГ АВТОЗАПЧАСТЕЙ (ОБНОВЛЕННАЯ ВЕРСИЯ)
# ============================================================================
class HighVolumeAutoPartsCatalog:
    """
    Высокопроизводительный каталог автозапчастей с поддержкой:
    - Полнотекстового поиска (FTS) с fallback
    - Многопоточной обработки файлов
    - Интеллектуального маппинга колонок
    - Расширенного управления связями
    - Кэширования запросов
    - Экспорта в форматы маркетплейсов
    - Шаблонов названий и описаний
    - Настраиваемых колонок
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
        
        # Экспорт обработчик
        self.export_handler = ExportHandler(self)
    
    def _init_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Инициализация DuckDB с оптимизациями производительности"""
        conn = duckdb.connect(database=str(self.db_path))
        
        try:
            conn.execute("SET memory_limit = '4GB'")
            conn.execute("SET threads = 4")
            conn.execute("SET enable_object_cache = true")
            
            tmp_dir = Path("./auto_parts_data/tmp")
            tmp_dir.mkdir(exist_ok=True)
            conn.execute(f"SET temp_directory = '{tmp_dir}'")
        except Exception as e:
            logger.warning(f"Не удалось применить все настройки DuckDB: {e}")
        
        logger.info("✅ DuckDB инициализирован с оптимизациями")
        return conn
    
    # ========================================================================
    # КОНФИГУРАЦИИ (СОХРАНЕНЫ ИЗ ПРЕДЫДУЩЕЙ ВЕРСИИ)
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
        config_path = self.data_dir / "column_mapping.json"
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
            },
            'dimensions': {
                'artikul': [
                    'артикул', 'article', 'sku', 'artikul', 'код товара', 'код',
                    'part number', 'номер детали'
                ],
                'brand': [
                    'бренд', 'brand', 'производитель', 'manufacturer', 'марка'
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
                'dimensions_str': [
                    'весогабариты', 'размеры', 'dimensions', 'size', 'габариты',
                    'длинна/ширина/высота', 'длина/ширина/высота', 'дхшхв',
                    'dimension', 'gabarity'
                ]
            },
            'barcode': {
                'artikul': [
                    'артикул', 'article', 'sku', 'artikul', 'код товара', 'код',
                    'part number', 'номер детали'
                ],
                'brand': [
                    'бренд', 'brand', 'производитель', 'manufacturer', 'марка'
                ],
                'barcode': [
                    'штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13', 'ean13',
                    'bar code', 'штрих код'
                ],
                'multiplicity': [
                    'кратность шт', 'кратность', 'multiplicity', 'кратность упаковки',
                    'количество в упаковке', 'упаковка', 'pack quantity', 'pack qty'
                ]
            },
            'images': {
                'artikul': [
                    'артикул', 'article', 'sku', 'artikul', 'код товара', 'код',
                    'part number', 'номер детали'
                ],
                'brand': [
                    'бренд', 'brand', 'производитель', 'manufacturer', 'марка'
                ],
                'image_url': [
                    'ссылка', 'url', 'изображение', 'image', 'картинка', 'фото',
                    'ссылка на изображение', 'image url', 'picture', 'img'
                ]
            }
        }
        
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
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
        config_path = self.data_dir / "column_mapping.json"
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(self.column_mapping_config, f, indent=2, ensure_ascii=False)
    
    def load_link_rules(self) -> Dict[str, Any]:
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
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS oe (
                oe_number_norm VARCHAR PRIMARY KEY,
                oe_number VARCHAR,
                name VARCHAR,
                applicability VARCHAR,
                category VARCHAR,
                length DOUBLE DEFAULT 0.0,
                width DOUBLE DEFAULT 0.0,
                height DOUBLE DEFAULT 0.0,
                weight DOUBLE DEFAULT 0.0,
                dimensions_str VARCHAR
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
                PRIMARY KEY (artikul_norm, brand_norm)
            )
        """)
        
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_references (
                oe_number_norm VARCHAR,
                artikul_norm VARCHAR,
                brand_norm VARCHAR,
                link_type VARCHAR DEFAULT 'direct',
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
        logger.info("✅ База данных настроена")
    
    @timing_decorator
    def create_indexes(self):
        """Создание индексов для оптимизации запросов"""
        logger.info("⚙️ Создание индексов...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_oe_number_norm ON oe(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_oe_category ON oe(category)",
            "CREATE INDEX IF NOT EXISTS idx_oe_name ON oe(name)",
            "CREATE INDEX IF NOT EXISTS idx_parts_keys ON parts(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_brand ON parts(brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_artikul ON parts(artikul_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe ON cross_references(oe_number_norm)",
            "CREATE INDEX IF NOT EXISTS idx_cross_artikul ON cross_references(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_keys ON prices(artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_prices_price ON prices(price)",
            "CREATE INDEX IF NOT EXISTS idx_cross_oe_artikul ON cross_references(oe_number_norm, artikul_norm, brand_norm)",
            "CREATE INDEX IF NOT EXISTS idx_parts_brand_artikul ON parts(brand_norm, artikul_norm)"
        ]
        
        for index_sql in indexes:
            try:
                self.conn.execute(index_sql)
            except Exception as e:
                logger.warning(f"Не удалось создать индекс: {e}")
        
        logger.info("🛠️ Индексы созданы")
    
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
            if self.db_path.exists():
                checks['db_size_mb'] = round(self.db_path.stat().st_size / (1024 * 1024), 2)
            
            tables = self.conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema='main' AND table_type='BASE TABLE'
            """).fetchall()
            
            existing_tables = {t[0] for t in tables}
            expected_tables = {'oe', 'parts', 'cross_references', 'prices', 'metadata'}
            checks['tables_ok'] = expected_tables.issubset(existing_tables)
            
            for table in expected_tables:
                if table in existing_tables:
                    try:
                        count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                        checks['total_rows'][table] = count
                    except Exception:
                        checks['total_rows'][table] = -1
            
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
            
            try:
                self.conn.execute("ANALYZE")
                logger.info("✅ ANALYZE завершен")
            except Exception:
                pass
            
            return True
        except Exception as e:
            logger.error(f"Ошибка VACUUM: {e}")
            return False
    
    # ========================================================================
    # НОРМАЛИЗАЦИЯ И ОЧИСТКА (СОХРАНЕНЫ ИЗ ПРЕДЫДУЩЕЙ ВЕРСИИ)
    # ========================================================================
    @staticmethod
    def normalize_key(series: pl.Series) -> pl.Series:
        """Улучшенная нормализация ключей с обработкой спецсимволов"""
        return (series
                .fill_null("")
                .cast(pl.Utf8)
                .str.replace_all(r"[''\"]", "")
                .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
                .str.replace_all(r"\s+", " ")
                .str.strip_chars()
                .str.to_lowercase())
    
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
        
        categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
        
        for key, category in sorted(self.category_mapping.items(), key=lambda x: len(x[0]), reverse=True):
            categorization_expr = categorization_expr.when(
                name_lower.str.contains(key.lower(), literal=True)
            ).then(pl.lit(category))
        
        categories_map = {
            'Фильтры': r'фильтр|filter|filtr|воздушный|масляный|салонный|топливный',
            'Тормозная система': r'тормоз|brake|колодк|диск тормозной|суппорт|барабан|цилиндр тормозной|шланг тормозной',
            'Подвеска и рулевое': r'амортизатор|стойк|spring|подвеск|рычаг|сайлентблок|опора|пружин|рессор|тяга|наконечник|steering|рулевой|шаровая',
            'Двигатель и выпуск': r'двигатель|engine|свеч|поршень|клапан|прокладк|ремень грм|цепь грм|глушитель|катализатор|выхлоп|exhaust|коллектор|турбин|распредвал|коленвал',
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
    # БЕЗОПАСНАЯ КОНВЕРТАЦИЯ В ЧИСЛО
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
            
            if value.lower() in ['нет', 'no', 'none', 'null', 'nan', 'inf', '-inf']:
                return 0.0
            
            if '/' in value and value.count('/') == 1:
                try:
                    num, den = value.split('/')
                    return float(num) / float(den)
                except (ValueError, ZeroDivisionError):
                    pass
            
            cleaned = re.sub(r'[^\d.,\-]', '', value)
            if not cleaned:
                return 0.0
            
            cleaned = cleaned.replace(',', '.')
            parts = cleaned.split('.')
            if len(parts) > 2:
                cleaned = parts[0] + '.' + ''.join(parts[1:])
            
            if cleaned.startswith('.') and len(cleaned) > 1:
                cleaned = '0' + cleaned
            
            if cleaned.endswith('.'):
                cleaned = cleaned[:-1]
            
            try:
                return float(cleaned)
            except ValueError:
                return 0.0
        
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
    # ОПРЕДЕЛЕНИЕ ТИПА ФАЙЛА
    # ========================================================================
    def _detect_file_type(self, columns: Set[str]) -> str:
        """Определение типа файла по набору колонок"""
        cols_lower = {c.lower() for c in columns}
        
        if 'oe' in cols_lower and ('artikul' in cols_lower or 'артикул' in cols_lower):
            return 'cross'
        
        if 'oe' in cols_lower and ('name' in cols_lower or 'наименование' in cols_lower or 'applicability' in cols_lower or 'применимость' in cols_lower):
            return 'oe'
        
        dimension_cols = {'длина', 'длина (см)', 'length', 'ширина', 'ширина (см)', 'width', 
                         'высота', 'высота (см)', 'height', 'вес', 'вес (кг)', 'weight'}
        if len(cols_lower & dimension_cols) >= 2 and ('artikul' in cols_lower or 'артикул' in cols_lower):
            return 'dimensions'
        
        barcode_cols = {'barcode', 'штрих-код', 'штрихкод', 'ean13', 'ean'}
        if len(cols_lower & barcode_cols) >= 1:
            return 'barcode'
        
        image_cols = {'image_url', 'ссылка на изображение', 'изображение', 'image', 'img'}
        if len(cols_lower & image_cols) >= 1:
            return 'images'
        
        price_cols = {'price', 'цена', 'cost'}
        if len(cols_lower & price_cols) >= 1:
            return 'prices'
        
        if ('artikul' in cols_lower or 'артикул' in cols_lower) and ('brand' in cols_lower or 'бренд' in cols_lower):
            return 'oe'
        
        return 'unknown'
    
    # ========================================================================
    # ОБРАБОТКА ФАЙЛОВ (СОХРАНЕНА ИЗ ПРЕДЫДУЩЕЙ ВЕРСИИ)
    # ========================================================================
    def detect_columns_advanced(self, actual_columns: List[str], file_type: str) -> Dict[str, str]:
        """Расширенное определение колонок"""
        if file_type not in self.column_mapping_config:
            for known_type in self.column_mapping_config:
                if known_type in file_type or file_type in known_type:
                    file_type = known_type
                    break
            else:
                if 'oe' in self.column_mapping_config:
                    file_type = 'oe'
                else:
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
                    
                    if variant_lower == actual_l:
                        score = 100
                    elif variant_lower in actual_l:
                        score = 80 + len(variant_lower) / max(len(actual_l), 1) * 20
                    elif actual_l in variant_lower:
                        score = 60 + len(actual_l) / max(len(variant_lower), 1) * 20
                    else:
                        variant_words = set(variant_lower.split())
                        actual_words = set(actual_l.split())
                        common_words = variant_words & actual_words
                        if common_words:
                            score = 40 + (len(common_words) / max(len(variant_words), len(actual_words), 1)) * 40
                    
                    if actual_l.startswith(variant_lower) or variant_lower.startswith(actual_l):
                        score += 10
                    
                    if score > best_score:
                        best_score = score
                        best_match = actual_orig
            
            if best_match and best_score > 30:
                mapping[best_match] = expected_field
                used_actual.add(best_match)
                logger.debug(f"Маппинг: {best_match} -> {expected_field} (score: {best_score:.0f})")
        
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
            
            file_path_obj = Path(file_path)
            file_ext = file_path_obj.suffix.lower()
            file_size_mb = file_path_obj.stat().st_size / (1024 * 1024)
            
            logger.info(f"Размер файла: {file_size_mb:.2f} МБ")
            
            if file_ext == '.csv':
                try:
                    df = pl.read_csv(
                        file_path,
                        ignore_errors=True,
                        encoding='utf-8',
                        truncate_ragged_lines=True
                    )
                except Exception as e:
                    logger.error(f"Ошибка чтения CSV: {e}")
                    return pl.DataFrame()
            
            elif file_ext in ['.xlsx', '.xls']:
                try:
                    df = pl.read_excel(file_path, engine='calamine')
                except Exception:
                    try:
                        df = pl.read_excel(file_path)
                    except Exception as e:
                        logger.error(f"Ошибка чтения Excel: {e}")
                        return pl.DataFrame()
            
            elif file_ext == '.parquet':
                try:
                    df = pl.read_parquet(file_path)
                except Exception as e:
                    logger.error(f"Ошибка чтения Parquet: {e}")
                    return pl.DataFrame()
            
            elif file_ext == '.json':
                try:
                    df = pl.read_json(file_path)
                except Exception as e:
                    logger.error(f"Ошибка чтения JSON: {e}")
                    return pl.DataFrame()
            
            else:
                logger.error(f"Неподдерживаемый формат файла: {file_ext}")
                return pl.DataFrame()
            
            if df.is_empty():
                logger.warning(f"Пустой файл: {file_path}")
                return pl.DataFrame()
            
            logger.info(f"Исходные колонки файла {file_type}: {df.columns}")
            logger.info(f"Исходные типы колонок файла {file_type}: {df.schema}")
            
        except Exception as e:
            logger.exception(f"Ошибка чтения файла {file_path}: {e}")
            return pl.DataFrame()
        
        column_mapping = self.detect_columns_advanced(df.columns, file_type)
        
        if not column_mapping:
            logger.warning(f"Не удалось определить колонки для {file_type}")
            return pl.DataFrame()
        
        logger.info(f"Маппинг колонок для {file_type}: {column_mapping}")
        
        try:
            rename_dict = {old: new for old, new in column_mapping.items() 
                          if old in df.columns and new not in df.columns}
            if rename_dict:
                df = df.rename(rename_dict)
        except Exception as e:
            logger.warning(f"Ошибка при rename: {e}")
            for old_name, new_name in column_mapping.items():
                try:
                    if old_name in df.columns and new_name not in df.columns:
                        df = df.rename({old_name: new_name})
                except Exception as e2:
                    logger.warning(f"Не удалось переименовать {old_name} -> {new_name}: {e2}")
        
        if len(df.columns) != len(set(df.columns)):
            logger.warning("Обнаружены дубликаты колонок")
            seen = set()
            cols_to_keep = []
            for col in df.columns:
                if col not in seen:
                    seen.add(col)
                    cols_to_keep.append(col)
                else:
                    logger.warning(f"Удаляем дубликат колонки: {col}")
            df = df.select(cols_to_keep)
        
        for col in ['artikul', 'brand', 'oe_number', 'name', 'applicability']:
            if col in df.columns:
                try:
                    df = df.with_columns(self.clean_values(pl.col(col)).alias(col))
                except Exception as e:
                    logger.warning(f"Ошибка очистки колонки {col}: {e}")
        
        numeric_cols = ['length', 'width', 'height', 'weight', 'price']
        for col in numeric_cols:
            if col in df.columns:
                try:
                    current_dtype = df[col].dtype
                    if current_dtype in [pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.UInt32, pl.UInt64]:
                        df = df.with_columns(
                            pl.col(col)
                            .cast(pl.Float64, strict=False)
                            .fill_null(0.0)
                            .round(2)
                            .alias(col)
                        )
                        logger.info(f"✅ Колонка '{col}' уже числовая ({current_dtype}), приведена к Float64")
                    else:
                        df = df.with_columns(
                            pl.col(col)
                            .cast(pl.Utf8)
                            .str.replace_all(r"[^\d.,\-]", "")
                            .str.replace(",", ".")
                            .cast(pl.Float64, strict=False)
                            .fill_null(0.0)
                            .round(2)
                            .alias(col)
                        )
                        logger.info(f"✅ Колонка '{col}' сконвертирована из строки в Float64")
                except Exception as e:
                    logger.warning(f"Не удалось преобразовать {col}: {e}")
                    if col not in df.columns:
                        try:
                            df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
                        except Exception:
                            pass
        
        key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
        if key_cols:
            df = df.unique(subset=key_cols, keep='first')
        
        for col in ['artikul', 'brand', 'oe_number']:
            if col in df.columns:
                try:
                    df = df.with_columns(
                        self.normalize_key(pl.col(col)).alias(f"{col}_norm")
                    )
                except Exception as e:
                    logger.warning(f"Ошибка нормализации {col}: {e}")
        
        logger.info(f"Файл {file_type} обработан. Итоговые колонки: {df.columns}")
        logger.info(f"Итоговые типы колонок файла {file_type}: {df.schema}")
        
        memory_monitor()
        
        return df
    
    def _align_dataframes_for_concat(self, dfs: List[pl.DataFrame]) -> List[pl.DataFrame]:
        """Выравнивание списка DataFrames для объединения с приведением типов"""
        if not dfs:
            return []
        
        if len(dfs) == 1:
            return dfs
        
        all_columns = set()
        for d in dfs:
            all_columns.update(d.columns)
        
        column_types = {}
        for d in dfs:
            for col in all_columns:
                if col in d.columns and col not in column_types:
                    column_types[col] = d[col].dtype
                elif col not in d.columns and col not in column_types:
                    if col in ['length', 'width', 'height', 'weight', 'price']:
                        column_types[col] = pl.Float64
                    elif col in ['multiplicity']:
                        column_types[col] = pl.Int64
                    elif col in ['oe_number', 'oe_number_norm', 'artikul', 'brand', 'artikul_norm', 'brand_norm', 'name', 'applicability', 'dimensions_str', 'image_url', 'barcode', 'description', 'category']:
                        column_types[col] = pl.Utf8
                    else:
                        column_types[col] = pl.Utf8
        
        aligned_dfs = []
        for d in dfs:
            d_aligned = d
            
            for mc in all_columns:
                if mc not in d.columns:
                    target_type = column_types.get(mc, pl.Utf8)
                    if target_type == pl.Float64:
                        d_aligned = d_aligned.with_columns(pl.lit(None).cast(pl.Float64).alias(mc))
                    elif target_type == pl.Int64:
                        d_aligned = d_aligned.with_columns(pl.lit(None).cast(pl.Int64).alias(mc))
                    else:
                        d_aligned = d_aligned.with_columns(pl.lit(None).cast(pl.Utf8).alias(mc))
            
            for col in all_columns:
                if col in d_aligned.columns:
                    target_type = column_types.get(col, pl.Utf8)
                    try:
                        if target_type == pl.Float64:
                            if d_aligned[col].dtype not in [pl.Float64, pl.Float32]:
                                d_aligned = d_aligned.with_columns(
                                    d_aligned[col].cast(pl.Float64, strict=False).fill_null(0.0)
                                )
                        elif target_type == pl.Int64:
                            if d_aligned[col].dtype not in [pl.Int64, pl.Int32]:
                                d_aligned = d_aligned.with_columns(
                                    d_aligned[col].cast(pl.Int64, strict=False).fill_null(1)
                                )
                        else:
                            if d_aligned[col].dtype not in [pl.Utf8]:
                                d_aligned = d_aligned.with_columns(
                                    d_aligned[col].cast(pl.Utf8, strict=False).fill_null("")
                                )
                    except Exception as e:
                        logger.warning(f"Не удалось привести колонку {col} к типу {target_type}: {e}")
            
            d_aligned = d_aligned.select(sorted(all_columns))
            aligned_dfs.append(d_aligned)
        
        return aligned_dfs
    
    def process_uploaded_files(self, uploaded_files_dict: Dict[str, Any]) -> Dict[str, pl.DataFrame]:
        """Обработка загруженных файлов с использованием временных файлов"""
        results = {}
        temp_dir = self.data_dir / "temp_uploads"
        temp_dir.mkdir(exist_ok=True)
        
        universal_files = uploaded_files_dict.pop('universal', [])
        other_files = uploaded_files_dict
        
        for file_type, files in other_files.items():
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
                            logger.info(f"✅ Файл '{uploaded_file.name}' успешно обработан. Строк: {len(df)}")
                        else:
                            logger.warning(f"⚠️ Файл '{uploaded_file.name}' обработан, но DataFrame пуст.")
                    except Exception as e:
                        logger.exception(f"❌ Критическая ошибка при обработке файла '{uploaded_file.name}': {e}")
                        st.error(f"❌ Ошибка обработки файла '{uploaded_file.name}': {str(e)}")
            
            if dfs_for_type:
                try:
                    aligned_dfs = self._align_dataframes_for_concat(dfs_for_type)
                    combined_df = pl.concat(aligned_dfs)
                    results[file_type] = combined_df.unique(keep='first')
                    logger.info(f"📦 Тип {file_type}: объединено {len(combined_df)} записей")
                except Exception as e:
                    logger.error(f"Ошибка объединения DataFrame для {file_type}: {e}")
                    logger.exception("Детали ошибки объединения:")
                    for i, d in enumerate(dfs_for_type):
                        logger.info(f"DataFrame {i} колонки: {d.columns}")
                        logger.info(f"DataFrame {i} типы: {d.schema}")
                    st.error(f"❌ Ошибка объединения файлов типа '{file_type}': {str(e)}")
        
        if universal_files:
            logger.info(f"🔍 Обработка {len(universal_files)} универсальных файлов...")
            
            universal_groups = {
                'oe': [],
                'cross': [],
                'prices': [],
                'dimensions': [],
                'barcode': [],
                'images': []
            }
            
            for idx, uploaded_file in enumerate(universal_files):
                logger.info(f"Анализ универсального файла {idx + 1}/{len(universal_files)}: {uploaded_file.name}")
                
                with temp_upload_file(uploaded_file) as temp_path:
                    try:
                        temp_df = self.read_and_prepare_file(str(temp_path), 'universal')
                        
                        if temp_df.is_empty():
                            logger.warning(f"⚠️ Файл '{uploaded_file.name}' пуст, пропускаем")
                            continue
                        
                        cols = set(c.lower() for c in temp_df.columns)
                        detected_type = self._detect_file_type(cols)
                        
                        logger.info(f"📌 Файл '{uploaded_file.name}' определен как тип: {detected_type}")
                        
                        if detected_type != 'unknown':
                            with temp_upload_file(uploaded_file) as temp_path2:
                                df = self.read_and_prepare_file(str(temp_path2), detected_type)
                                if not df.is_empty():
                                    universal_groups[detected_type].append(df)
                                    logger.info(f"✅ Файл '{uploaded_file.name}' добавлен в группу {detected_type}")
                                else:
                                    logger.warning(f"⚠️ Файл '{uploaded_file.name}' не удалось прочитать с типом {detected_type}")
                        else:
                            with temp_upload_file(uploaded_file) as temp_path2:
                                df = self.read_and_prepare_file(str(temp_path2), 'oe')
                                if not df.is_empty():
                                    universal_groups['oe'].append(df)
                                    logger.info(f"✅ Файл '{uploaded_file.name}' добавлен в группу oe (как fallback)")
                                else:
                                    logger.warning(f"⚠️ Не удалось определить тип файла '{uploaded_file.name}'")
                    
                    except Exception as e:
                        logger.exception(f"❌ Ошибка анализа файла '{uploaded_file.name}': {e}")
                        st.error(f"❌ Ошибка обработки файла '{uploaded_file.name}': {str(e)}")
            
            for file_type, dfs in universal_groups.items():
                if not dfs:
                    continue
                
                try:
                    aligned_dfs = self._align_dataframes_for_concat(dfs)
                    combined_df = pl.concat(aligned_dfs)
                    results[file_type] = combined_df.unique(keep='first')
                    logger.info(f"📦 Универсальная группа {file_type}: объединено {len(combined_df)} записей")
                    
                except Exception as e:
                    logger.error(f"Ошибка объединения универсальных файлов для {file_type}: {e}")
                    st.error(f"❌ Ошибка обработки универсальных файлов типа '{file_type}': {str(e)}")
        
        return results
    
    # ========================================================================
    # ЗАГРУЗКА И ОБНОВЛЕНИЕ В БАЗЕ
    # ========================================================================
    def upsert_data_batched(self, table_name: str, df: pl.DataFrame, batch_size: int = BATCH_SIZE):
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
                insert_sql = f"INSERT OR REPLACE INTO {table_name} SELECT * FROM {temp_view_name}"
                self.conn.execute(insert_sql)
                
                successful_rows += len(batch)
                
                if start_idx % (batch_size * 10) == 0 or end_idx >= total_rows:
                    progress = (end_idx / total_rows) * 100
                    logger.info(f"⏳ Прогресс {table_name}: {progress:.0f}% ({end_idx}/{total_rows})")
                
            except Exception as e:
                logger.error(f"Ошибка при UPSERT в {table_name} (строки {start_idx}-{end_idx}): {e}")
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
        
        logger.info(f"✅ UPSERT в {table_name} завершен: {successful_rows}/{total_rows} записей")
    
    @timing_decorator
    def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Улучшенная загрузка данных с индикацией прогресса"""
        if not dataframes:
            st.warning("Нет данных для загрузки")
            return
        
        st.info("🔄 Начало загрузки и обновления данных в базе...")
        
        steps = []
        if 'oe' in dataframes:
            steps.append(('oe', self._process_oe_data))
        if 'cross' in dataframes:
            steps.append(('cross', self._process_cross_data))
        if 'prices' in dataframes:
            steps.append(('prices', self._process_prices_data))
        steps.append(('parts', self._process_parts_data))
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for idx, (step_name, step_func) in enumerate(steps):
            progress = (idx + 1) / len(steps)
            progress_bar.progress(progress)
            status_text.text(f"Обработка: {step_name} ({idx + 1}/{len(steps)})...")
            
            try:
                if step_name == 'parts':
                    step_func(dataframes)
                elif step_name in dataframes:
                    step_func(dataframes[step_name])
            except Exception as e:
                logger.exception(f"Ошибка обработки {step_name}: {e}")
                st.error(f"Ошибка при обработке {step_name}: {str(e)}")
        
        progress_bar.progress(1.0)
        status_text.text("✅ Загрузка данных завершена!")
        
        st.session_state.uploaded_files = {
            k: len(v) for k, v in dataframes.items()
        }
        
        self.vacuum_database()
        
        time.sleep(1)
        progress_bar.empty()
        status_text.empty()
    
    def _process_oe_data(self, oe_df: Optional[pl.DataFrame]):
        """Обработка OE данных"""
        if oe_df is None or oe_df.is_empty():
            return
        
        df = oe_df.filter(pl.col('oe_number_norm') != "")
        
        for col in ['length', 'width', 'height', 'weight']:
            if col not in df.columns:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
        
        if 'dimensions_str' not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias('dimensions_str'))
        
        oe_cols = ['oe_number_norm', 'oe_number', 'name', 'applicability',
                   'length', 'width', 'height', 'weight', 'dimensions_str']
        
        available_cols = [c for c in oe_cols if c in df.columns]
        oe_clean = df.select(available_cols).unique(subset=['oe_number_norm'], keep='first')
        
        if 'name' in oe_clean.columns:
            oe_clean = oe_clean.with_columns(
                self.determine_category_vectorized(pl.col('name')).alias('category')
            )
        else:
            oe_clean = oe_clean.with_columns(pl.lit('Разное').alias('category'))
        
        logger.info(f"Колонки oe_df перед upsert: {oe_clean.columns}")
        logger.info(f"Количество колонок в oe_df: {len(oe_clean.columns)}")
        
        self.upsert_data_batched('oe', oe_clean)
        
        if 'artikul_norm' in df.columns and 'brand_norm' in df.columns:
            cross_from_oe = df.filter(pl.col('artikul_norm') != "").select([
                'oe_number_norm', 'artikul_norm', 'brand_norm'
            ]).unique()
            
            if not cross_from_oe.is_empty():
                cross_from_oe = cross_from_oe.with_columns(
                    pl.lit('direct').alias('link_type')
                )
                self.upsert_data_batched('cross_references', cross_from_oe)
    
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
        cross_clean = cross_clean.with_columns(
            pl.lit('direct').alias('link_type')
        )
        
        self.upsert_data_batched('cross_references', cross_clean)
    
    def _process_prices_data(self, prices_df: Optional[pl.DataFrame]):
        """Обработка цен"""
        if prices_df is None or prices_df.is_empty():
            return
        
        if 'artikul' in prices_df.columns and 'brand' in prices_df.columns:
            prices_df = prices_df.with_columns([
                self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
                self.normalize_key(pl.col('brand')).alias('brand_norm')
            ])
            
            if 'currency' not in prices_df.columns:
                prices_df = prices_df.with_columns(
                    pl.lit(self.price_rules.get('currency', 'RUB')).alias('currency')
                )
            
            prices_df = prices_df.filter(
                (pl.col('price') >= self.price_rules.get('min_price', 0.0)) &
                (pl.col('price') <= self.price_rules.get('max_price', 99999.0))
            )
            
            if self.price_rules.get('round_prices', True):
                precision = self.price_rules.get('price_precision', 2)
                prices_df = prices_df.with_columns(
                    pl.col('price').round(precision).alias('price')
                )
            
            self.upsert_data_batched('prices', prices_df)
    
    def _process_parts_data(self, dataframes: Dict[str, pl.DataFrame]):
        """Сборка и загрузка данных по артикулам"""
        parts_to_concat = []
        file_priority = ['oe', 'dimensions', 'barcode', 'images']
        
        for ftype in file_priority:
            if ftype in dataframes and not dataframes[ftype].is_empty():
                df = dataframes[ftype]
                if 'artikul_norm' in df.columns and 'brand_norm' in df.columns:
                    cols = ['artikul_norm', 'brand_norm']
                    for c in ['artikul', 'brand']:
                        if c in df.columns:
                            cols.append(c)
                    parts_to_concat.append(df.select(cols))
        
        if not parts_to_concat:
            return
        
        aligned_parts = self._align_dataframes_for_concat(parts_to_concat)
        if not aligned_parts:
            return
        
        parts_df = pl.concat(aligned_parts).filter(
            pl.col('artikul_norm') != ""
        ).unique(subset=['artikul_norm', 'brand_norm'], keep='first')
        
        if parts_df.is_empty():
            return
        
        for ftype in file_priority:
            if ftype not in dataframes or dataframes[ftype].is_empty():
                continue
            
            df = dataframes[ftype]
            if 'artikul_norm' not in df.columns:
                continue
            
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
        
        parts_df = self._format_dimensions_string(parts_df)
        parts_df = self._create_description(parts_df)
        
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
        
        self.upsert_data_batched('parts', parts_final)
    
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
    # ПОСТРОЕНИЕ ЗАПРОСА ДЛЯ ЭКСПОРТА
    # ========================================================================
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
    
    def build_export_query(self, selected_columns=None, include_prices=True, 
                          apply_markup=True, apply_exclusions=True, use_link_rules=True):
        """Построение запроса для экспорта с учетом всех настроек"""
        description_text = (
            "Состояние товара: новый (в упаковке). Высококачественные автозапчасти и автотовары — "
            "надежное решение для вашего автомобиля. Обеспечьте безопасность, долговечность и "
            "высокую производительность вашего авто с помощью нашего широкого ассортимента "
            "оригинальных и совместимых автозапчастей."
        )
        
        if use_link_rules and self.link_rules.get('use_cross_references', True):
            max_depth = self.link_rules.get('max_link_depth', 2)
            link_by_oe_only = self.link_rules.get('link_by_oe_only', False)
        else:
            max_depth = 1
            link_by_oe_only = False
        
        brand_markups_sql = self._get_brand_markups_sql()
        
        select_parts = []
        
        price_requested = include_prices and (not selected_columns or 
                                             "Цена" in selected_columns or 
                                             "Валюта" in selected_columns)
        
        if price_requested:
            if apply_markup:
                global_markup = self.price_rules.get('global_markup', 0)
                select_parts.append(
                    f"CASE WHEN pr.price IS NOT NULL THEN pr.price * (1 + COALESCE(brm.markup, {global_markup})) ELSE pr.price END AS \"Цена\""
                )
            else:
                select_parts.append('pr.price AS "Цена"')
            select_parts.append("COALESCE(pr.currency, 'RUB') AS \"Валюта\"")
        
        columns_map = {
            "Артикул": 'r.artikul AS "Артикул"',
            "Артикул (SKU)": 'r.artikul AS "Артикул (SKU)"',
            "Артикул производителя": 'r.artikul AS "Артикул производителя"',
            "Артикул товара (SKU)": 'r.artikul AS "Артикул товара (SKU)"',
            "Бренд": 'r.brand AS "Бренд"',
            "Производитель": 'r.brand AS "Производитель"',
            "Название товара": 'COALESCE(r.representative_name, r.analog_representative_name) AS "Название товара"',
            "Название": 'COALESCE(r.representative_name, r.analog_representative_name) AS "Название"',
            "Применимость": 'COALESCE(r.representative_applicability, r.analog_representative_applicability) AS "Применимость"',
            "Описание товара": 'r.description AS "Описание товара"',
            "Описание": 'r.description AS "Описание"',
            "Категория": 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория"',
            "Категория на Маркете": 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория на Маркете"',
            "Категория Yandex": 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория Yandex"',
            "Категория Ozon": 'COALESCE(r.representative_category, r.analog_representative_category) AS "Категория Ozon"',
            "Кратность": 'r.multiplicity AS "Кратность"',
            "Мин. партия": 'r.multiplicity AS "Мин. партия"',
            "Длина, см": 'COALESCE(NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_length AS DOUBLE), 2), 0), 0.0) AS "Длина, см"',
            "Длина": 'COALESCE(NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_length AS DOUBLE), 2), 0), 0.0) AS "Длина"',
            "Ширина, см": 'COALESCE(NULLIF(ROUND(CAST(r.width AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_width AS DOUBLE), 2), 0), 0.0) AS "Ширина, см"',
            "Ширина": 'COALESCE(NULLIF(ROUND(CAST(r.width AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_width AS DOUBLE), 2), 0), 0.0) AS "Ширина"',
            "Высота, см": 'COALESCE(NULLIF(ROUND(CAST(r.height AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_height AS DOUBLE), 2), 0), 0.0) AS "Высота, см"',
            "Высота": 'COALESCE(NULLIF(ROUND(CAST(r.height AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_height AS DOUBLE), 2), 0), 0.0) AS "Высота"',
            "Вес, кг": 'COALESCE(NULLIF(ROUND(CAST(r.weight AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_weight AS DOUBLE), 2), 0), 0.0) AS "Вес, кг"',
            "Вес": 'COALESCE(NULLIF(ROUND(CAST(r.weight AS DOUBLE), 2), 0), NULLIF(ROUND(CAST(r.analog_weight AS DOUBLE), 2), 0), 0.0) AS "Вес"',
            "OE номер": 'r.oe_list AS "OE номер"',
            "Кроссы": 'r.analog_list AS "Кроссы"',
            "Аналоги": 'r.analog_list AS "Аналоги"',
            "Ссылка на изображение": 'r.image_url AS "Ссылка на изображение"',
            "Дополнительные изображения": 'r.image_url AS "Дополнительные изображения"',
            "Штрихкод": 'r.barcode AS "Штрихкод"',
            "EAN": 'r.barcode AS "EAN"',
            "Штрих-код": 'r.barcode AS "Штрих-код"',
            "Наличие": '100 AS "Наличие"',
            "Состояние": 'pl.lit("Новый") AS "Состояние"',
            "Гарантийный срок": 'pl.lit("12 месяцев") AS "Гарантийный срок"',
        }
        
        for name, expr in columns_map.items():
            if not selected_columns or name in selected_columns:
                select_parts.append(expr.strip())
        
        if not select_parts:
            select_parts = ['r.artikul AS "Артикул"', 'r.brand AS "Бренд"']
        
        select_clause = ",\n".join(select_parts)
        
        exclusion_where = ""
        if apply_exclusions and self.exclusion_rules:
            exclusion_conditions = []
            for rule in self.exclusion_rules:
                safe_rule = rule.replace("'", "''")
                exclusion_conditions.append(
                    f"LOWER(COALESCE(r.representative_name, '')) NOT LIKE '%{safe_rule.lower()}%'"
                )
            
            if exclusion_conditions:
                exclusion_where = "AND " + " AND ".join(exclusion_conditions)
        
        query = f"""
        WITH DescriptionTemplate AS (
            SELECT CHR(10) || CHR(10) || $${description_text}$$ AS text
        ),
        BrandMarkups AS (
            SELECT brand, markup FROM ({brand_markups_sql}) AS tmp
        ),
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
                i.oe_number_norm AS oe_number_norm,
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
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm FROM Level1Analogs
            UNION
            SELECT source_artikul_norm, source_brand_norm, related_artikul_norm, related_brand_norm FROM Level2Analogs
        ),
        AggregatedAnalogData AS (
            SELECT
                arp.source_artikul_norm AS artikul_norm,
                arp.source_brand_norm AS brand_norm,
                ROUND(MAX(CASE WHEN p2.length IS NOT NULL AND p2.length != 0 THEN p2.length ELSE NULL END), 2) AS analog_length,
                ROUND(MAX(CASE WHEN p2.width IS NOT NULL AND p2.width != 0 THEN p2.width ELSE NULL END), 2) AS analog_width,
                ROUND(MAX(CASE WHEN p2.height IS NOT NULL AND p2.height != 0 THEN p2.height ELSE NULL END), 2) AS analog_height,
                ROUND(MAX(CASE WHEN p2.weight IS NOT NULL AND p2.weight != 0 THEN p2.weight ELSE NULL END), 2) AS analog_weight,
                ANY_VALUE(CASE WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX' THEN p2.dimensions_str ELSE NULL END) AS analog_dimensions_str,
                ANY_VALUE(CASE WHEN pd2.representative_name IS NOT NULL AND pd2.representative_name != '' THEN pd2.representative_name ELSE NULL END) AS analog_representative_name,
                ANY_VALUE(CASE WHEN pd2.representative_applicability IS NOT NULL AND pd2.representative_applicability != '' THEN pd2.representative_applicability ELSE NULL END) AS analog_representative_applicability,
                ANY_VALUE(CASE WHEN pd2.representative_category IS NOT NULL AND pd2.representative_category != '' THEN pd2.representative_category ELSE NULL END) AS analog_representative_category
            FROM AllRelatedParts arp
            JOIN parts p2 ON arp.related_artikul_norm = p2.artikul_norm AND arp.related_brand_norm = p2.brand_norm
            LEFT JOIN PartDetails pd2 ON p2.artikul_norm = pd2.artikul_norm AND p2.brand_norm = pd2.brand_norm
            GROUP BY arp.source_artikul_norm, arp.source_brand_norm
        ),
        RankedData AS (
            SELECT
                p.artikul_norm, p.brand_norm, p.artikul, p.brand, p.description, p.multiplicity,
                ROUND(CAST(p.length AS DOUBLE), 2) AS length,
                ROUND(CAST(p.width AS DOUBLE), 2) AS width,
                ROUND(CAST(p.height AS DOUBLE), 2) AS height,
                ROUND(CAST(p.weight AS DOUBLE), 2) AS weight,
                p.dimensions_str, p.image_url, p.barcode,
                pd.representative_name, pd.representative_applicability, pd.representative_category, pd.oe_list,
                aa.analog_list,
                p_analog.analog_length, p_analog.analog_width, p_analog.analog_height, p_analog.analog_weight,
                p_analog.analog_dimensions_str,
                p_analog.analog_representative_name, p_analog.analog_representative_applicability, p_analog.analog_representative_category,
                ROW_NUMBER() OVER (PARTITION BY p.artikul_norm, p.brand_norm ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST) AS rn
            FROM parts p
            LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
            LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
            LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.brand_norm = p_analog.brand_norm
        )
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
    
    # ========================================================================
    # МЕТОДЫ ЭКСПОРТА (ОБНОВЛЕННЫЕ)
    # ========================================================================
    @timing_decorator
    def export_data(
        self,
        output_path: str,
        selected_columns: Optional[List[str]] = None,
        include_prices: bool = True,
        apply_markup: bool = True,
        apply_exclusions: bool = True,
        format_type: str = 'csv',
        market_format: Optional[str] = None,
        template_name: Optional[str] = None,
        template_description: Optional[str] = None,
        csv_separator: str = ';',
        decimal_separator: str = '.',
        encoding: str = 'windows-1251',
        add_apostrophe: bool = True,
        use_crlf: bool = True,
        remove_semicolon: bool = False,
        split_size: int = 500000,
        zip_archive: bool = False
    ) -> bool:
        """
        Экспорт данных с поддержкой всех настроек
        """
        return self.export_handler.export_data(
            output_path=output_path,
            selected_columns=selected_columns,
            include_prices=include_prices,
            apply_markup=apply_markup,
            apply_exclusions=apply_exclusions,
            format_type=format_type,
            market_format=market_format,
            template_name=template_name,
            template_description=template_description,
            csv_separator=csv_separator,
            decimal_separator=decimal_separator,
            encoding=encoding,
            add_apostrophe=add_apostrophe,
            use_crlf=use_crlf,
            remove_semicolon=remove_semicolon,
            split_size=split_size,
            zip_archive=zip_archive
        )
    
    # ========================================================================
    # ПОИСК (С КЭШИРОВАНИЕМ)
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
        """Улучшенный поиск с кэшированием и поиском по аналогам"""
        if not query or not query.strip():
            return pd.DataFrame()
        
        cache_key = hashlib.md5(f"{query}:{limit}".encode()).hexdigest()
        
        if use_cache and cache_key in self._search_cache:
            cached_time, cached_result = self._search_cache[cache_key]
            if time.time() - cached_time < self._search_cache_ttl:
                self.performance_metrics['cache_hits'] += 1
                return cached_result
        
        self.performance_metrics['queries'] += 1
        
        start_time = time.time()
        
        query_norm = self.normalize_key(pl.Series([query]))[0]
        
        result = self._search_like(query, limit)
        
        if result is None or result.empty:
            logger.info(f"🔍 Поиск по аналогам для запроса: {query}")
            result = self._search_by_analog(query_norm, limit)
        
        if result is None or result.empty:
            logger.info(f"🔍 Поиск по OE номеру для запроса: {query}")
            result = self._search_by_oe_number(query_norm, limit)
        
        if result is not None and not result.empty:
            self._search_cache[cache_key] = (time.time(), result)
            self._clean_search_cache()
        
        self.performance_metrics['total_time'] += (time.time() - start_time)
        
        return result if result is not None else pd.DataFrame()
    
    def _search_like(self, query: str, limit: int) -> pd.DataFrame:
        """Поиск через LIKE с нормализацией"""
        query_norm = self.normalize_key(pl.Series([query]))[0]
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
                p.barcode,
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
                     p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url, p.barcode
            LIMIT {limit}
        """
        
        try:
            return self.conn.execute(sql_like, [safe_query, safe_query, safe_query, safe_query]).pl().to_pandas()
        except Exception as e:
            logger.error(f"Ошибка поиска LIKE: {e}")
            return pd.DataFrame()
    
    def _search_by_analog(self, query_norm: str, limit: int) -> pd.DataFrame:
        """Поиск по аналогам"""
        sql_analog = f"""
            WITH FoundParts AS (
                SELECT DISTINCT p.artikul_norm, p.brand_norm
                FROM parts p
                LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
                LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
                WHERE 
                    p.artikul_norm LIKE '%' || ? || '%'
                    OR p.brand_norm LIKE '%' || ? || '%'
                    OR o.oe_number_norm LIKE '%' || ? || '%'
                    OR o.name LIKE '%' || ? || '%'
                LIMIT 10
            ),
            AnalogParts AS (
                SELECT DISTINCT
                    cr2.artikul_norm,
                    cr2.brand_norm
                FROM FoundParts fp
                JOIN cross_references cr1 ON fp.artikul_norm = cr1.artikul_norm AND fp.brand_norm = cr1.brand_norm
                JOIN cross_references cr2 ON cr1.oe_number_norm = cr2.oe_number_norm
                WHERE NOT (cr2.artikul_norm = fp.artikul_norm AND cr2.brand_norm = fp.brand_norm)
                LIMIT {limit}
            )
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
                p.barcode,
                STRING_AGG(DISTINCT o.oe_number, ', ') as oe_numbers,
                STRING_AGG(DISTINCT o.name, ', ') as oe_names,
                'найден по аналогу' as search_type
            FROM AnalogParts ap
            JOIN parts p ON ap.artikul_norm = p.artikul_norm AND ap.brand_norm = p.brand_norm
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY p.artikul, p.brand, p.description, p.multiplicity,
                     p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url, p.barcode
            LIMIT {limit}
        """
        
        try:
            result = self.conn.execute(sql_analog, [query_norm, query_norm, query_norm, query_norm]).pl().to_pandas()
            logger.info(f"🔍 Найдено {len(result)} аналогов для запроса: {query_norm}")
            return result
        except Exception as e:
            logger.error(f"Ошибка поиска по аналогам: {e}")
            return pd.DataFrame()
    
    def _search_by_oe_number(self, query_norm: str, limit: int) -> pd.DataFrame:
        """Поиск по OE номеру"""
        sql_oe = f"""
            WITH OEParts AS (
                SELECT DISTINCT
                    cr.artikul_norm,
                    cr.brand_norm
                FROM cross_references cr
                WHERE cr.oe_number_norm LIKE '%' || ? || '%'
                LIMIT {limit}
            )
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
                p.barcode,
                STRING_AGG(DISTINCT o.oe_number, ', ') as oe_numbers,
                STRING_AGG(DISTINCT o.name, ', ') as oe_names,
                'найден по OE' as search_type
            FROM OEParts op
            JOIN parts p ON op.artikul_norm = p.artikul_norm AND op.brand_norm = p.brand_norm
            LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
            LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
            GROUP BY p.artikul, p.brand, p.description, p.multiplicity,
                     p.length, p.width, p.height, p.weight, p.dimensions_str, p.image_url, p.barcode
            LIMIT {limit}
        """
        
        try:
            result = self.conn.execute(sql_oe, [query_norm]).pl().to_pandas()
            logger.info(f"🔍 Найдено {len(result)} записей по OE номеру: {query_norm}")
            return result
        except Exception as e:
            logger.error(f"Ошибка поиска по OE: {e}")
            return pd.DataFrame()
    
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
            
            cross_coverage = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT p.artikul_norm || p.brand_norm) as total_parts,
                    COUNT(DISTINCT cr.artikul_norm || cr.brand_norm) as linked_parts
                FROM parts p
                LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm 
                    AND p.brand_norm = cr.brand_norm
            """).fetchone()
            
            if cross_coverage[0] > 0:
                stats['link_percentage'] = round(
                    (cross_coverage[1] / cross_coverage[0]) * 100, 1
                )
            else:
                stats['link_percentage'] = 0
            
            if self.db_path.exists():
                stats['db_size_mb'] = round(self.db_path.stat().st_size / (1024 * 1024), 2)
            
            stats['performance'] = self.performance_metrics.copy()
            stats['cache_hit_rate'] = (
                self.performance_metrics['cache_hits'] / max(self.performance_metrics['queries'], 1) * 100
            )
            
        except Exception as e:
            logger.error(f"Ошибка сбора статистики: {e}")
        
        return stats
    
    # ========================================================================
    # УПРАВЛЕНИЕ ДАННЫМИ
    # ========================================================================
    def delete_by_brand(self, brand_norm: str) -> int:
        """Удаление записей по бренду с каскадным удалением связей"""
        try:
            count_parts = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
            
            if count_parts == 0:
                logger.info(f"Нет записей для бренда: {brand_norm}")
                return 0
            
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
            
            self.conn.execute("DELETE FROM prices WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute("DELETE FROM cross_references WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute("DELETE FROM parts WHERE artikul_norm = ?", [artikul_norm])
            
            self.vacuum_database()
            
            return count_parts
            
        except Exception as e:
            logger.error(f"Ошибка удаления артикула {artikul_norm}: {e}")
            raise
    
    # ========================================================================
    # ИНТЕРФЕЙСЫ ПОЛЬЗОВАТЕЛЯ
    # ========================================================================
    def show_export_interface(self):
        """Расширенный интерфейс экспорта"""
        st.header("📤 Экспорт данных")
        
        total = self.conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)"
        ).fetchone()[0]
        
        if total == 0:
            st.warning("Нет данных для экспорта")
            return
        
        col1, col2 = st.columns(2)
        col1.metric("Всего товаров", f"{total:,}")
        
        # ============================================================
        # ОСНОВНЫЕ НАСТРОЙКИ
        # ============================================================
        with st.expander("⚙️ Основные настройки", expanded=True):
            col1, col2 = st.columns(2)
            
            with col1:
                # Формат экспорта
                format_type = st.selectbox(
                    "Формат файла:",
                    ["CSV", "Excel (.xlsx)", "JSON"],
                    index=0
                )
                
                # Формат для маркетплейса
                market_format = st.selectbox(
                    "Площадка (форматирование):",
                    ["Без форматирования", "Yandex Market", "Ozon", "Avito", "СберМегаМаркет"],
                    index=0
                )
                
                market_format_map = {
                    "Без форматирования": None,
                    "Yandex Market": "yandex",
                    "Ozon": "ozon",
                    "Avito": "avito",
                    "СберМегаМаркет": "sber"
                }
                market_format_key = market_format_map.get(market_format)
            
            with col2:
                # Настройки CSV
                csv_separator = st.selectbox(
                    "Разделитель столбцов:",
                    [";", ",", "\t"],
                    index=0
                )
                
                encoding = st.selectbox(
                    "Кодировка:",
                    ["windows-1251", "utf-8", "cp866"],
                    index=0
                )
                
                decimal_separator = st.selectbox(
                    "Десятичный разделитель:",
                    [".", ","],
                    index=0
                )
        
        # ============================================================
        # НАСТРОЙКИ РАЗБИЕНИЯ
        # ============================================================
        with st.expander("📦 Разбиение и архивация", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                split_size = st.number_input(
                    "Разбить на части (строк):",
                    min_value=0,
                    max_value=1000000,
                    value=500000,
                    step=10000,
                    help="0 - не разбивать"
                )
            
            with col2:
                zip_archive = st.checkbox(
                    "Упаковать все части в архив (.zip)",
                    value=False
                )
        
        # ============================================================
        # НАСТРОЙКИ ФОРМАТИРОВАНИЯ
        # ============================================================
        with st.expander("🔤 Форматирование", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                add_apostrophe = st.checkbox(
                    "Апостроф в номер",
                    value=True,
                    help="Добавляет ' перед номером для предотвращения преобразования в Excel"
                )
                
                use_crlf = st.checkbox(
                    "Использовать CRLF для переноса строк",
                    value=True
                )
            
            with col2:
                remove_semicolon = st.checkbox(
                    "Удалять символ ; из названий",
                    value=False,
                    help="Удаляет точки с запятой из текстовых полей"
                )
                
                include_prices = st.checkbox(
                    "Включить цены",
                    value=True
                )
        
        # ============================================================
        # ШАБЛОНЫ
        # ============================================================
        with st.expander("📝 Шаблоны названий и описаний", expanded=False):
            st.info("Используйте переменные в формате {{variable_name}}")
            
            col1, col2 = st.columns(2)
            
            with col1:
                template_name = st.text_area(
                    "Шаблон названия:",
                    value=self.export_handler.template_engine.get_default_name_template(),
                    height=100,
                    help="Доступные переменные: {{oem}}, {{make_name}}, {{detail_name}}, {{min_qnt}}, {{barcode}} и др."
                )
            
            with col2:
                template_description = st.text_area(
                    "Шаблон описания:",
                    value=self.export_handler.template_engine.get_default_description_template(),
                    height=150,
                    help="Доступные переменные: {{oem}}, {{make_name}}, {{detail_name}}, {{product_description}}, {{cross}} и др."
                )
            
            # Список доступных переменных
            with st.expander("📋 Доступные переменные", expanded=False):
                variables = [
                    ("{{oem}}", "Номер детали"),
                    ("{{source_oem}}", "Неформатированный номер из прайс-листа"),
                    ("{{make_name}}", "Бренд"),
                    ("{{detail_name}}", "Название детали"),
                    ("{{cross}}", "Кроссы из базы"),
                    ("{{barcode}}", "EAN13 штрих-код"),
                    ("{{qnt}}", "Количество"),
                    ("{{min_qnt}}", "Минимальное количество"),
                    ("{{price}}", "Цена"),
                    ("{{currency}}", "Валюта"),
                    ("{{length}}", "Длина, см"),
                    ("{{width}}", "Ширина, см"),
                    ("{{height}}", "Высота, см"),
                    ("{{weight}}", "Вес, кг"),
                    ("{{category}}", "Категория"),
                    ("{{applicability}}", "Применимость"),
                    ("{{product_description}}", "Описание товара"),
                    ("{{image_url}}", "Ссылка на изображение"),
                    ("{{additional_images}}", "Дополнительные изображения"),
                    ("{{properties_1}}", "Свойство 1"),
                    ("{{properties_2}}", "Свойство 2"),
                    ("{{properties_3}}", "Свойство 3"),
                    ("{{properties_4}}", "Свойство 4"),
                    ("{{properties_5}}", "Свойство 5"),
                    ("{{properties_6}}", "Свойство 6"),
                ]
                
                var_df = pd.DataFrame(variables, columns=["Переменная", "Описание"])
                st.dataframe(var_df, use_container_width=True, hide_index=True)
        
        # ============================================================
        # ВЫБОР КОЛОНОК
        # ============================================================
        with st.expander("📋 Настройка колонок", expanded=True):
            st.subheader("Выберите колонки для экспорта")
            
            # Все доступные колонки
            all_columns = ExportColumnConfig.ALL_COLUMNS
            
            # Создание категорий для выбора
            selected_columns = []
            
            for category, columns in all_columns.items():
                st.markdown(f"**{category}**")
                
                # Поиск по колонкам
                cols_per_row = 4
                col_container = st.columns(cols_per_row)
                
                for idx, col in enumerate(columns):
                    col_idx = idx % cols_per_row
                    checked = col_container[col_idx].checkbox(
                        col,
                        value=True,
                        key=f"col_{col}_{idx}"
                    )
                    if checked:
                        selected_columns.append(col)
                
                st.markdown("---")
            
            # Отображение выбранных колонок
            st.info(f"Выбрано колонок: {len(selected_columns)}")
            
            # Возможность сортировки колонок
            if len(selected_columns) > 1:
                st.subheader("Порядок колонок")
                
                # Сортировка через перетаскивание (замена - через selectbox)
                sorted_columns = st.multiselect(
                    "Перетащите для изменения порядка:",
                    selected_columns,
                    default=selected_columns,
                    key="column_order"
                )
                
                if sorted_columns:
                    selected_columns = sorted_columns
        
        # ============================================================
        # ДОПОЛНИТЕЛЬНЫЕ НАСТРОЙКИ
        # ============================================================
        with st.expander("🔧 Дополнительные настройки", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                apply_markup = st.checkbox(
                    "Применить наценку",
                    value=True,
                    help=f"Глобальная наценка: {self.price_rules.get('global_markup', 0) * 100:.1f}%"
                )
            
            with col2:
                apply_exclusions = st.checkbox(
                    "Применить исключения",
                    value=True,
                    help=f"Активно правил: {len(self.exclusion_rules)}"
                )
        
        # ============================================================
        # ИМЯ ФАЙЛА И ЭКСПОРТ
        # ============================================================
        filename = st.text_input(
            "Имя файла:",
            value=f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            help="Без расширения"
        )
        
        if st.button("🚀 Экспортировать", type="primary", use_container_width=True):
            # Определение расширения
            ext_map = {
                "CSV": "csv",
                "Excel (.xlsx)": "xlsx",
                "JSON": "json"
            }
            ext = ext_map.get(format_type, "csv")
            
            output_filename = f"{filename}.{ext}"
            output_path = self.data_dir / output_filename
            
            with st.spinner(f"⏳ Генерация файла {output_filename}..."):
                progress_bar = st.progress(0)
                
                try:
                    success = self.export_data(
                        output_path=str(output_path),
                        selected_columns=selected_columns if selected_columns else None,
                        include_prices=include_prices,
                        apply_markup=apply_markup,
                        apply_exclusions=apply_exclusions,
                        format_type=format_type.lower().replace(" ", "").replace(".", ""),
                        market_format=market_format_key,
                        template_name=template_name if template_name else None,
                        template_description=template_description if template_description else None,
                        csv_separator=csv_separator,
                        decimal_separator=decimal_separator,
                        encoding=encoding,
                        add_apostrophe=add_apostrophe,
                        use_crlf=use_crlf,
                        remove_semicolon=remove_semicolon,
                        split_size=split_size,
                        zip_archive=zip_archive
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
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'size_mb': round(len(file_data) / (1024 * 1024), 2),
                            'format': format_type,
                            'rows': total,
                            'marketplace': market_format if market_format != "Без форматирования" else "Стандартный"
                        })
                        
                        st.success(f"✅ Экспорт успешно создан!")
                    
                except Exception as e:
                    st.error(f"❌ Ошибка экспорта: {str(e)}")
                    logger.exception("Ошибка экспорта")
                
                finally:
                    progress_bar.empty()
        
        # ============================================================
        # ИСТОРИЯ ЭКСПОРТОВ
        # ============================================================
        if 'export_history' in st.session_state and st.session_state.export_history:
            with st.expander("📋 История экспортов", expanded=False):
                history_df = pd.DataFrame(st.session_state.export_history)
                st.dataframe(
                    history_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "filename": "Имя файла",
                        "timestamp": "Время",
                        "size_mb": st.column_config.NumberColumn("Размер, МБ", format="%.2f"),
                        "format": "Формат",
                        "rows": "Записей",
                        "marketplace": "Площадка"
                    }
                )
                
                if st.button("🗑️ Очистить историю"):
                    st.session_state.export_history = []
                    st.rerun()
    
    def show_price_settings(self):
        """Настройки цен"""
        st.header("💰 Управление ценами и наценками")
        
        tabs = st.tabs(["Общие настройки", "Наценки по брендам", "Ограничения"])
        
        with tabs[0]:
            st.subheader("Общие настройки цен")
            
            col1, col2 = st.columns(2)
            
            with col1:
                global_markup = st.number_input(
                    "Глобальная наценка (%):",
                    min_value=0.0,
                    max_value=1000.0,
                    value=self.price_rules.get('global_markup', 0.2) * 100,
                    step=1.0
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
            
            brand_markups = self.price_rules.get('brand_markups', {})
            
            try:
                brands = self.conn.execute(
                    "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL ORDER BY brand"
                ).fetchall()
                available_brands = [row[0] for row in brands]
            except Exception:
                available_brands = []
            
            if available_brands:
                if brand_markups:
                    markup_df = pd.DataFrame([
                        {"Бренд": brand, "Наценка (%)": f"{markup * 100:.1f}%"}
                        for brand, markup in brand_markups.items()
                    ])
                    st.dataframe(markup_df, use_container_width=True, hide_index=True)
                
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
                    step=10.0
                )
                self.price_rules['min_price'] = min_price
            
            with col2:
                max_price = st.number_input(
                    "Максимальная цена:",
                    min_value=0.0,
                    value=float(self.price_rules.get('max_price', 99999)),
                    step=1000.0
                )
                self.price_rules['max_price'] = max_price
        
        if st.button("💾 Сохранить все настройки цен", type="primary"):
            self.save_price_rules()
            st.success("✅ Все настройки цен сохранены")
    
    def show_statistics(self):
        """Интерфейс статистики"""
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
            
            if st.button("🔍 Проверить целостность БД"):
                with st.spinner("Проверка базы данных..."):
                    health = self.check_database_health()
                    
                    if health.get('corruption_detected'):
                        st.error("⚠️ Обнаружены проблемы с целостностью данных!")
                    else:
                        st.success("✅ База данных в порядке")
                    
                    st.json(health)
    
    def show_data_management(self):
        """Интерфейс управления данными"""
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
                "🔍 Диагностика"
            ],
            horizontal=False
        )
        
        if management_option == "🗑️ Удаление данных":
            self._show_delete_interface()
        elif management_option == "💰 Цены и наценки":
            self.show_price_settings()
        elif management_option == "🚫 Исключения":
            self._show_exclusion_settings()
        elif management_option == "🗂️ Категории":
            self._show_category_mapping()
        elif management_option == "🔗 Управление связями":
            self._show_link_rules_interface()
        elif management_option == "📋 Маппинг колонок":
            self._show_column_mapping_interface()
        elif management_option == "🔍 Диагностика":
            self._show_diagnostics()
    
    def _show_delete_interface(self):
        """Интерфейс удаления данных"""
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
            
            parts_count = self.conn.execute("SELECT COUNT(*) FROM parts").fetchone()[0]
            oe_count = self.conn.execute("SELECT COUNT(*) FROM oe").fetchone()[0]
            cross_count = self.conn.execute("SELECT COUNT(*) FROM cross_references").fetchone()[0]
            prices_count = self.conn.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
            
            st.info(f"""
            Будут удалены все записи из таблиц:
            - Parts: {parts_count:,} записей
            - OE: {oe_count:,} записей
            - Cross References: {cross_count:,} записей
            - Prices: {prices_count:,} записей
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
                            backup_path = self.data_dir / f"backup_before_clean_{datetime.now().strftime('%Y%m%d_%H%M%S')}.duckdb"
                            shutil.copy2(self.db_path, backup_path)
                            
                            self.conn.execute("DELETE FROM prices")
                            self.conn.execute("DELETE FROM cross_references")
                            self.conn.execute("DELETE FROM oe")
                            self.conn.execute("DELETE FROM parts")
                            
                            self.vacuum_database()
                            
                            st.success(f"✅ Все данные удалены. Бэкап сохранен: {backup_path.name}")
                            st.rerun()
                            
                        except Exception as e:
                            st.error(f"Ошибка при удалении: {e}")
    
    def _show_exclusion_settings(self):
        """Настройки исключений"""
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
    
    def _show_category_mapping(self):
        """Настройки категорий"""
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
    
    def _show_link_rules_interface(self):
        """Настройки связей"""
        st.header("🔗 Управление связями данных")
        st.info("Настройте правила связывания OE номеров с артикулами")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Основные настройки")
            
            use_cross = st.checkbox(
                "Использовать кросс-ссылки",
                value=self.link_rules.get('use_cross_references', True)
            )
            self.link_rules['use_cross_references'] = use_cross
            
            if use_cross:
                max_depth = st.slider(
                    "Глубина связей:",
                    min_value=1,
                    max_value=3,
                    value=self.link_rules.get('max_link_depth', 2)
                )
                self.link_rules['max_link_depth'] = max_depth
                
                link_by_oe = st.checkbox(
                    "Связывать только через OE",
                    value=self.link_rules.get('link_by_oe_only', False)
                )
                self.link_rules['link_by_oe_only'] = link_by_oe
            
            prefer_original = st.checkbox(
                "Предпочитать оригинальные OE",
                value=self.link_rules.get('prefer_original_oe', True)
            )
            self.link_rules['prefer_original_oe'] = prefer_original
        
        with col2:
            st.subheader("Дополнительные связи")
            
            use_dimensions = st.checkbox(
                "Связывать по габаритам",
                value=self.link_rules.get('use_dimensions_linking', True)
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
                    default=priority_brands
                )
                self.link_rules['priority_brands_for_linking'] = new_priority
            
            with col2:
                st.markdown("**Исключенные бренды:**")
                new_exclude = st.multiselect(
                    "Выберите исключаемые бренды:",
                    available_brands,
                    default=exclude_brands
                )
                self.link_rules['exclude_brands_from_linking'] = new_exclude
        
        if st.button("💾 Сохранить правила связывания", type="primary"):
            self.save_link_rules()
            st.success("✅ Правила связывания сохранены")
    
    def _show_column_mapping_interface(self):
        """Настройки маппинга колонок"""
        st.header("📋 Управление маппингом колонок")
        st.info("Настройте соответствие названий колонок в загружаемых файлах")
        
        file_types = list(self.column_mapping_config.keys())
        selected_type = st.selectbox(
            "Тип файла:",
            file_types,
            format_func=lambda x: {
                'oe': 'OE данные',
                'cross': 'Кросс-ссылки',
                'prices': 'Цены',
                'dimensions': 'Габариты',
                'barcode': 'Штрих-коды',
                'images': 'Изображения'
            }.get(x, x)
        )
        
        if selected_type in self.column_mapping_config:
            st.subheader(f"Поля для типа: {selected_type}")
            
            config = self.column_mapping_config[selected_type]
            
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
                    height=200
                )
                
                if st.button("💾 Сохранить варианты"):
                    new_variants = [v.strip() for v in new_variants_text.split("\n") if v.strip()]
                    config[field_to_edit] = new_variants
                    self.save_column_mapping_config()
                    st.success(f"✅ Варианты для '{field_to_edit}' сохранены")
                    st.rerun()
            
            st.markdown("---")
            
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
    
    def _show_diagnostics(self):
        """Диагностика системы"""
        st.subheader("🔍 Диагностика системы")
        
        if st.button("🔄 Запустить диагностику"):
            with st.spinner("Выполнение диагностики..."):
                health = self.check_database_health()
                
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
                
                st.markdown("### Статистика таблиц")
                
                table_stats = []
                for table in ['parts', 'oe', 'cross_references', 'prices']:
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
                
                orphan_details = health.get('orphan_details', {})
                if orphan_details.get('cross_orphans', 0) > 0 or orphan_details.get('oe_orphans', 0) > 0:
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
        page_title="Каталог автозапчастей v200.2",
        page_icon="🔧",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    st.title("🔧 Каталог автозапчастей v200.2")
    
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
                    display_cols = [c for c in ['artikul', 'brand', 'multiplicity', 'barcode'] if c in results.columns]
                    if display_cols:
                        st.dataframe(
                            results[display_cols],
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
                    key="oe_uploader"
                )
                
                cross_files = st.file_uploader(
                    "Кроссы (связи OE-артикул)",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="cross_uploader"
                )
            
            with col2:
                prices_files = st.file_uploader(
                    "Цены",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="prices_uploader"
                )
        
        with st.expander("📎 Дополнительные данные", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                dimensions_files = st.file_uploader(
                    "Габариты (Д×Ш×В×Вес)",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="dimensions_uploader"
                )
                
                barcode_files = st.file_uploader(
                    "Штрих-коды и кратность",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="barcode_uploader"
                )
            
            with col2:
                images_files = st.file_uploader(
                    "Изображения (URL)",
                    type=['xlsx', 'xls', 'csv'],
                    accept_multiple_files=True,
                    key="images_uploader"
                )
        
        with st.expander("📦 Универсальная загрузка", expanded=False):
            universal_files = st.file_uploader(
                "Универсальный файл (все данные в одном)",
                type=['xlsx', 'xls', 'csv'],
                accept_multiple_files=True,
                key="universal_uploader"
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
                
                uploaded_files_dict = {k: v for k, v in uploaded_files_dict.items() if v}
                
                if not uploaded_files_dict:
                    st.warning("⚠️ Не выбрано ни одного файла для загрузки")
                else:
                    total_files = sum(len(files) for files in uploaded_files_dict.values())
                    st.info(f"📦 Загружено файлов: {total_files}")
                    
                    with st.spinner("🔄 Обработка файлов... Это может занять несколько минут"):
                        try:
                            dataframes = catalog.process_uploaded_files(uploaded_files_dict)
                            
                            if not dataframes:
                                st.error("❌ Не удалось обработать ни одного файла. Проверьте логи.")
                            else:
                                catalog.process_and_load_data(dataframes)
                                
                                st.session_state.uploaded_files = {
                                    k: len(v) for k, v in dataframes.items()
                                }
                                
                                st.success("✅ Все данные успешно загружены в базу!")
                                
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
                    
                    if 'search_type' in results_df.columns:
                        st.caption(f"🔍 Способ поиска: {results_df['search_type'].iloc[0] if not results_df.empty else 'прямой'}")
                    
                    available_cols = [c for c in results_df.columns 
                                    if c not in ['artikul_norm', 'brand_norm', 'search_type']]
                    
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
                            "barcode": st.column_config.TextColumn("Штрихкод", width="medium"),
                        }
                    )
    
    elif menu == "⚙️ Управление и настройки":
        catalog.show_data_management()
    
    elif menu == "📤 Экспорт":
        catalog.show_export_interface()

if __name__ == "__main__":
    main()
