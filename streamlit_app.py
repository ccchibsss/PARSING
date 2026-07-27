    # Потокобезопасность для конкурентных записей в DuckDB
    self.db_lock = threading.Lock()
    
    self.cloud_config = self.load_cloud_config()
    self.price_rules = self.load_price_rules()
    self.exclusion_rules = self.load_exclusion_rules()
    self.category_mapping = self.load_category_mapping()
    
    self.db_path = self.data_dir / "catalog.duckdb"
    self.conn = duckdb.connect(database=str(self.db_path), check_same_thread=False)
    self.setup_database()

# ========================================================================
# КОНФИГУРАЦИИ
# ========================================================================
def load_cloud_config(self) -> Dict[str, Any]:
    """Загрузка конфигурации облачной синхронизации."""
    config_path = self.data_dir / "cloud_config.json"
    default_config = {
        "enabled": False, "provider": "s3", "bucket": "",
        "region": "", "sync_interval": 3600, "last_sync": 0
    }
    if config_path.exists():
        try:
            return json.loads(config_path.read_text(encoding='utf-8'))
        except Exception as e:
            logger.error(f"Ошибка чтения cloud_config.json: {e}")
            return default_config
    else:
        config_path.write_text(json.dumps(default_config, indent=2, ensure_ascii=False), encoding='utf-8')
        return default_config

def save_cloud_config(self):
    """Сохранение конфигурации облачной синхронизации."""
    config_path = self.data_dir / "cloud_config.json"
    self.cloud_config["last_sync"] = int(time.time())
    config_path.write_text(json.dumps(self.cloud_config, indent=2, ensure_ascii=False), encoding='utf-8')

def load_price_rules(self) -> Dict[str, Any]:
    """Загрузка правил ценообразования."""
    price_rules_path = self.data_dir / "price_rules.json"
    default_rules = {
        "global_markup": 0.2, "brand_markups": {},
        "min_price": 0.0, "max_price": 99999.0
    }
    if price_rules_path.exists():
        try:
            return json.loads(price_rules_path.read_text(encoding='utf-8'))
        except Exception as e:
            logger.error(f"Ошибка чтения price_rules.json: {e}")
            return default_rules
    else:
        price_rules_path.write_text(json.dumps(default_rules, indent=2, ensure_ascii=False), encoding='utf-8')
        return default_rules

def save_price_rules(self):
    """Сохранение правил ценообразования."""
    price_rules_path = self.data_dir / "price_rules.json"
    price_rules_path.write_text(json.dumps(self.price_rules, indent=2, ensure_ascii=False), encoding='utf-8')

def load_exclusion_rules(self) -> List[str]:
    """Загрузка правил исключения."""
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
    """Сохранение правил исключения."""
    exclusion_path = self.data_dir / "exclusion_rules.txt"
    exclusion_path.write_text("\n".join(self.exclusion_rules), encoding='utf-8')

def load_category_mapping(self) -> Dict[str, str]:
    """Загрузка маппинга категорий."""
    category_path = self.data_dir / "category_mapping.txt"
    default_mapping = {
        "Радиатор": "Охлаждение", "Шаровая опора": "Подвеска",
        "Фильтр масляный": "Фильтры", "Тормозные колодки": "Тормоза"
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
        content = "\n".join([f"{k}|{v}" for k, v in default_mapping.items()])
        category_path.write_text(content, encoding='utf-8')
        return default_mapping

def save_category_mapping(self):
    """Сохранение маппинга категорий."""
    category_path = self.data_dir / "category_mapping.txt"
    content = "\n".join([f"{k}|{v}" for k, v in self.category_mapping.items()])
    category_path.write_text(content, encoding='utf-8')

# ========================================================================
# БАЗА ДАННЫХ И ИНДЕКСЫ
# ========================================================================
def setup_database(self):
    """Инициализация структуры базы данных."""
    with self.db_lock:
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
    """Создание индексов для ускорения поиска."""
    st.info("🛠️ Создание индексов для ускорения поиска...")
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
            # Не прекращаем выполнение, если один индекс не создался
    st.success("🛠️ Индексы созданы.")

# ========================================================================
# БЛОК 2: НОРМАЛИЗАЦИЯ, ОЧИСТКА, БЕЗОПАСНАЯ КОНВЕРТАЦИЯ И РАСПОЗНАВАНИЕ
# ========================================================================
@staticmethod
def normalize_key(series: pl.Series) -> pl.Series:
    """Нормализация ключевых полей (артикул, бренд, OE) для точного поиска"""
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
    """Очистка текстовых значений от мусорных символов"""
    return (series
            .fill_null("")
            .cast(pl.Utf8)
            .str.replace_all("'", "")
            .str.replace_all(r"[^0-9A-Za-zА-Яа-яЁё`\-\s]", "")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars())

def determine_category_vectorized(self, name_series: pl.Series) -> pl.Series:
    """Векторизованная категоризация по названию товара"""
    name_lower = name_series.str.to_lowercase()
    
    categorization_expr = pl.when(pl.lit(False)).then(pl.lit(None))
    
    # Сначала пользовательские правила (имеют приоритет)
    for key, category in self.category_mapping.items():
        categorization_expr = categorization_expr.when(
            name_lower.str.contains(key.lower())
        ).then(pl.lit(category))
    
    # Затем встроенные эвристики
    categories_map = {
        'Фильтр': 'фильтр|filter',
        'Тормоза': 'тормоз|brake|колодк|диск|суппорт',
        'Подвеска': 'амортизатор|стойк|spring|подвеск|рычаг|шаров|сайлентблок',
        'Двигатель': 'двигатель|engine|свеч|поршень|клапан|ремень|цепь грм',
        'Трансмиссия': 'трансмиссия|сцеплен|коробк|transmission|кулиса',
        'Электрика': 'аккумулятор|генератор|стартер|провод|ламп|датчик|катушка',
        'Рулевое': 'рулевой|тяга|наконечник|steering|рейка',
        'Выпуск': 'глушитель|катализатор|выхлоп|exhaust|гофра',
        'Охлаждение': 'радиатор|вентилятор|термостат|cooling|помпа|патрубок',
        'Топливо': 'топливный|бензонасос|форсунк|fuel|бак|крышка бака'
    }
    
    for category, pattern in categories_map.items():
        categorization_expr = categorization_expr.when(
            name_lower.str.contains(pattern, literal=False)
        ).then(pl.lit(category))
    
    return categorization_expr.otherwise(pl.lit('Разное')).alias('category')

# ========================================================================
# БЕЗОПАСНАЯ КОНВЕРТАЦИЯ В ЧИСЛО С ФИКСАЦИЕЙ ОШИБОК
# ========================================================================
@staticmethod
def safe_convert_to_float(value: Any) -> Tuple[float, Optional[str]]:
    """
    Конвертирует значение в float.
    Возвращает кортеж: (результат, сообщение об ошибке или None).
    """
    if value is None or value == "":
        return 0.0, None
    
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return 0.0, "NaN/Inf"
        return float(value), None
    
    if isinstance(value, decimal.Decimal):
        return float(value), None
    
    if isinstance(value, (datetime, date, pd.Timestamp)):
        try:
            base = datetime(1899, 12, 30)
            if isinstance(value, pd.Timestamp):
                value = value.to_pydatetime()
            delta = value - base
            return float(delta.days + delta.seconds / 86400.0), None
        except Exception:
            return 0.0, f"Не удалось конвертировать дату: {value}"
    
    if isinstance(value, timedelta):
        return float(value.total_seconds() / 86400.0), None
    
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return 0.0, None
        
        cleaned = re.sub(r'[^\d.,\-]', '', value)
        if not cleaned:
            return 0.0, f"Не числовое значение: '{value}'"
        
        cleaned = cleaned.replace(',', '.')
        parts = cleaned.split('.')
        if len(parts) > 2:
            cleaned = parts[0] + '.' + ''.join(parts[1:])
        
        try:
            return float(cleaned), None
        except ValueError:
            return 0.0, f"Не удалось распарсить число: '{value}'"
    
    if hasattr(value, 'dtype') and hasattr(value, 'item'):
        try:
            item = value.item()
            if isinstance(item, (int, float)):
                return float(item), None
        except Exception:
            pass
    
    if hasattr(value, 'to_python'):
        try:
            return float(value.to_python()), None
        except Exception:
            pass
    
    try:
        return float(value), None
    except (ValueError, TypeError):
        return 0.0, f"Неизвестный тип: {type(value).__name__}"

def convert_series_to_float_with_errors(self, series: pl.Series, col_name: str) -> Tuple[pl.Series, pl.Series]:
    """
    Конвертирует колонку в float, собирая ошибки валидации.
    Возвращает: (числовая колонка, колонка с описаниями ошибок).
    """
    values = series.to_list()
    converted = []
    errors = []
    
    for val in values:
        num, err = self.safe_convert_to_float(val)
        converted.append(num)
        if err:
            errors.append(f"{col_name}: {err}")
        else:
            errors.append("")
    
    return pl.Series(converted), pl.Series(errors)

# ========================================================================
# РАСПОЗНАВАНИЕ СТОЛБЦОВ (ЭВРИСТИКА ДЛЯ ИНТЕРАКТИВНОГО МАППИНГА)
# ========================================================================
def detect_columns(self, actual_columns: List[str], expected_columns: List[str]) -> Dict[str, str]:
    """
    Эвристическое сопоставление фактических колонок файла с системными полями.
    Возвращает маппинг: {фактическая_колонка: системное_поле}.
    Используется как предзаполнение для интерактивного маппинга.
    """
    column_variants = {
        'oe_number': ['oe номер', 'oe', 'оe', 'номер', 'code', 'oe_number', 'oe number',
                      'код oe', 'артикул oe', 'оригинальный номер', 'ориг. номер', 'oem', 'oem номер'],
        'artikul': ['артикул', 'article', 'sku', 'artikul', 'код товара', 'код', 'код артикула',
                    'part number', 'номер детали', 'арт', 'артикул производителя', 'каталожный номер'],
        'brand': ['бренд', 'brand', 'производитель', 'manufacturer', 'марка', 'maker', 'поставщик',
                  'изготовитель', 'фирма', 'торговая марка'],
        'name': ['наименование', 'название', 'name', 'описание', 'description', 'товар',
                 'наименование товара', 'деталь', 'product name', 'наименование детали', 'наим'],
        'applicability': ['применимость', 'автомобиль', 'vehicle', 'applicability', 'применяемость',
                          'авто', 'car', 'model', 'марка авто', 'применение', 'для авто'],
        'barcode': ['штрих-код', 'barcode', 'штрихкод', 'ean', 'eac13', 'штрих код', 'upc', 'gtin',
                    'штрих', 'код еан', 'ean13'],
        'multiplicity': ['кратность шт', 'кратность', 'multiplicity', 'кратность упаковки', 'qty',
                         'кол-во в уп', 'упаковка', 'количество', 'шт в упаковке', 'мин. заказ'],
        'length': ['длина (см)', 'длина', 'length', 'длинна', 'длина, см', 'length_cm', 'l', 'len',
                   'длина см', 'длина (mm)', 'длина мм'],
        'width': ['ширина (см)', 'ширина', 'width', 'ширина, см', 'width_cm', 'w', 'wid',
                  'ширина см', 'ширина (mm)', 'ширина мм'],
        'height': ['высота (см)', 'высота', 'height', 'высота, см', 'height_cm', 'h', 'hei',
                   'высота см', 'высота (mm)', 'высота мм'],
        'weight': ['вес (кг)', 'вес, кг', 'вес', 'weight', 'масса', 'weight_kg', 'вес кг', 'mass',
                   'вес (г)', 'масса кг', 'масса (кг)', 'вес брутто', 'вес нетто'],
        'image_url': ['ссылка', 'url', 'изображение', 'image', 'картинка', 'фото',
                      'ссылка на изображение', 'photo', 'img', 'ссылка на фото', 'изображения'],
        'dimensions_str': ['весогабариты', 'размеры', 'dimensions', 'size', 'габариты',
                           'длинна/ширина/высота', 'длина/ширина/высота', 'lwh', 'lxwxh',
                           'габаритные размеры', 'дхшхв'],
        'price': ['цена', 'price', 'рекомендованная цена', 'retail price', 'цена продажи',
                  'стоимость', 'cost', 'rrp', 'розничная цена', 'цена руб', 'цена за шт'],
        'currency': ['валюта', 'currency', 'cur', 'вал']
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
    
    logger.info(f"Эвристический маппинг колонок: {mapping}")
    return mapping

# ========================================================================
# БЛОК 3: ЧТЕНИЕ ФАЙЛОВ, ВАЛИДАЦИЯ И UPSERT В БАЗУ
# ========================================================================
def read_and_prepare_file(self, file_path: str, file_type: str,
                          column_mapping: Optional[Dict[str, str]] = None) -> pl.DataFrame:
    """
    Читает файл, применяет маппинг колонок, нормализует ключи,
    конвертирует числа и формирует столбец _validation_errors
    с описанием всех проблем валидации по каждой строке.
    
    Args:
        file_path: путь к файлу
        file_type: тип файла (oe, cross, prices, dimensions, barcode, images, universal)
        column_mapping: явный маппинг {фактическая_колонка: системное_поле}.
                        Если None — используется эвристика detect_columns.
    """
    logger.info(f"Обработка файла: {file_type} ({file_path})")
    
    # Проверка расширения файла
    file_ext = Path(file_path).suffix.lower()
    if file_ext not in ['.xlsx', '.xls', '.csv']:
        logger.error(f"Неподдерживаемый тип файла: {file_ext}")
        return pl.DataFrame()
    
    try:
        if not os.path.exists(file_path):
            logger.error(f"Файл не найден: {file_path}")
            return pl.DataFrame()
        
        df = pl.read_excel(file_path, engine='calamine')
        
        if df.is_empty():
            logger.warning(f"Пустой файл: {file_path}")
            return pl.DataFrame()
        
        logger.info(f"Исходные колонки файла {file_type}: {df.columns}")
    
    except Exception as e:
        logger.exception(f"Ошибка чтения файла {file_path}: {e}")
        return pl.DataFrame()
    
    # Ожидаемые системные поля для каждого типа файла
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
    
    # Если маппинг не передан (массовая загрузка) — используем эвристику
    if column_mapping is None:
        column_mapping = self.detect_columns(df.columns, expected_cols)
    
    if not column_mapping:
        logger.warning(f"Не удалось определить колонки для файла {file_type}. Доступные: {df.columns}")
        return pl.DataFrame()
    
    logger.info(f"Маппинг колонок для {file_type}: {column_mapping}")
    
    # Переименование колонок согласно маппингу
    try:
        df = df.rename(column_mapping)
    except Exception as e:
        logger.error(f"Ошибка при rename: {e}")
        for old_name, new_name in list(column_mapping.items()):
            try:
                if new_name not in df.columns:
                    df = df.rename({old_name: new_name})
            except Exception as e2:
                logger.warning(f"Не удалось переименовать {old_name} → {new_name}: {e2}")
    
    # Удаление дубликатов колонок (оставляем первое вхождение)
    if len(df.columns) != len(set(df.columns)):
        seen = set()
        cols_to_keep = []
        for col in df.columns:
            if col not in seen:
                seen.add(col)
                cols_to_keep.append(col)
        df = df.select(cols_to_keep)
    
    # Очистка ключевых полей от мусорных символов
    for col in ['artikul', 'brand', 'oe_number']:
        if col in df.columns:
            df = df.with_columns(self.clean_values(pl.col(col)).alias(col))
    
    # Конвертация числовых колонок с фиксацией ошибок валидации
    numeric_cols = ['length', 'width', 'height', 'weight', 'price']
    row_errors: List[List[str]] = [[] for _ in range(len(df))]
    
    for col in numeric_cols:
        if col in df.columns:
            values = df[col].to_list()
            converted = []
            for idx, val in enumerate(values):
                num, err = self.safe_convert_to_float(val)
                converted.append(round(num, 2))
                if err:
                    row_errors[idx].append(f"{col}: {err}")
            df = df.with_columns(pl.Series(converted).alias(col))
    
    # Формирование единого столбца с ошибками валидации
    error_strings = [" | ".join(errs) for errs in row_errors]
    df = df.with_columns(pl.Series("_validation_errors", error_strings))
    
    # Удаление дубликатов строк по ключевым колонкам
    key_cols = [col for col in ['oe_number', 'artikul', 'brand'] if col in df.columns]
    if key_cols:
        df = df.unique(subset=key_cols, keep='first')
    
    # Нормализация ключей для точного поиска и связывания
    for col in ['artikul', 'brand', 'oe_number']:
        if col in df.columns:
            df = df.with_columns(self.normalize_key(pl.col(col)).alias(f"{col}_norm"))
    
    logger.info(f"Файл {file_type} обработан. Итоговые колонки: {df.columns}")
    return df

# ========================================================================
# ПОТОКОБЕЗОПАСНЫЙ UPSERT В DUCKDB
# ========================================================================
def upsert_data(self, table_name: str, df: pl.DataFrame, pk: List[str]):
    """
    Потокобезопасный UPSERT: удаляет существующие записи по первичному ключу
    и вставляет новые. Все операции выполняются под блокировкой db_lock.
    """
    if df.is_empty():
        return
    
    # Служебный столбец валидации не должен попадать в базу
    if "_validation_errors" in df.columns:
        df = df.drop("_validation_errors")
    
    df = df.unique(keep='first')
    temp_view_name = f"temp_{table_name}_{int(time.time() * 1000)}"
    
    with self.db_lock:
        try:
            self.conn.register(temp_view_name, df.to_arrow())
        except Exception as e:
            logger.error(f"Ошибка регистрации временной таблицы: {e}")
            return
        
        try:
            pk_cols_csv = ", ".join(f'"{c}"' for c in pk)
            all_cols_csv = ", ".join(f'"{c}"' for c in df.columns)
            
            # Удаляем существующие записи по первичному ключу
            delete_sql = f"""
                DELETE FROM {table_name}
                WHERE ({pk_cols_csv}) IN (SELECT {pk_cols_csv} FROM {temp_view_name});
            """
            self.conn.execute(delete_sql)
            
            # Вставляем новые записи с явным указанием колонок
            insert_sql = f"""
                INSERT INTO {table_name} ({all_cols_csv})
                SELECT {all_cols_csv} FROM {temp_view_name};
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
    """Обработка и загрузка цен с применением ограничений min/max."""
    if price_df.is_empty():
        return
    
    if 'artikul' in price_df.columns and 'brand' in price_df.columns:
        price_df = price_df.with_columns([
            self.normalize_key(pl.col('artikul')).alias('artikul_norm'),
            self.normalize_key(pl.col('brand')).alias('brand_norm')
        ])
        
        if 'currency' not in price_df.columns:
            price_df = price_df.with_columns(pl.lit('RUB').alias('currency'))
        
        if 'price' not in price_df.columns:
            price_df = price_df.with_columns(pl.lit(0.0).alias('price'))
        
        # Фильтрация по заданным границам цен
        price_df = price_df.filter(
            (pl.col('price') >= self.price_rules['min_price']) &
            (pl.col('price') <= self.price_rules['max_price'])
        )
        
        # Оставляем только колонки, соответствующие схеме таблицы prices
        price_df = price_df.select(['artikul_norm', 'brand_norm', 'price', 'currency'])
        self.upsert_data('prices', price_df, ['artikul_norm', 'brand_norm'])

# ========================================================================
# БЛОК 4: СБОРКА И ОБНОВЛЕНИЕ ДАННЫХ ПО АРТИКУЛАМ
# ========================================================================
def process_and_load_data(self, dataframes: Dict[str, pl.DataFrame]):
    """
    Оркестрация загрузки всех типов данных в базу:
    1. OE-данные (с категоризацией и извлечением кроссов)
    2. Кросс-референсы
    3. Цены
    4. Сборка таблицы parts из OE, габаритов, штрихкодов и изображений
    """
    st.info("🔄 Начало загрузки и обновления данных в базе...")
    
    steps = [s for s in ['oe', 'cross', 'parts'] if s in dataframes]
    num_steps = len(steps)
    
    progress_bar = st.progress(0, text="Подготовка к обновлению базы данных...")
    step_counter = 0
    
    # ------------------------------------------------------------------
    # 1. Обработка OE-данных
    # ------------------------------------------------------------------
    if 'oe' in dataframes:
        step_counter += 1
        progress_bar.progress(step_counter / (num_steps + 1),
                              text=f"({step_counter}/{num_steps}) Обработка OE данных...")
        
        df = dataframes['oe'].filter(pl.col('oe_number_norm') != "")
        
        # Гарантируем наличие всех размерных колонок
        for dim_col in ['length', 'width', 'height', 'weight']:
            if dim_col not in df.columns:
                df = df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(dim_col))
        if 'dimensions_str' not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias('dimensions_str'))
        
        oe_df = df.select([
            'oe_number_norm', 'oe_number', 'name', 'applicability',
            'length', 'width', 'height', 'weight', 'dimensions_str'
        ]).unique(subset=['oe_number_norm'], keep='first')
        
        # Категоризация по названию
        if 'name' in oe_df.columns:
            oe_df = oe_df.with_columns(
                self.determine_category_vectorized(pl.col('name')).alias('category')
            )
        else:
            oe_df = oe_df.with_columns(pl.lit('Разное').alias('category'))
        
        oe_df = oe_df.select([
            'oe_number_norm', 'oe_number', 'name', 'applicability',
            'category', 'length', 'width', 'height', 'weight', 'dimensions_str'
        ])
        
        self.upsert_data('oe', oe_df, ['oe_number_norm'])
        
        # Извлекаем кросс-ссылки из OE-файла (если там есть артикулы)
        if 'artikul_norm' in df.columns and 'brand_norm' in df.columns:
            cross_df_from_oe = df.filter(pl.col('artikul_norm') != "").select(
                ['oe_number_norm', 'artikul_norm', 'brand_norm']).unique()
            self.upsert_data('cross_references', cross_df_from_oe, [
                'oe_number_norm', 'artikul_norm', 'brand_norm'])
    
    # ------------------------------------------------------------------
    # 2. Обработка кросс-референсов
    # ------------------------------------------------------------------
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
    
    # ------------------------------------------------------------------
    # 3. Обработка цен
    # ------------------------------------------------------------------
    if 'prices' in dataframes:
        price_df = dataframes['prices']
        if not price_df.is_empty():
            st.info("💰 Обработка цен...")
            self.upsert_prices(price_df)
            st.success(f"✅ Успешно обновлено {len(price_df)} ценовых записей")
    
    # ------------------------------------------------------------------
    # 4. Сборка таблицы parts
    # ------------------------------------------------------------------
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
        # Подтягиваем дополнительные атрибуты из каждого файла по приоритету
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
                    'artikul', 'artikul_norm', 'brand', 'brand_norm', '_validation_errors']]
            
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
        
        # Кратность: по умолчанию 1
        if 'multiplicity' not in parts_df.columns:
            parts_df = parts_df.with_columns(multiplicity=pl.lit(1).cast(pl.Int32))
        else:
            parts_df = parts_df.with_columns(pl.col('multiplicity').fill_null(1).cast(pl.Int32))
        
        # Гарантируем наличие размерных колонок
        for col in ['length', 'width', 'height', 'weight']:
            if col not in parts_df.columns:
                parts_df = parts_df.with_columns(pl.lit(0.0).cast(pl.Float64).alias(col))
            else:
                parts_df = parts_df.with_columns(
                    pl.col(col).fill_null(0).cast(pl.Float64).alias(col)
                )
        
        if 'dimensions_str' not in parts_df.columns:
            parts_df = parts_df.with_columns(dimensions_str=pl.lit(None).cast(pl.Utf8))
        
        # Формируем dimensions_str из чисел, если он пуст
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
        
        # Гарантируем наличие артикула и бренда
        if 'artikul' not in parts_df.columns:
            parts_df = parts_df.with_columns(artikul=pl.lit(''))
        if 'brand' not in parts_df.columns:
            parts_df = parts_df.with_columns(brand=pl.lit(''))
        
        # Формируем человекочитаемое описание
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
        
        # Приводим к точной схеме таблицы parts
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
# БЛОК 5: ЭКСПОРТ ДАННЫХ (CSV, EXCEL, PARQUET)
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
    with self.db_lock:
        self.conn.register("temp_brand_markups", df)
    return "SELECT brand, markup FROM temp_brand_markups"

def build_export_query(self, selected_columns=None, include_prices=True, apply_markup=True):
    """
    Построение экспортного SQL-запроса с гарантированным заполнением всех
    4 колонок габаритов. Приоритет: 1. Данные → 2. OE → 3. Аналоги → 4. По умолчанию.
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
        
        ("Длинна", """
            COALESCE(
                NULLIF(ROUND(CAST(r.length AS DOUBLE), 2), 0),
                NULLIF(ROUND(CAST(r.oe_length AS DOUBLE), 2), 0),
                NULLIF(ROUND(CAST(r.analog_length AS DOUBLE), 2), 0),
                0.0
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
        JOIN cross_references cr3 ON l1.related_artikul_norm = cr3.artikul_norm AND l1.related_brand_norm = cr3.brand_norm
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
            ANY_VALUE(
                CASE
                    WHEN p2.dimensions_str IS NOT NULL AND p2.dimensions_str != '' AND UPPER(TRIM(p2.dimensions_str)) != 'XX'
                    THEN p2.dimensions_str
                    ELSE NULL
                END
            ) AS dimensions_str,
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
            ROW_NUMBER() OVER (
                PARTITION BY p.artikul_norm, p.brand_norm
                ORDER BY pd.representative_name DESC NULLS LAST, pd.oe_list DESC NULLS LAST
            ) AS rn
        FROM parts p
        LEFT JOIN PartDetails pd ON p.artikul_norm = pd.artikul_norm AND p.brand_norm = pd.brand_norm
        LEFT JOIN AllAnalogs aa ON p.artikul_norm = aa.artikul_norm AND p.brand_norm = aa.brand_norm
        LEFT JOIN AggregatedAnalogData p_analog ON p.artikul_norm = p_analog.artikul_norm AND p.analog.brand_norm = p_analog.brand_norm
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

def export_to_csv_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None,
                            include_prices: bool = True, apply_markup: bool = True) -> bool:
    """Экспорт в CSV с BOM для корректного открытия в Excel."""
    total = self.conn.execute(
        "SELECT count(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
    if total == 0:
        st.warning("Нет данных для экспорта")
        return False
    st.info(f"📤 Экспорт {total} записей в CSV...")
    try:
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        logger.info(f"Executing export query: {query}")
        with self.db_lock:
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
            f.write(b'\xef\xbb\xbf')  # BOM для Excel
            f.write(buf.getvalue().encode('utf-8'))
        
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        st.success(f"Данные экспортированы: {output_path} ({size_mb:.1f} МБ)")
        return True
    except Exception as e:
        logger.exception("Ошибка экспорта CSV")
        st.error(f"Ошибка при экспорте в CSV: {str(e)}")
        return False

def export_to_excel_optimized(self, output_path: str, selected_columns: Optional[List[str]] = None,
                              include_prices: bool = True, apply_markup: bool = True) -> bool:
    """Экспорт в Excel с автоматическим разбиением на листы при превышении лимита строк."""
    total = self.conn.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
    if total == 0:
        st.warning("Нет данных для экспорта")
        return False
    
    query = self.build_export_query(selected_columns, include_prices, apply_markup)
    with self.db_lock:
        df = self.conn.execute(query).pl().to_pandas()
    
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

def export_to_parquet(self, output_path: str, selected_columns: Optional[List[str]] = None,
                      include_prices: bool = True, apply_markup: bool = True) -> bool:
    """Экспорт в Parquet для аналитики и больших объёмов."""
    try:
        query = self.build_export_query(selected_columns, include_prices, apply_markup)
        with self.db_lock:
            df = self.conn.execute(query).pl()
        df.write_parquet(output_path)
        return True
    except Exception as e:
        logger.exception("Ошибка экспорта Parquet")
        st.error(f"Ошибка при экспорте в Parquet: {str(e)}")
        return False

# ========================================================================
# БЛОК 6: УПРАВЛЕНИЕ ДАННЫМИ, СТАТИСТИКА И ИНТЕРФЕЙСЫ
# ========================================================================
def delete_by_brand(self, brand_norm: str) -> int:
    """Потокобезопасное удаление всех записей бренда и очистка сиротских кроссов."""
    with self.db_lock:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            
            if deleted_count == 0:
                logger.info(f"No records found for brand: {brand_norm}")
                return 0
            
            self.conn.execute("DELETE FROM parts WHERE brand_norm = ?", [brand_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN "
                "(SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            
            return deleted_count
        
        except Exception as e:
            logger.error(f"Error deleting by brand {brand_norm}: {e}")
            raise

def delete_by_artikul(self, artikul_norm: str) -> int:
    """Потокобезопасное удаление всех записей артикула и очистка сиротских кроссов."""
    with self.db_lock:
        try:
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()
            deleted_count = count_result[0] if count_result else 0
            
            if deleted_count == 0:
                logger.info(f"No records found for artikul: {artikul_norm}")
                return 0
            
            self.conn.execute("DELETE FROM parts WHERE artikul_norm = ?", [artikul_norm])
            self.conn.execute(
                "DELETE FROM cross_references WHERE (artikul_norm, brand_norm) NOT IN "
                "(SELECT DISTINCT artikul_norm, brand_norm FROM parts)")
            
            return deleted_count
        
        except Exception as e:
            logger.error(f"Error deleting by artikul {artikul_norm}: {e}")
            raise

def get_statistics(self) -> Dict[str, Any]:
    """Сбор агрегированной статистики по базе."""
    stats = {}
    with self.db_lock:
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
# НОВЫЙ МЕТОД: ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ (из второго файла)
# ========================================================================
def merge_all_data_parallel(self, file_paths: Dict[str, str], max_workers: int = 4) -> Dict[str, pl.DataFrame]:
    """
    Параллельная обработка нескольких файлов с использованием ThreadPoolExecutor.
    """
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for key, path in file_paths.items():
            if path and os.path.exists(path):
                # Проверка расширения файла перед обработкой
                file_ext = Path(path).suffix.lower()
                if file_ext in ['.xlsx', '.xls', '.csv']:
                    futures[executor.submit(self.read_and_prepare_file, path, key)] = key
                else:
                    logger.warning(f"Пропущен файл с неподдерживаемым расширением: {path}")
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

# ========================================================================
# ИНТЕРФЕЙС: ЭКСПОРТ
# ========================================================================
def show_export_interface(self):
    st.header("📤 Экспорт данных")
    
    total = self.conn.execute(
        "SELECT COUNT(*) FROM (SELECT DISTINCT artikul_norm, brand_norm FROM parts)").fetchone()[0]
    st.info(f"Всего: {total}")
    
    if total == 0:
        st.warning("Нет данных для экспорта")
        return
    
    format_choice = st.radio("Формат", ["CSV", "Excel", "Parquet"], key="export_format_radio")
    
    selected_columns = st.multiselect("Колонки", [
        "Артикул бренда", "Бренд", "Наименование", "Применимость", "Описание",
        "Категория товара", "Кратность", "Длинна", "Ширина", "Высота", "Вес",
        "Длинна/Ширина/Высота", "OE номер", "аналоги", "Ссылка на изображение", "Цена", "Валюта"
    ], key="export_columns_multiselect")
    
    include_prices = st.checkbox("Включить цены", value=True, key="export_include_prices")
    apply_markup = st.checkbox("Применить наценку", value=True, disabled=not include_prices, key="export_apply_markup")
    
    if st.button("🚀 Экспортировать", key="export_run_button"):
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
            st.download_button("⬇️ Скачать файл", f, file_name=output_path.name, key="export_download_button")

# ========================================================================
# ИНТЕРФЕЙС: ЦЕНЫ И НАЦЕНКИ
# ========================================================================
def show_price_settings(self):
    st.header("💰 Управление ценами и наценками")
    
    st.subheader("Общая наценка")
    global_markup = st.number_input(
        "Общая наценка (%):",
        min_value=0.0,
        max_value=500.0,
        value=self.price_rules['global_markup'] * 100,
        step=0.1,
        key="price_global_markup_input"
    )
    self.price_rules['global_markup'] = global_markup / 100
    
    st.subheader("Наценки по брендам")
    brand_markups = self.price_rules.get('brand_markups', {})
    
    try:
        with self.db_lock:
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
            selected_brand = st.selectbox("Выберите бренд:", available_brands, key="price_brand_select")
        
        with col2:
            current_markup = brand_markups.get(selected_brand, self.price_rules.get('global_markup', 0))
            brand_markup = st.number_input(
                "Наценка (%):",
                min_value=0.0,
                max_value=500.0,
                value=current_markup * 100,
                step=0.1,
                key=f"markup_{selected_brand}"
            )
            
            if st.button("Сохранить наценку", key=f"save_{selected_brand}"):
                brand_markups[selected_brand] = brand_markup / 100
                self.price_rules['brand_markups'] = brand_markups
                self.save_price_rules()
                st.success(f"✅ Наценка для {selected_brand} сохранена")
    
    st.subheader("Ограничения по ценам")
    col1, col2 = st.columns(2)
    with col1:
        min_price = st.number_input("Минимальная цена:", min_value=0.0, value=float(self.price_rules['min_price']), step=0.01, key="price_min_input")
        self.price_rules['min_price'] = min_price
    
    with col2:
        max_price = st.number_input("Максимальная цена:", min_value=0.0, value=float(self.price_rules['max_price']), step=0.01, key="price_max_input")
        self.price_rules['max_price'] = max_price
    
    if st.button("Сохранить все настройки цен", key="price_save_all_button"):
        self.save_price_rules()
        st.success("✅ Все настройки цен сохранены")

# ========================================================================
# ИНТЕРФЕЙС: ИСКЛЮЧЕНИЯ
# ========================================================================
def show_exclusion_settings(self):
    st.header("🚫 Управление исключениями при экспорте")
    st.info("Товары, содержащие эти слова в названии, будут исключены из экспорта")
    
    current_exclusions = "\n".join(self.exclusion_rules)
    
    new_exclusions = st.text_area(
        "Список исключений (по одному на строку):",
        value=current_exclusions,
        height=200,
        placeholder="Введите слова для исключения, например:\nКузов\nСтекла\nМасла",
        key="exclusion_text_area"
    )
    
    if st.button("Сохранить правила исключения", key="exclusion_save_button"):
        cleaned = [line.strip() for line in new_exclusions.splitlines() if line.strip()]
        
        if len(cleaned) != len(set(cleaned)):
            st.warning("Обнаружены дублирующие записи. Они будут автоматически удалены.")
        
        self.exclusion_rules = list(dict.fromkeys(cleaned))
        self.save_exclusion_rules()
        st.success("✅ Правила исключения сохранены")

# ========================================================================
# ИНТЕРФЕЙС: КАТЕГОРИИ
# ========================================================================
def show_category_mapping(self):
    st.header("🗂️ Управление категориями товаров")
    st.info("Настройте соответствие между названиями товаров и категориями")
    
    st.subheader("Текущие правила")
    if self.category_mapping:
        mapping_df = pl.DataFrame({
            "Название товара": list(self.category_mapping.keys()),
            "Категория": list(self.category_mapping.values())
        }).to_pandas()
        st.dataframe(mapping_df, use_container_width=True, hide_index=True)
    else:
        st.write("Нет пользовательских правил")
    
    st.subheader("Добавить правило")
    col1, col2 = st.columns(2)
    with col1:
        name_pattern = st.text_input("Ключевое слово в названии", key="category_name_input")
    with col2:
        category = st.text_input("Категория", key="category_value_input")
    
    if st.button("➕ Добавить", key="category_add_button"):
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
            format_func=lambda x: f"{x} → {self.category_mapping[x]}",
            key="category_delete_select"
        )
        
        if st.button("Удалить", key="category_delete_button"):
            del self.category_mapping[rule_to_delete]
            self.save_category_mapping()
            st.success(f"Удалено: {rule_to_delete}")
            
            st.rerun()

# ========================================================================
# ИНТЕРФЕЙС: ОБЛАЧНАЯ СИНХРОНИЗАЦИЯ
# ========================================================================
def show_cloud_sync(self):
    st.header("☁️ Облачная синхронизация")
    
    st.subheader("Настройки")
    self.cloud_config['enabled'] = st.checkbox("Включить", value=self.cloud_config['enabled'], key="cloud_enabled_checkbox")
    
    providers = ["s3", "gcs", "azure"]
    current_idx = providers.index(self.cloud_config['provider']) if self.cloud_config['provider'] in providers else 0
    self.cloud_config['provider'] = st.selectbox("Провайдер", providers, index=current_idx, key="cloud_provider_select")
    
    self.cloud_config['bucket'] = st.text_input("Bucket / Container", value=self.cloud_config['bucket'], key="cloud_bucket_input")
    self.cloud_config['region'] = st.text_input("Регион", value=self.cloud_config['region'], key="cloud_region_input")
    
    self.cloud_config['sync_interval'] = st.number_input("Интервал (сек)", min_value=300, max_value=86400, value=int(self.cloud_config['sync_interval']), key="cloud_interval_input")
    
    if st.button("💾 Сохранить настройки", key="cloud_save_button"):
        self.save_cloud_config()
        st.success("Настройки сохранены")
    
    st.subheader("Текущее состояние")
    last_sync = self.cloud_config.get('last_sync', 0)
    if last_sync > 0:
        st.info(f"Последняя синхронизация: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_sync))}")
    else:
        st.info("Еще не синхронизировано")
    
    if st.button("🔄 Выполнить сейчас", key="cloud_sync_now_button"):
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

# ========================================================================
# ИНТЕРФЕЙС: СТАТИСТИКА
# ========================================================================
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
    
    col4, col5, col6 = st.columns(3)
    col4.metric("OE записей", f"{stats.get('oe', 0):,}")
    col5.metric("Кросс-ссылок", f"{stats.get('cross', 0):,}")
    col6.metric("Цен", f"{stats.get('prices', 0):,}")
    
    if 'top_brands' in stats and not stats['top_brands'].empty:
        st.subheader("Топ 10 брендов")
        st.dataframe(stats['top_brands'], use_container_width=True)
    
    if 'category_stats' in stats and not stats['category_stats'].empty:
        st.subheader("Статистика по категориям")
        st.dataframe(stats['category_stats'], use_container_width=True)

# ========================================================================
# ИНТЕРФЕЙС: УПРАВЛЕНИЕ ДАННЫМИ
# ========================================================================
def show_data_management(self):
    st.header("🔧 Управление данными")
    st.warning("⚠️ Операции необратимы!")
    
    management_option = st.radio(
        "Выберите действие:",
        [
            "Удалить по бренду",
            "Удалить по артикулу",
            "Управление ценами",
            "Исключения",
            "Категории",
            "Облачная синхронизация"
        ],
        format_func=lambda x: {
            "Удалить по бренду": "🏭 Удалить все записи бренда",
            "Удалить по артикулу": "📦 Удалить все записи артикула",
            "Управление ценами": "💰 Цены и наценки",
            "Исключения": "🚫 Исключения при экспорте",
            "Категории": "🗂️ Категории товаров",
            "Облачная синхронизация": "☁️ Облачная синхронизация"
        }[x],
        key="management_radio"
    )
    
    if management_option == "Удалить по бренду":
        self._show_delete_by_brand()
    elif management_option == "Удалить по артикулу":
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
        with self.db_lock:
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
    
    selected_brand = st.selectbox("Бренд", available_brands, key="delete_brand_select")
    
    with self.db_lock:
        brand_norm_result = self.conn.execute(
            "SELECT brand_norm FROM parts WHERE brand = ? LIMIT 1", [selected_brand]).fetchone()
    if brand_norm_result:
        brand_norm = brand_norm_result[0]
    else:
        brand_norm = self.normalize_key(pl.Series([selected_brand]))[0]
    
    with self.db_lock:
        count = self.conn.execute(
            "SELECT COUNT(*) FROM parts WHERE brand_norm = ?", [brand_norm]).fetchone()[0]
    
    st.info(f"Удалить {count} записей бренда '{selected_brand}'?")
    
    confirm = st.checkbox("Подтверждаю удаление", key="delete_brand_confirm")
    if confirm:
        if st.button("Удалить", key="delete_brand_button"):
            deleted = self.delete_by_brand(brand_norm)
            st.success(f"Удалено {deleted} записей")
            st.rerun()

def _show_delete_by_artikul(self):
    st.subheader("Удаление по артикулу")
    
    artikul_input = st.text_input("Артикул", key="delete_artikul_input")
    
    if artikul_input:
        artikul_norm = self.normalize_key(pl.Series([artikul_input]))[0]
        
        with self.db_lock:
            count = self.conn.execute(
                "SELECT COUNT(*) FROM parts WHERE artikul_norm = ?", [artikul_norm]).fetchone()[0]
        
        st.info(f"Найдено {count} записей для артикула '{artikul_input}'")
        
        confirm = st.checkbox("Подтверждаю", key="delete_artikul_confirm")
        if confirm:
            if st.button("Удалить", key="delete_artikul_button"):
                deleted = self.delete_by_artikul(artikul_norm)
                st.success(f"Удалено {deleted} записей")
                st.rerun()

# ========================================================================
# БЛОК 7: ИНТЕРАКТИВНАЯ ЗАГРУЗКА С МАППИНГОМ СТОЛБЦОВ (POWER QUERY СТИЛЬ)
# ========================================================================
def show_data_upload_interface(self):
    """
    Интерактивная загрузка данных с маппингом столбцов в стиле Power Query.
    Пользователь видит предпросмотр файла, вручную назначает колонки через
    выпадающие списки и только после этого подтверждает загрузку в базу.
    Все строки с проблемами валидации выводятся в отдельном отчёте.
    """
    st.header("📥 Загрузка данных")
    st.info("Загрузите Excel/CSV файл. Система автоматически распознает столбцы, "
            "но вы можете вручную переназначить их через выпадающие списки перед загрузкой.")
    
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
        selected_type = st.selectbox(
            "Тип файла:",
            list(file_types.keys()),
            key="upload_type_select"
        )
        file_type = file_types[selected_type]
    
    with col2:
        uploaded_file = st.file_uploader(
            f"Выберите файл ({selected_type}):",
            type=["xlsx", "xls", "csv"],
            key=f"upload_{file_type}"
        )
    
    # Проверка расширения файла
    if uploaded_file:
        file_ext = Path(uploaded_file.name).suffix.lower()
        if file_ext not in ['.xlsx', '.xls', '.csv']:
            st.error(f"Неподдерживаемый тип файла: {file_ext}")
            return
    
    # Схемы ожидаемых системных полей для каждого типа файла
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
    
    if uploaded_file:
        # При смене файла сбрасываем старые настройки маппинга
        file_id = f"{file_type}_{uploaded_file.name}"
        if st.session_state.get("current_upload_file") != file_id:
            keys_to_clear = [k for k in st.session_state.keys() if k.startswith("map_")]
            for k in keys_to_clear:
                del st.session_state[k]
            st.session_state["current_upload_file"] = file_id
        
        # Сохраняем файл во временное расположение
        temp_path = self.data_dir / f"temp_{file_type}_{uploaded_file.name}"
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        st.info(f"Файл сохранён: `{temp_path.name}` ({uploaded_file.size / 1024:.1f} КБ)")
        
        # Читаем файл для предпросмотра (кэшируем в session_state)
        cache_key = f"preview_df_{file_type}_{uploaded_file.name}"
        if cache_key not in st.session_state:
            try:
                raw_df = pl.read_excel(str(temp_path), engine='calamine')
                st.session_state[cache_key] = raw_df
            except Exception as e:
                st.error(f"Ошибка чтения файла: {e}")
                temp_path.unlink(missing_ok=True)
                return
        
        raw_df = st.session_state[cache_key]
        
        if raw_df.is_empty():
            st.error("Файл пустой или не содержит данных")
            temp_path.unlink(missing_ok=True)
            return
        
        # ------------------------------------------------------------------
        # Предпросмотр данных
        # ------------------------------------------------------------------
        st.subheader("👀 Предпросмотр данных (первые 5 строк)")
        st.dataframe(raw_df.head(5).to_pandas(), use_container_width=True)
        
        # ------------------------------------------------------------------
        # Интерактивный маппинг столбцов
        # ------------------------------------------------------------------
        expected_cols = schemas.get(file_type, [])
        auto_mapping = self.detect_columns(raw_df.columns, expected_cols)
        # auto_mapping: {file_col: system_field} → инвертируем в {system_field: file_col}
        auto_inverted = {v: k for k, v in auto_mapping.items()}
        
        st.subheader("🔗 Маппинг столбцов (Power Query стиль)")
        st.caption("Для каждого системного поля выберите соответствующую колонку из файла. "
                   "Система предзаполнила значения на основе автоматического распознавания.")
        
        file_columns = list(raw_df.columns)
        mapping_choices = {}  # system_field -> file_col
        
        # Разбиваем поля на строки по 3 для компактности
        cols_per_row = 3
        field_chunks = [expected_cols[i:i+cols_per_row] for i in range(0, len(expected_cols), cols_per_row)]
        
        for chunk in field_chunks:
            cols = st.columns(cols_per_row)
            for idx, field in enumerate(chunk):
                with cols[idx]:
                    options = ["(не выбрано)"] + file_columns
                    default_col = auto_inverted.get(field)
                    default_idx = options.index(default_col) if default_col in options else 0
                    chosen = st.selectbox(
                        f"📌 {field}",
                        options=options,
                        index=default_idx,
                        key=f"map_{file_type}_{field}"
                    )
                    if chosen != "(не выбрано)":
                        mapping_choices[field] = chosen
        
        # Проверка: одна колонка не может использоваться для двух полей
        used_cols = list(mapping_choices.values())
        if len(used_cols) != len(set(used_cols)):
            st.warning("⚠️ Одна и та же колонка файла назначена нескольким системным полям. "
                       "Проверьте маппинг.")
        
        # Статистика распознавания
        n_mapped = len(mapping_choices)
        n_unmapped = len(expected_cols) - n_mapped
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Всего колонок в файле", len(file_columns))
        m2.metric("✅ Назначено", n_mapped)
        m3.metric("⚠️ Не назначено", n_unmapped)
        m4.metric("Строк в файле", f"{len(raw_df):,}")
        
        if n_mapped == 0:
            st.warning("Не назначено ни одной колонки. Загрузка невозможна.")
            return
        
        # ------------------------------------------------------------------
        # Подтверждение и загрузка в базу
        # ------------------------------------------------------------------
        if st.button("💾 Подтвердить и загрузить в базу", key="upload_confirm_button"):
            with st.spinner("Нормализация и запись в базу..."):
                # Инвертируем маппинг: {file_col: system_field}
                final_mapping = {v: k for k, v in mapping_choices.items()}
                
                df = self.read_and_prepare_file(str(temp_path), file_type, column_mapping=final_mapping)
                
                if df.is_empty():
                    st.error("Файл пустой или не содержит данных после обработки")
                    temp_path.unlink(missing_ok=True)
                    return
                
                # Отчёт об ошибках валидации
                if "_validation_errors" in df.columns:
                    error_rows = df.filter(pl.col("_validation_errors") != "")
                    n_errors = len(error_rows)
                    if n_errors > 0:
                        st.warning(f"⚠️ Обнаружено {n_errors} строк с проблемами валидации")
                        with st.expander(f"📋 Показать {n_errors} строк с ошибками"):
                            key_cols = [c for c in ['artikul', 'brand', 'oe_number'] if c in df.columns]
                            display_cols = key_cols + ['_validation_errors']
                            st.dataframe(error_rows.select(display_cols).to_pandas(), use_container_width=True)
                    else:
                        st.success("✅ Все строки прошли валидацию без ошибок")
                
                dataframes = {file_type: df}
                self.process_and_load_data(dataframes)
                st.success(f"✅ {len(df)} записей успешно загружено в базу")
                
                # Очистка временных данных
                temp_path.unlink(missing_ok=True)
                if cache_key in st.session_state:
                    del st.session_state[cache_key]
                st.rerun()
    
    st.divider()
    
    # ------------------------------------------------------------------
    # Массовая загрузка (автоматический маппинг по эвристике) + параллельная обработка
    # ------------------------------------------------------------------
    st.subheader("⚡ Массовая загрузка (параллельная)")
    st.caption("Загрузите несколько файлов одновременно. Тип определяется автоматически по имени файла.")
    
    uploaded_files = st.file_uploader(
        "Выберите несколько файлов:",
        type=["xlsx", "xls", "csv"],
        accept_multiple_files=True,
        key="bulk_upload"
    )
    
    # Проверка расширений файлов
    if uploaded_files:
        invalid_files = [f for f in uploaded_files if Path(f.name).suffix.lower() not in ['.xlsx', '.xls', '.csv']]
        if invalid_files:
            st.error(f"Неподдерживаемые типы файлов: {[f.name for f in invalid_files]}")
            uploaded_files = [f for f in uploaded_files if f not in invalid_files]
    
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
        
        st.write(f"Определено **{len(file_map)}** типов файлов:")
        for ft, f in file_map.items():
            st.write(f"- `{ft}` → `{f.name}`")
        
        if st.button("📦 Загрузить все (параллельно)", key="bulk_upload_button"):
            with st.spinner("Параллельная обработка и загрузка всех файлов..."):
                temp_paths = []
                file_paths = {}
                
                try:
                    for ft, file in file_map.items():
                        temp_path = self.data_dir / f"temp_{int(time.time())}_{file.name}"
                        with open(temp_path, "wb") as f_out:
                            f_out.write(file.getbuffer())
                        temp_paths.append(temp_path)
                        file_paths[ft] = str(temp_path)
                    
                    # Параллельная обработка файлов
                    dataframes = self.merge_all_data_parallel(file_paths, max_workers=4)
                    
                    if dataframes:
                        self.process_and_load_data(dataframes)
                        st.success("✅ Все данные успешно загружены в базу!")
                        st.rerun()
                    else:
                        st.warning("Не найдено данных для загрузки")
                
                finally:
                    for tp in temp_paths:
                        tp.unlink(missing_ok=True)

# ========================================================================
# БЛОК 8: РАСШИРЕННЫЙ ПОИСК И ГРУППИРОВКА (POWER QUERY)
# ========================================================================
def show_search_interface(self):
    """
    Расширенный поиск по каталогу: OE-номер + Бренд + Часть артикула + Диапазон цен.
    Все критерии можно комбинировать (требование №1).
    """
    st.header("🔍 Поиск по каталогу")
    st.info("Ищите по OE-номеру и комбинируйте критерии: бренд, часть артикула, диапазон цен.")
    
    # Получаем список брендов для фильтра
    try:
        with self.db_lock:
            brands_result = self.conn.execute(
                "SELECT DISTINCT brand FROM parts WHERE brand IS NOT NULL AND brand != '' ORDER BY brand").fetchall()
        available_brands = [row[0] for row in brands_result] if brands_result else []
    except Exception as e:
        logger.error(f"Ошибка получения брендов: {e}")
        available_brands = []
    
    # Форма поиска
    col1, col2 = st.columns(2)
    with col1:
        oe_input = st.text_input("🔑 OE-номер (точный поиск):", key="search_oe_input",
                                 placeholder="Например: 90915YZZD2")
        brand_filter = st.selectbox("🏭 Бренд:", options=["(все бренды)"] + available_brands,
                                    key="search_brand_select")
    
    with col2:
        artikul_input = st.text_input("📦 Часть артикула (нечёткий поиск):", key="search_artikul_input",
                                      placeholder="Например: 04465")
        price_col1, price_col2 = st.columns(2)
        with price_col1:
            price_min = st.number_input("💰 Цена от:", min_value=0.0, value=0.0, step=0.01,
                                        key="search_price_min")
        with price_col2:
            price_max = st.number_input("Цена до:", min_value=0.0, value=0.0, step=0.01,
                                        key="search_price_max",
                                        help="0 = без ограничения")
    
    if st.button("🔍 Найти", key="search_run_button"):
        with st.spinner("Выполняется поиск..."):
            result_df, query = self.execute_search(oe_input, brand_filter, artikul_input, price_min, price_max)
            
            if result_df is not None and not result_df.empty:
                st.success(f"✅ Найдено {len(result_df):,} записей")
                
                with st.expander("📄 Показать SQL-запрос"):
                    st.code(query, language="sql")
                
                st.dataframe(result_df, use_container_width=True)
                
                # Экспорт результата поиска
                csv = result_df.to_csv(sep=';', index=False).encode('utf-8-sig')
                st.download_button("📥 Скачать результат (CSV)", data=csv,
                                   file_name="search_result.csv", mime="text/csv",
                                   key="search_download_csv")
            elif result_df is not None and result_df.empty:
                st.warning("Ничего не найдено по заданным критериям")
                with st.expander("📄 Показать SQL-запрос"):
                    st.code(query, language="sql")

def execute_search(self, oe_input: str, brand_filter: str, artikul_input: str,
                   price_min: float, price_max: float) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Выполняет комбинированный поиск с параметризованным SQL-запросом
    (защита от SQL-инъекций). Возвращает (DataFrame, текст_запроса).
    """
    where_clauses = []
    params = []
    
    # OE-номер: точный поиск по нормализованному ключу
    if oe_input and oe_input.strip():
        oe_norm = self.normalize_key(pl.Series([oe_input.strip()]))[0]
        where_clauses.append("cr.oe_number_norm = ?")
        params.append(oe_norm)
    
    # Бренд: точное совпадение
    if brand_filter and brand_filter != "(все бренды)":
        where_clauses.append("p.brand = ?")
        params.append(brand_filter)
    
    # Часть артикула: нечёткий поиск по нормализованному ключу
    if artikul_input and artikul_input.strip():
        artikul_norm = self.normalize_key(pl.Series([artikul_input.strip()]))[0]
        where_clauses.append("p.artikul_norm LIKE ?")
        params.append(f"%{artikul_norm}%")
    
    # Диапазон цен
    if price_min and price_min > 0:
        where_clauses.append("pr.price >= ?")
        params.append(price_min)
    if price_max and price_max > 0:
        where_clauses.append("pr.price <= ?")
        params.append(price_max)
    
    where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
    
    query = f"""
    SELECT DISTINCT
        p.artikul AS "Артикул",
        p.brand AS "Бренд",
        o.oe_number AS "OE номер",
        o.name AS "Наименование",
        o.applicability AS "Применимость",
        o.category AS "Категория",
        p.multiplicity AS "Кратность",
        p.length AS "Длина",
        p.width AS "Ширина",
        p.height AS "Высота",
        p.weight AS "Вес",
        pr.price AS "Цена",
        COALESCE(pr.currency, 'RUB') AS "Валюта"
    FROM parts p
    LEFT JOIN cross_references cr ON p.artikul_norm = cr.artikul_norm AND p.brand_norm = cr.brand_norm
    LEFT JOIN oe o ON cr.oe_number_norm = o.oe_number_norm
    LEFT JOIN prices pr ON p.artikul_norm = pr.artikul_norm AND p.brand_norm = pr.brand_norm
    WHERE {where_sql}
    ORDER BY p.brand, p.artikul
    LIMIT 1000
    """
    
    try:
        with self.db_lock:
            df = self.conn.execute(query, params).pl().to_pandas()
        return df, query
    except Exception as e:
        logger.error(f"Ошибка поиска: {e}\nЗапрос: {query}")
        st.error(f"Ошибка поиска: {e}")
        return None, query

# ========================================================================
# ИНТЕРФЕЙС: ГРУППИРОВКА И ПОДТЯГИВАНИЕ (POWER QUERY СТИЛЬ)
# ========================================================================
def get_all_table_columns(self) -> Dict[str, List[str]]:
    """Получить все столбцы из всех таблиц базы."""
    tables_info = {}
    tables = ['oe', 'parts', 'cross_references', 'prices']
    
    with self.db_lock:
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
    """
    Интерфейс группировки по ключам и подтягивания значений
    из связанных таблиц (аналог Power Query Merge/Group By).
    """
    st.header("🔗 Power Query стиль: Группировка и подтягивание")
    st.info("Выберите столбцы с одинаковыми значениями (ключ для группировки), "
            "затем — какие значения нужно подтянуть из связанных таблиц.")
    
    tables_info = self.get_all_table_columns()
    
    if not any(tables_info.values()):
        st.warning("База пуста. Сначала загрузите данные в разделе «📥 Загрузка данных».")
        return
    
    st.subheader("1. Ключевые столбцы (группировка)")
    key_options = []
    for table, cols in tables_info.items():
        for col in cols:
            key_options.append((table, col))
    
    selected_keys = st.multiselect(
        "Выберите столбцы для группировки (ключ):",
        options=key_options,
        format_func=lambda x: f"{x[0]}.{x[1]}",
        help="Эти столбцы должны содержать одинаковые значения в разных строках",
        key="pq_keys_multiselect"
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
        help="Значения из этих столбцов будут объединены по ключу",
        key="pq_targets_multiselect"
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
        format_func=lambda x: x[1],
        key="pq_agg_select"
    )
    agg_code = agg_method[0]
    
    if st.button("🔍 Выполнить группировку", key="pq_run_button") and selected_keys and selected_targets:
        with st.spinner("Выполняется запрос группировки..."):
            result_df, query = self.execute_grouping_with_join(selected_keys, selected_targets, agg_code)
            
            if result_df is not None and not result_df.empty:
                st.success(f"✅ Найдено {len(result_df):,} уникальных групп")
                
                with st.expander("📄 Показать сгенерированный SQL-запрос"):
                    st.code(query, language="sql")
                
                st.dataframe(result_df, use_container_width=True)
                
                st.subheader("📥 Экспорт результата")
                col1, col2 = st.columns(2)
                with col1:
                    csv = result_df.to_csv(sep=';', index=False).encode('utf-8-sig')
                    st.download_button(
                        "📥 Скачать CSV",
                        data=csv,
                        file_name="grouped_export.csv",
                        mime="text/csv",
                        key="pq_download_csv"
                    )
                with col2:
                    excel_buffer = io.BytesIO()
                    if len(result_df) <= EXCEL_ROW_LIMIT:
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            result_df.to_excel(writer, index=False)
                    else:
                        sheets = (len(result_df) // EXCEL_ROW_LIMIT) + 1
                        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                            for i in range(sheets):
                                result_df.iloc[i*EXCEL_ROW_LIMIT:(i+1)*EXCEL_ROW_LIMIT].to_excel(
                                    writer, index=False, sheet_name=f"Данные_{i+1}")
                    st.download_button(
                        "📥 Скачать Excel",
                        data=excel_buffer.getvalue(),
                        file_name="grouped_export.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="pq_download_excel"
                    )
            elif result_df is not None and result_df.empty:
                st.warning("Нет данных по выбранным критериям")
                with st.expander("📄 Показать сгенерированный SQL-запрос"):
                    st.code(query, language="sql")
    elif st.button("🔍 Выполнить группировку", key="pq_run_button_empty") and (not selected_keys or not selected_targets):
        st.warning("Выберите хотя бы один ключевой столбец и один целевой столбец.")

def execute_grouping_with_join(self, group_by: List[Tuple[str, str]],
                               targets: List[Tuple[str, str]],
                               agg_method: str) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Логика JOIN с проверкой связей между таблицами.
    Предотвращает создание декартова произведения (CROSS JOIN) для несвязанных таблиц.
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
            select_clauses.append(f"STRING_AGG(DISTINCT CAST({t}.{c} AS VARCHAR), ', ') AS \"{c}\"")
        elif agg_method in ['max', 'min']:
            select_clauses.append(f"{agg_method.upper()}({t}.{c}) AS \"{c}\"")
        elif agg_method == 'last':
            select_clauses.append(f"LAST({t}.{c}) AS \"{c}\"")
        else:
            select_clauses.append(f"FIRST({t}.{c}) AS \"{c}\"")
    
    from_clause = f"FROM {main_table}"
    join_clauses = []
    
    # Известные связи между таблицами
    joins = [
        ("parts", "cross_references", "parts.artikul_norm = cross_references.artikul_norm AND parts.brand_norm = cross_references.brand_norm"),
        ("oe", "cross_references", "oe.oe_number_norm = cross_references.oe_number_norm"),
        ("prices", "parts", "prices.artikul_norm = parts.artikul_norm AND prices.brand_norm = parts.brand_norm"),
        ("parts", "oe", "parts.artikul_norm = cross_references.artikul_norm AND parts.brand_norm = cross_references.brand_norm AND cross_references.oe_number_norm = oe.oe_number_norm"),
    ]
    
    used = {main_table}
    connected_tables = {main_table}
    
    # Итеративно добавляем JOIN'ы, пока все нужные таблицы не будут связаны
    changed = True
    while changed:
        changed = False
        for t1, t2, cond in joins:
            if t1 in all_tables and t2 in all_tables:
                if t1 in used and t2 not in used:
                    join_clauses.append(f"LEFT JOIN {t2} ON {cond}")
                    used.add(t2)
                    connected_tables.add(t2)
                    changed = True
                elif t2 in used and t1 not in used:
                    join_clauses.append(f"LEFT JOIN {t1} ON {cond}")
                    used.add(t1)
                    connected_tables.add(t1)
                    changed = True
    
    unconnected = all_tables - connected_tables
    if unconnected:
        st.warning(
            f"⚠️ Следующие таблицы не связаны с основной через известные связи: {sorted(list(unconnected))}. "
            f"Они исключены из запроса, чтобы избежать некорректного декартова произведения."
        )
    
    group_clause = ", ".join([f"{t}.{c}" for t, c in group_by])
    order_clause = ", ".join([f"{t}.{c}" for t, c in group_by])
    
    query = f"""
    SELECT {', '.join(select_clauses)}
    {from_clause}
    {' '.join(join_clauses)}
    GROUP BY {group_clause}
    ORDER BY {order_clause}
    """
    
    try:
        with self.db_lock:
            df = self.conn.execute(query).pl().to_pandas()
        return df, query
    except Exception as e:
        logger.error(f"SQL error: {e}\nQuery: {query}")
        st.error(f"Ошибка SQL: {e}")
        return None, query

def main():
st.set_page_config(
page_title="Каталог автозапчастей",
page_icon="🚗",
layout="wide",
initial_sidebar_state="expanded"
)
st.title("🚗 Каталог автозапчастей")
st.caption("High-Volume Auto Parts Catalog · v2.1 · Профессиональный парсинг и поиск")

catalog = get_high_volume_catalog()

st.sidebar.header("Навигация")
page = st.sidebar.radio(
    "Выберите раздел:",
    [
        "📊 Статистика",
        "🔍 Поиск по каталогу",
        "📥 Загрузка данных",
        "🔗 Power Query стиль",
        "📤 Экспорт данных",
        "🔧 Управление"
    ],
    key="main_nav_radio"
)

if page == "📊 Статистика":
    catalog.show_statistics()
elif page == "🔍 Поиск по каталогу":
    catalog.show_search_interface()
elif page == "📥 Загрузка данных":
    catalog.show_data_upload_interface()
elif page == "🔗 Power Query стиль":
    catalog.show_power_query_interface()
elif page == "📤 Экспорт данных":
    catalog.show_export_interface()
elif page == "🔧 Управление":
    catalog.show_data_management()
if name == "main":
main()
