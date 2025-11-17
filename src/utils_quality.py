import pandas as pd
from datetime import datetime
import re

def calculate_completeness(df, column):
    """
    Calcula el porcentaje de valores no nulos en una columna.
    
    Args:
        df (pd.DataFrame): DataFrame
        column (str): Nombre de la columna
        
    Returns:
        float: Porcentaje (0-100) de valores no nulos
    """
    if column not in df.columns:
        return 0.0
    
    total = len(df)
    if total == 0:
        return 0.0
    
    non_null = df[column].notna().sum()
    return (non_null / total) * 100

def validate_iso_date(date_value):
    """
    Verifica si un valor es una fecha válida en formato ISO-8601.
    
    Args:
        date_value: Valor a validar
        
    Returns:
        bool: True si es fecha ISO válida
        
    Acepta:
        - YYYY
        - YYYY-MM
        - YYYY-MM-DD
    """
    if pd.isna(date_value):
        return False
    
    date_str = str(date_value).strip()
    if not date_str:
        return False

    # YYYY-MM-DD
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        try:
            datetime.fromisoformat(date_str)
            return True
        except (ValueError, OverflowError):
            return False

    # YYYY-MM
    if re.match(r'^\d{4}-\d{2}$', date_str):
        try:
            # Completamos con día 01 para poder parsear
            datetime.fromisoformat(date_str + '-01')
            return True
        except (ValueError, OverflowError):
            return False

    # YYYY
    if re.match(r'^\d{4}$', date_str):
        try:
            year = int(date_str)
            return 1000 <= year <= 9999
        except (ValueError, OverflowError):
            return False

    return False

def validate_bcp47_language(lang):
    """
    Verifica si un código de idioma es válido según BCP-47 (simplificado).
    
    Args:
        lang (str): Código de idioma
        
    Returns:
        bool: True si parece válido
        
    Acepta:
        - es, en, fr (2 letras)
        - en-US, pt-BR (2 letras + guión + 2 letras)
    """
    if pd.isna(lang):
        return False
    
    lang_str = str(lang).strip().lower()
    if not lang_str:
        return False

    # Patrón BCP-47: 2-3 letras de idioma, opcionalmente seguidas de subtags separados por guiones
    # Ejemplos válidos: es, en, fr, en-us, pt-br, zh-hant, sr-latn-rs
    pattern = r'^[a-z]{2,3}(-[a-z0-9]{2,8})*$'

    return bool(re.match(pattern, lang_str))

def validate_iso4217_currency(currency):
    """
    Verifica si un código de moneda es válido según ISO-4217 (simplificado).
    
    Args:
        currency (str): Código de moneda
        
    Returns:
        bool: True si parece válido
        
    Acepta códigos de 3 letras: USD, EUR, GBP, etc.
    """
    if pd.isna(currency):
        return False
    
    currency_str = str(currency).strip().upper()
    
    # ISO-4217: 3 letras mayúsculas
    common_currencies = {
        'USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 
        'CNY', 'SEK', 'NZD', 'MXN', 'BRL', 'ARS', 'CLP'
    }
    
    return currency_str in common_currencies or re.match(r'^[A-Z]{3}$', currency_str)

def check_data_quality(df, required_columns=None):
    """
    Genera un reporte de calidad de datos para un DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame a analizar
        required_columns (list): Columnas que deben existir
        
    Returns:
        dict: Reporte de calidad
    """
    report = {
        'row_count': len(df),
        'column_count': len(df.columns),
        'completeness': {},
        'duplicates': {},
        'data_types': {}
    }
    
    # Completitud por columna
    for col in df.columns:
        report['completeness'][col] = calculate_completeness(df, col)
    
    # Verificar columnas requeridas
    if required_columns:
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            report['missing_required_columns'] = list(missing_cols)
    
    # Detectar duplicados
    if 'book_id' in df.columns:
        n_duplicates = df['book_id'].duplicated().sum()
        report['duplicates']['book_id'] = int(n_duplicates)
    
    # Tipos de datos
    for col in df.columns:
        report['data_types'][col] = str(df[col].dtype)
    
    return report

def validate_date_column(df, column):
    """
    Valida una columna de fechas ISO-8601.
    
    Args:
        df (pd.DataFrame): DataFrame
        column (str): Nombre de la columna
        
    Returns:
        dict: Estadísticas de validación
    """
    if column not in df.columns:
        return {'error': f'Column {column} not found'}
    
    total = df[column].notna().sum()
    if total == 0:
        return {'valid_count': 0, 'valid_percentage': 0.0, 'total_non_null': 0}
    
    valid_count = df[column].apply(validate_iso_date).sum()
    
    return {
        'total_non_null': int(total),
        'valid_count': int(valid_count),
        'valid_percentage': (valid_count / total) * 100
    }

def validate_language_column(df, column):
    """
    Valida una columna de códigos de idioma BCP-47.
    
    Args:
        df (pd.DataFrame): DataFrame
        column (str): Nombre de la columna
        
    Returns:
        dict: Estadísticas de validación
    """
    if column not in df.columns:
        return {'error': f'Column {column} not found'}
    
    total = df[column].notna().sum()
    if total == 0:
        return {'valid_count': 0, 'valid_percentage': 0.0, 'total_non_null': 0}
    
    valid_count = df[column].apply(validate_bcp47_language).sum()
    
    return {
        'total_non_null': int(total),
        'valid_count': int(valid_count),
        'valid_percentage': (valid_count / total) * 100
    }

def validate_currency_column(df, column):
    """
    Valida una columna de códigos de moneda ISO-4217.
    
    Args:
        df (pd.DataFrame): DataFrame
        column (str): Nombre de la columna
        
    Returns:
        dict: Estadísticas de validación
    """
    if column not in df.columns:
        return {'error': f'Column {column} not found'}
    
    total = df[column].notna().sum()
    if total == 0:
        return {'valid_count': 0, 'valid_percentage': 0.0, 'total_non_null': 0}
    
    valid_count = df[column].apply(validate_iso4217_currency).sum()
    
    return {
        'total_non_null': int(total),
        'valid_count': int(valid_count),
        'valid_percentage': (valid_count / total) * 100
    }