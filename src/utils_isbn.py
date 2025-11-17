### src/utils_isbn.py
### Utilidades para validación y limpieza de ISBNs

import re
import pandas as pd

def clean_isbn(isbn):
    """
    Limpia un ISBN permitiendo solo dígitos y X/x.

    - Convierte el valor de entrada a string.
    - Elimina espacios, guiones y cualquier carácter que no sea dígito o X/x.
    - Devuelve el ISBN limpio en mayúsculas o None si no queda nada.

    Args:
        isbn (str|int|float): ISBN sucio o potencialmente nulo.

    Returns:
        str | None: ISBN limpio (solo dígitos y X, en mayúsculas) o None si es inválido.
    """
    # Nulos o NaN
    if isbn is None or (isinstance(isbn, float) and pd.isna(isbn)):
        return None

    # Convertir a string y limpiar espacios
    s = str(isbn).strip()
    if not s:
        return None

    # Solo permitir dígitos y X/x (para ISBN-10)
    s = re.sub(r'[^0-9Xx]', '', s)

    # Normalizar X a mayúscula
    s = s.upper()

    return s if s else None

def validate_isbn10(isbn):
    """
    Valida un ISBN-10 usando el algoritmo de checksum.
    
    Args:
        isbn (str): ISBN-10 a validar
        
    Returns:
        bool: True si es válido, False si no
        
    Example:
        >>> validate_isbn10("0134685997")
        True
    """
    if not isbn:
        return False
    
    isbn = clean_isbn(isbn)
    
    if not isbn or len(isbn) != 10:
        return False
    
    try:
        # Calcular checksum
        total = 0
        for i in range(9):
            total += int(isbn[i]) * (10 - i)
        
        # El último dígito puede ser X (= 10)
        last = 10 if isbn[9].upper() == 'X' else int(isbn[9])
        total += last
        
        # Válido si divisible por 11
        return total % 11 == 0
    except (ValueError, IndexError):
        return False

def validate_isbn13(isbn):
    """
    Valida un ISBN-13 usando el algoritmo de checksum.
    
    Args:
        isbn (str): ISBN-13 a validar
        
    Returns:
        bool: True si es válido, False si no
        
    Example:
        >>> validate_isbn13("9780134685991")
        True
    """
    if not isbn:
        return False
    
    isbn = clean_isbn(isbn)
    
    if not isbn or len(isbn) != 13:
        return False
    
    try:
        # Convertir a lista de enteros
        digits = [int(d) for d in isbn]
        
        # Calcular checksum: suma alterna de 1x y 3x
        checksum = sum(digits[i] if i % 2 == 0 else digits[i] * 3 
                      for i in range(12))
        
        # El dígito de control debe hacer que la suma sea múltiplo de 10
        check_digit = (10 - (checksum % 10)) % 10
        
        return check_digit == digits[12]
    except (ValueError, IndexError):
        return False

def isbn10_to_isbn13(isbn10):
    """
    Convierte un ISBN-10 a ISBN-13.
    
    Args:
        isbn10 (str): ISBN-10 válido
        
    Returns:
        str: ISBN-13 o None si el input no es válido
        
    Example:
        >>> isbn10_to_isbn13("0134685997")
        "9780134685991"
    """
    if not validate_isbn10(isbn10):
        return None
    
    isbn10 = clean_isbn(isbn10)[:9]  # Quitar el check digit
    
    # Añadir prefijo 978
    isbn13_base = '978' + isbn10
    
    # Calcular nuevo check digit
    digits = [int(d) for d in isbn13_base]
    checksum = sum(digits[i] if i % 2 == 0 else digits[i] * 3 
                  for i in range(12))
    check_digit = (10 - (checksum % 10)) % 10
    
    return isbn13_base + str(check_digit)

def normalize_isbn(isbn):
    """
    Normaliza un ISBN: limpia y valida.
    
    Args:
        isbn (str): ISBN a normalizar
        
    Returns:
        dict: {'isbn': str, 'valid': bool, 'type': 'isbn10'|'isbn13'|None}
        
    Example:
        >>> normalize_isbn("978-0-13-468599-1")
        {'isbn': '9780134685991', 'valid': True, 'type': 'isbn13'}
    """
    cleaned = clean_isbn(isbn)
    
    if not cleaned:
        return {'isbn': None, 'valid': False, 'type': None}
    
    if len(cleaned) == 10:
        return {
            'isbn': cleaned,
            'valid': validate_isbn10(cleaned),
            'type': 'isbn10'
        }
    elif len(cleaned) == 13:
        return {
            'isbn': cleaned,
            'valid': validate_isbn13(cleaned),
            'type': 'isbn13'
        }
    else:
        return {'isbn': cleaned, 'valid': False, 'type': None}