###
### src/integrate_pipeline.py
###

# ============================================================
# 0. IMPORTS
# ============================================================
import pandas as pd
import json
import os
import hashlib
from datetime import datetime
import sys
from pathlib import Path

# Ruta base del proyecto (carpeta que contiene src/, landing/, docs/, standard/)
BASE_DIR = Path(__file__).resolve().parent.parent
LANDING_DIR = BASE_DIR / "landing"
STANDARD_DIR = BASE_DIR / "standard"
DOCS_DIR = BASE_DIR / "docs"

# Importar nuestros utils
from utils_isbn import normalize_isbn, isbn10_to_isbn13, validate_isbn13
from utils_quality import (
    check_data_quality,
    validate_date_column,
    validate_language_column,
    validate_currency_column,
    calculate_completeness
)

# ============================================================
# 1. LECTURA
# ============================================================

def leer_goodreads():
    """Lee el JSON de Goodreads"""
    print("📖 Leyendo landing/goodreads_books.json...")

    filepath = LANDING_DIR / "goodreads_books.json"
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    df = pd.DataFrame(data['books'])
    print(f"   ✓ {len(df)} libros de Goodreads")
    return df


def leer_googlebooks():
    """Lee el CSV de Google Books"""
    print("📖 Leyendo landing/googlebooks_books.csv...")

    filepath = LANDING_DIR / "googlebooks_books.csv"
    df = pd.read_csv(filepath, encoding='utf-8')
    print(f"   ✓ {len(df)} registros de Google Books")
    return df

# ============================================================
# 2. STAGING (mapeo de columnas a esquema común)
# ============================================================

def mapear_goodreads(df_gr):
    """Mapea columnas de Goodreads a esquema estándar"""
    print("🔄 Mapeando columnas de Goodreads...")

    df_staging = pd.DataFrame({
        'source_name': 'goodreads',
        'source_row_number': df_gr['row_number'],
        'titulo': df_gr['title'],
        'autor': df_gr['author'],
        'rating': df_gr['rating'],
        'ratings_count': df_gr['ratings_count'],
        'anio_publicacion': df_gr.get('published_year'),
        'isbn10': df_gr.get('isbn10'),
        'isbn13': df_gr.get('isbn13'),
        'book_url': df_gr.get('book_url'),
        # Campos que GR no tiene
        'editorial': None,
        'idioma': None,
        'paginas': None,
        'categorias': None,
        'precio': None,
        'moneda': None,
        'subtitulo': None,
        'autores_lista': None,   # NUEVO: para que el merge genere autores_lista_gr / autores_lista_gb
    })
    return df_staging


def mapear_googlebooks(df_gb):
    """Mapea columnas de Google Books a esquema estándar"""
    print("🔄 Mapeando columnas de Google Books...")

    # Extraer año de published_date (puede ser YYYY, YYYY-MM, YYYY-MM-DD)
    def extraer_anio(date_str):
        if pd.isna(date_str):
            return None
        try:
            return int(str(date_str)[:4])
        except Exception:
            return None

    # Separar primer autor de la lista (viene como "autor1|autor2|...")
    def extraer_primer_autor(authors_str):
        if pd.isna(authors_str):
            return None
        return str(authors_str).split('|')[0].strip()

    df_staging = pd.DataFrame({
        'source_name': 'googlebooks',
        'source_row_number': df_gb['row_number'],
        'titulo': df_gb['title'],
        'subtitulo': df_gb['subtitle'],
        'autor': df_gb['authors'].apply(extraer_primer_autor),
        'autores_lista': df_gb['authors'],  # Lista completa
        'editorial': df_gb['publisher'],
        'anio_publicacion': df_gb['published_date'].apply(extraer_anio),
        'fecha_publicacion': df_gb['published_date'],
        'idioma': df_gb['language'],
        'paginas': df_gb['page_count'],
        'categorias': df_gb['categories'],
        'isbn10': df_gb['isbn10'],
        'isbn13': df_gb['isbn13'],
        'precio': df_gb['price_amount'],
        'moneda': df_gb['price_currency'],
        # Campos que GB no tiene
        'rating': None,
        'ratings_count': None,
        'book_url': None,
    })

    return df_staging

# ============================================================
# 3. NORMALIZACIÓN
# ============================================================

def normalizar_titulo(titulo):
    """Normaliza título para matching: minúsculas, sin tildes, sin signos"""
    if pd.isna(titulo):
        return None

    import unicodedata

    # Quitar tildes
    titulo_str = str(titulo)
    titulo_nfd = unicodedata.normalize('NFD', titulo_str)
    titulo_sin_tildes = ''.join(
        c for c in titulo_nfd if unicodedata.category(c) != 'Mn'
    )

    # Minúsculas y quitar caracteres especiales (mantener espacios y letras)
    titulo_norm = titulo_sin_tildes.lower()
    titulo_norm = ''.join(
        c if c.isalnum() or c.isspace() else ' ' for c in titulo_norm
    )

    # Espacios múltiples → uno solo
    titulo_norm = ' '.join(titulo_norm.split())

    return titulo_norm


def normalizar_autor(autor):
    """Normaliza autor: minúsculas, sin tildes"""
    if pd.isna(autor):
        return None

    import unicodedata

    autor_str = str(autor)
    autor_nfd = unicodedata.normalize('NFD', autor_str)
    autor_sin_tildes = ''.join(
        c for c in autor_nfd if unicodedata.category(c) != 'Mn'
    )
    autor_norm = autor_sin_tildes.lower().strip()

    return autor_norm


def normalizar_fecha(fecha):
    """Normaliza fecha a ISO-8601 (YYYY-MM-DD)"""
    if pd.isna(fecha):
        return None

    fecha_str = str(fecha).strip()

    # Si ya está en formato YYYY-MM-DD, ok
    if len(fecha_str) == 10 and fecha_str[4] == '-' and fecha_str[7] == '-':
        return fecha_str

    # Si es YYYY-MM, añadir -01
    if len(fecha_str) == 7 and fecha_str[4] == '-':
        return fecha_str + '-01'

    # Si es YYYY, añadir -01-01
    if len(fecha_str) == 4 and fecha_str.isdigit():
        return fecha_str + '-01-01'

    return None


def normalizar_idioma(idioma):
    """Normaliza idioma a BCP-47 minúsculas"""
    if pd.isna(idioma):
        return None

    return str(idioma).lower().strip()


def normalizar_moneda(moneda):
    """Normaliza moneda a ISO-4217 mayúsculas"""
    if pd.isna(moneda):
        return None

    return str(moneda).upper().strip()


def normalizar_df(df):
    """Aplica todas las normalizaciones a un DataFrame"""
    print("🔧 Normalizando datos...")

    df_norm = df.copy()

    # Títulos y autores normalizados (para matching)
    df_norm['titulo_normalizado'] = df_norm['titulo'].apply(normalizar_titulo)
    df_norm['autor_normalizado'] = df_norm['autor'].apply(normalizar_autor)

    # Fechas ISO-8601
    if 'fecha_publicacion' in df_norm.columns:
        df_norm['fecha_publicacion'] = df_norm['fecha_publicacion'].apply(
            normalizar_fecha
        )

    # Idioma BCP-47
    if 'idioma' in df_norm.columns:
        df_norm['idioma'] = df_norm['idioma'].apply(normalizar_idioma)

    # Moneda ISO-4217
    if 'moneda' in df_norm.columns:
        df_norm['moneda'] = df_norm['moneda'].apply(normalizar_moneda)

    # ISBNs limpios
    df_norm['isbn10'] = df_norm['isbn10'].apply(
        lambda x: normalize_isbn(x)['isbn'] if pd.notna(x) else None
    )
    df_norm['isbn13'] = df_norm['isbn13'].apply(
        lambda x: normalize_isbn(x)['isbn'] if pd.notna(x) else None
    )

    return df_norm

# ============================================================
# 4. GENERAR book_id CANÓNICO
# ============================================================

def generar_book_id(row):
    """
    Genera un book_id canónico:
    1. Si tiene ISBN13 válido → usar ISBN13
    2. Si tiene ISBN10 válido → convertir a ISBN13
    3. Si no tiene ISBN → hash de (titulo_norm + autor_norm + editorial + año)
    """
    # Prioridad 1: ISBN13
    if pd.notna(row['isbn13']) and validate_isbn13(row['isbn13']):
        return row['isbn13']

    # Prioridad 2: ISBN10 → convertir a ISBN13
    if pd.notna(row['isbn10']):
        isbn13_convertido = isbn10_to_isbn13(row['isbn10'])
        if isbn13_convertido:
            return isbn13_convertido

    # Prioridad 3: Hash de campos clave
    titulo = row.get('titulo_normalizado', '') or ''
    autor = row.get('autor_normalizado', '') or ''
    editorial = row.get('editorial', '') or ''
    anio = str(row.get('anio_publicacion', '')) or ''

    key = f"{titulo}|{autor}|{editorial}|{anio}"
    hash_id = hashlib.md5(key.encode('utf-8')).hexdigest()

    return f"hash_{hash_id[:16]}"  # Prefijo para identificar que es hash


def agregar_book_id(df):
    """Agrega columna book_id a un DataFrame"""
    print("🔑 Generando book_id canónico...")

    df['book_id'] = df.apply(generar_book_id, axis=1)

    # Stats
    isbn_count = df['book_id'].str.match(r'^\d{13}$').sum()
    hash_count = df['book_id'].str.startswith('hash_').sum()

    print(f"   ✓ {isbn_count} libros con ISBN13 como book_id")
    print(f"   ✓ {hash_count} libros con hash como book_id")

    return df

# ============================================================
# 5. CONSOLIDAR FUENTES
# ============================================================

def consolidar_fuentes(df_gr, df_gb):
    """
    Merge de Goodreads y Google Books por book_id.
    Aplicar reglas de supervivencia.
    """
    print("🔗 Consolidando fuentes...")

    # Merge outer para no perder registros
    df_merged = pd.merge(
        df_gr,
        df_gb,
        on='book_id',
        how='outer',
        suffixes=('_gr', '_gb')
    )

    print(f"   ✓ {len(df_merged)} registros tras merge")

    return df_merged

# ============================================================
# 6. DEDUPLICACIÓN Y SUPERVIVENCIA
# ============================================================

def aplicar_supervivencia(group, ts_run):
    """
    Aplica reglas de supervivencia a un grupo de duplicados.
    Reglas:
    - Título: el más largo/completo
    - Precio: el más reciente (Google Books > Goodreads)
    - Autores/categorías: unión sin duplicados
    - Otros campos: preferir no-null, luego Google Books > Goodreads
    """

    # Inicializar registro consolidado
    consolidated = {}

    # book_id (único para el grupo)
    consolidated['book_id'] = group['book_id'].iloc[0]

    # TÍTULO: el más largo
    titulo_cols = [c for c in ['titulo_gr', 'titulo_gb'] if c in group.columns]
    titulos = group[titulo_cols].values.flatten() if titulo_cols else []
    titulos_validos = [t for t in titulos if pd.notna(t)]
    consolidated['titulo'] = max(titulos_validos, key=len) if titulos_validos else None

    # TÍTULO NORMALIZADO: corresponde al título elegido
    consolidated['titulo_normalizado'] = normalizar_titulo(consolidated['titulo'])

    # SUBTÍTULO: preferir GB
    consolidated['subtitulo'] = (
        group['subtitulo_gb'].iloc[0]
        if pd.notna(group['subtitulo_gb'].iloc[0])
        else None
    )

    # AUTOR: preferir GB (más completo), sino GR
    autor_gb = group['autor_gb'].iloc[0] if 'autor_gb' in group.columns else None
    autor_gr = group['autor_gr'].iloc[0] if 'autor_gr' in group.columns else None
    consolidated['autor_principal'] = autor_gb if pd.notna(autor_gb) else autor_gr
    consolidated['autor_normalizado'] = normalizar_autor(
        consolidated['autor_principal']
    )

    # AUTORES LISTA: de GB (lista ya separada por '|')
    autores_gb = group['autores_lista_gb'].iloc[0] if 'autores_lista_gb' in group.columns else ''
    autores_gb = autores_gb if pd.notna(autores_gb) else ''
    autores_lista = [a.strip() for a in str(autores_gb).split('|') if a.strip()]
    consolidated['autores'] = autores_lista if autores_lista else None

    # EDITORIAL: preferir GB
    consolidated['editorial'] = (
        group['editorial_gb'].iloc[0]
        if pd.notna(group['editorial_gb'].iloc[0])
        else group['editorial_gr'].iloc[0]
    )

    # AÑO: preferir no-null, luego GB > GR
    consolidated['anio_publicacion'] = (
        group['anio_publicacion_gb'].iloc[0]
        if pd.notna(group['anio_publicacion_gb'].iloc[0])
        else group['anio_publicacion_gr'].iloc[0]
    )

    # FECHA: preferir GB (más completa)
    consolidated['fecha_publicacion'] = (
        group['fecha_publicacion_gb'].iloc[0]
        if 'fecha_publicacion_gb' in group.columns
        and pd.notna(group['fecha_publicacion_gb'].iloc[0])
        else None
    )

    # IDIOMA: GB
    consolidated['idioma'] = (
        group['idioma_gb'].iloc[0]
        if pd.notna(group['idioma_gb'].iloc[0])
        else None
    )

    # ISBNs: preferir no-null
    consolidated['isbn10'] = (
        group['isbn10_gb'].iloc[0]
        if pd.notna(group['isbn10_gb'].iloc[0])
        else group['isbn10_gr'].iloc[0]
    )
    consolidated['isbn13'] = (
        group['isbn13_gb'].iloc[0]
        if pd.notna(group['isbn13_gb'].iloc[0])
        else group['isbn13_gr'].iloc[0]
    )

    # PÁGINAS: GB
    consolidated['paginas'] = (
        group['paginas_gb'].iloc[0]
        if pd.notna(group['paginas_gb'].iloc[0])
        else None
    )

    # CATEGORÍAS: GB
    categorias_gb = (
        group['categorias_gb'].iloc[0]
        if pd.notna(group['categorias_gb'].iloc[0])
        else ''
    )
    categorias_lista = [c.strip() for c in str(categorias_gb).split('|') if c.strip()]
    consolidated['categorias'] = categorias_lista if categorias_lista else None

    # PRECIO Y MONEDA: GB (más reciente)
    consolidated['precio'] = (
        group['precio_gb'].iloc[0]
        if pd.notna(group['precio_gb'].iloc[0])
        else None
    )
    consolidated['moneda'] = (
        group['moneda_gb'].iloc[0]
        if pd.notna(group['moneda_gb'].iloc[0])
        else None
    )

    # RATING: GR
    consolidated['rating'] = (
        group['rating_gr'].iloc[0]
        if pd.notna(group['rating_gr'].iloc[0])
        else None
    )
    consolidated['ratings_count'] = (
        group['ratings_count_gr'].iloc[0]
        if pd.notna(group['ratings_count_gr'].iloc[0])
        else None
    )

    # FUENTE GANADORA: la que aportó más campos
    count_gb = group[[c for c in group.columns if c.endswith('_gb')]].notna().sum(
        axis=1
    ).iloc[0]
    count_gr = group[[c for c in group.columns if c.endswith('_gr')]].notna().sum(
        axis=1
    ).iloc[0]
    consolidated['fuente_ganadora'] = (
        'googlebooks' if count_gb >= count_gr else 'goodreads'
    )

    # TIMESTAMP: timestamp de la ejecución del pipeline
    consolidated['ts_ultima_actualizacion'] = ts_run

    return pd.Series(consolidated)


def deduplicar(df_merged, ts_run):
    """Deduplica por book_id aplicando reglas de supervivencia (versión robusta)"""
    print("🔀 Deduplicando y aplicando supervivencia...")

    filas_consolidadas = []

    # Recorremos cada grupo de book_id
    for book_id, group in df_merged.groupby('book_id', dropna=False):
        fila = aplicar_supervivencia(group, ts_run)  # ← devuelve un Series
        filas_consolidadas.append(fila)

    # Construimos un DataFrame limpio a partir de esa lista de Series
    df_dedup = pd.DataFrame(filas_consolidadas)

    duplicados_resueltos = len(df_merged) - len(df_dedup)
    print(f"   ✓ {duplicados_resueltos} duplicados resueltos")
    print(f"   ✓ {len(df_dedup)} libros únicos en dim_book")

    return df_dedup

# ============================================================
# 7. BOOK_SOURCE_DETAIL (trazabilidad)
# ============================================================

def crear_source_detail(df_gr, df_gb):
    """Crea tabla de trazabilidad con registros originales"""
    print("📋 Creando book_source_detail...")

    # Goodreads records
    gr_detail = pd.DataFrame({
        'source_id': df_gr.apply(
            lambda r: f"goodreads_{r['source_row_number']}", axis=1
        ),
        'source_name': 'goodreads',
        'source_file': 'goodreads_books.json',
        'row_number': df_gr['source_row_number'],
        'book_id_candidato': df_gr['book_id'],
        'titulo_original': df_gr['titulo'],
        'autor_original': df_gr['autor'],
        'isbn10_original': df_gr['isbn10'],
        'isbn13_original': df_gr['isbn13'],
        'rating_original': df_gr['rating'],
        'anio_publicacion_original': df_gr['anio_publicacion'],
        'timestamp_ingesta': datetime.now().isoformat()
    })

    # Google Books records
    gb_detail = pd.DataFrame({
        'source_id': df_gb.apply(
            lambda r: f"googlebooks_{r['source_row_number']}", axis=1
        ),
        'source_name': 'googlebooks',
        'source_file': 'googlebooks_books.csv',
        'row_number': df_gb['source_row_number'],
        'book_id_candidato': df_gb['book_id'],
        'titulo_original': df_gb['titulo'],
        'autor_original': df_gb['autor'],
        'isbn10_original': df_gb['isbn10'],
        'isbn13_original': df_gb['isbn13'],
        'editorial_original': df_gb['editorial'],
        'precio_original': df_gb['precio'],
        'moneda_original': df_gb['moneda'],
        'idioma_original': df_gb['idioma'],
        'anio_publicacion_original': df_gb['anio_publicacion'],
        'timestamp_ingesta': datetime.now().isoformat()
    })

    # Combinar
    detail = pd.concat([gr_detail, gb_detail], ignore_index=True)

    print(f"   ✓ {len(detail)} registros de trazabilidad")

    return detail

# ============================================================
# 8. QUALITY METRICS
# ============================================================

def generar_quality_metrics(dim_book, book_source_detail):
    """Genera métricas de calidad usando utils_quality"""
    print("📊 Generando quality_metrics.json...")

    metrics = {
        'timestamp': datetime.now().isoformat(),
        'dim_book': {},
        'book_source_detail': {}
    }

    # Métricas de dim_book
    metrics['dim_book']['row_count'] = int(len(dim_book))
    metrics['dim_book']['column_count'] = int(len(dim_book.columns))

    # Completitud de campos clave
    metrics['dim_book']['completeness'] = {
        'titulo': float(calculate_completeness(dim_book, 'titulo')),
        'isbn13': float(calculate_completeness(dim_book, 'isbn13')),
        'precio': float(calculate_completeness(dim_book, 'precio')),
        'autor_principal': float(calculate_completeness(dim_book, 'autor_principal')),
        'anio_publicacion': float(calculate_completeness(dim_book, 'anio_publicacion')),
    }

    # Validaciones
    metrics['dim_book']['validation'] = {
        'fechas_iso': validate_date_column(dim_book, 'fecha_publicacion'),
        'idiomas_bcp47': validate_language_column(dim_book, 'idioma'),
        'monedas_iso4217': validate_currency_column(dim_book, 'moneda'),
        'isbn13_validos': {
            'total_non_null': int(dim_book['isbn13'].notna().sum()),
            'valid_count': int(
                dim_book['isbn13'].apply(
                    lambda x: validate_isbn13(x) if pd.notna(x) else False
                ).sum()
            ),
            'valid_percentage': float(
                (
                    dim_book['isbn13'].apply(
                        lambda x: validate_isbn13(x) if pd.notna(x) else False
                    ).sum()
                    / dim_book['isbn13'].notna().sum()
                    * 100
                )
                if dim_book['isbn13'].notna().sum() > 0
                else 0
            )
        }
    }

    # Duplicados (debería ser 0 tras deduplicación)
    metrics['dim_book']['duplicates'] = {
        'book_id_duplicates': int(dim_book['book_id'].duplicated().sum())
    }

    # Por fuente
    fuente_counts = dim_book['fuente_ganadora'].value_counts().to_dict()
    metrics['dim_book']['by_source'] = {k: int(v) for k, v in fuente_counts.items()}

    # Métricas de book_source_detail
    metrics['book_source_detail']['row_count'] = int(len(book_source_detail))
    source_counts = book_source_detail['source_name'].value_counts().to_dict()
    metrics['book_source_detail']['by_source'] = {
        k: int(v) for k, v in source_counts.items()
    }

    return metrics

# ============================================================
# 9. ASERCIONES BLOQUEANTES
# ============================================================

def assert_calidad(dim_book):
    """Aserciones que deben cumplirse o el pipeline falla"""
    print("✅ Ejecutando aserciones bloqueantes...")

    # 1. Títulos no nulos >= 90%
    completitud_titulo = calculate_completeness(dim_book, 'titulo')
    assert completitud_titulo >= 90, (
        f"❌ ERROR: Solo {completitud_titulo:.1f}% de títulos válidos (mínimo 90%)"
    )
    print(f"   ✓ Títulos válidos: {completitud_titulo:.1f}%")

    # 2. book_id únicos
    assert dim_book['book_id'].is_unique, "❌ ERROR: book_id duplicados detectados"
    print(f"   ✓ book_id únicos: {len(dim_book)} registros")

    # 3. Precios en rango válido (si existen)
    precios_validos = dim_book['precio'].notna()
    if precios_validos.sum() > 0:
        precio_min = dim_book.loc[precios_validos, 'precio'].min()
        precio_max = dim_book.loc[precios_validos, 'precio'].max()
        assert 0 <= precio_min and precio_max <= 1000, (
            f"❌ ERROR: Precios fuera de rango [0, 1000]: "
            f"min={precio_min}, max={precio_max}"
        )
        print(f"   ✓ Precios en rango: [{precio_min:.2f}, {precio_max:.2f}]")

    # 4. Al menos 10 libros
    assert len(dim_book) >= 10, (
        f"❌ ERROR: Solo {len(dim_book)} libros (mínimo 10)"
    )
    print(f"   ✓ Total libros: {len(dim_book)}")

    print("   ✅ Todas las aserciones pasadas")

# ============================================================
# 10. EMITIR OUTPUTS
# ============================================================

def guardar_parquet(df, filepath):
    """Guarda DataFrame como Parquet (ruta relativa a la raíz del proyecto)"""
    full_path = BASE_DIR / filepath
    df.to_parquet(full_path, engine='pyarrow', index=False)
    size_kb = os.path.getsize(full_path) / 1024
    print(f"   ✓ {full_path} ({size_kb:.2f} KB)")


def guardar_json(data, filepath):
    """Guarda dict como JSON (ruta relativa a la raíz del proyecto)"""
    full_path = BASE_DIR / filepath
    with open(full_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    size_kb = os.path.getsize(full_path) / 1024
    print(f"   ✓ {full_path} ({size_kb:.2f} KB)")


def generar_schema_md(dim_book, filepath):
    """Genera documentación del esquema"""
    print(f"📝 Generando {filepath}...")

    schema_content = f"""# Schema - dim_book.parquet

Documentación del modelo de datos generado el {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Tabla: dim_book.parquet

Tabla canónica: **1 fila por libro**, deduplicada y normalizada.

### Campos ({len(dim_book.columns)} columnas)

| Campo | Tipo | Nulos | Ejemplo | Descripción |
|-------|------|-------|---------|-------------|
"""

    for col in dim_book.columns:
        dtype = str(dim_book[col].dtype)
        null_count = int(dim_book[col].isna().sum())
        null_pct = (null_count / len(dim_book)) * 100
        ejemplo = (
            dim_book[col].dropna().iloc[0]
            if dim_book[col].notna().any()
            else 'N/A'
        )

        # Truncar ejemplo si es muy largo
        if isinstance(ejemplo, str) and len(ejemplo) > 50:
            ejemplo = ejemplo[:47] + '...'
        elif isinstance(ejemplo, list):
            ejemplo = (
                str(ejemplo[:2]) + '...'
                if len(ejemplo) > 2
                else str(ejemplo)
            )

        schema_content += (
            f"| `{col}` | {dtype} | {null_pct:.1f}% | {ejemplo} | |\n"
        )

    schema_content += f"""
## Estadísticas

- Total de libros: {len(dim_book)}
- Libros con ISBN13: {dim_book['isbn13'].notna().sum()} ({dim_book['isbn13'].notna().sum()/len(dim_book)*100:.1f}%)
- Libros con precio: {dim_book['precio'].notna().sum()} ({dim_book['precio'].notna().sum()/len(dim_book)*100:.1f}%)
- Idiomas únicos: {dim_book['idioma'].nunique()}
- Fuentes: {', '.join(dim_book['fuente_ganadora'].unique())}

## Reglas de normalización

- **Fechas**: ISO-8601 (YYYY-MM-DD). Si la fuente solo proporciona año (YYYY) o año-mes (YYYY-MM), se completa al primer día del año o del mes para mantener un formato consistente. La precisión real de la fecha original se considera aproximada.
- **Idioma**: BCP-47 (ej: es, en, pt-br)
- **Moneda**: ISO-4217 (ej: EUR, USD)
- **ISBN**: Validados con checksum
- **book_id**: ISBN13 si existe, sino hash MD5 de campos clave

## Reglas de deduplicación

- **Clave principal**: ISBN13
- **Clave alternativa**: hash(titulo_normalizado + autor_normalizado + editorial + anio)
- **Título**: se elige el más completo
- **Precio**: se elige el más reciente (Google Books > Goodreads)
- **Autores/categorías**: unión sin duplicados
- **Fuente ganadora**: la que aporta más campos completos
"""

    full_path = BASE_DIR / filepath
    with open(full_path, 'w', encoding='utf-8') as f:
        f.write(schema_content)

    print(f"   ✓ {full_path}")

# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    print("=" * 60)
    print("  PIPELINE DE INTEGRACIÓN - Proyecto RA1")
    print("=" * 60)
    print()

    # Timestamp único para toda la ejecución
    run_ts = datetime.now().isoformat()

    # Crear carpetas si no existen
    os.makedirs(STANDARD_DIR, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)

    try:
        # 1. LECTURA
        df_gr_raw = leer_goodreads()
        df_gb_raw = leer_googlebooks()
        print()

        # 2. STAGING
        df_gr_staging = mapear_goodreads(df_gr_raw)
        df_gb_staging = mapear_googlebooks(df_gb_raw)
        print()

        # 3. NORMALIZACIÓN
        df_gr_norm = normalizar_df(df_gr_staging)
        df_gb_norm = normalizar_df(df_gb_staging)
        print()

        # 4. GENERAR book_id
        df_gr_norm = agregar_book_id(df_gr_norm)
        df_gb_norm = agregar_book_id(df_gb_norm)
        print()

        # 5. CONSOLIDAR
        df_merged = consolidar_fuentes(df_gr_norm, df_gb_norm)
        print()

        # 6. DEDUPLICAR
        dim_book = deduplicar(df_merged, run_ts)
        print()

        # 7. SOURCE DETAIL
        book_source_detail = crear_source_detail(df_gr_norm, df_gb_norm)
        print()

        # 8. QUALITY METRICS
        quality_metrics = generar_quality_metrics(dim_book, book_source_detail)
        print()

        # 9. ASERCIONES
        assert_calidad(dim_book)
        print()

        # 10. EMITIR OUTPUTS
        print("💾 Guardando outputs...")
        guardar_parquet(dim_book, 'standard/dim_book.parquet')
        guardar_parquet(
            book_source_detail, 'standard/book_source_detail.parquet'
        )
        guardar_json(quality_metrics, 'docs/quality_metrics.json')
        generar_schema_md(dim_book, 'docs/schema.md')

        print()
        print("=" * 60)
        print("  ✅ PIPELINE COMPLETADO CON ÉXITO")
        print("=" * 60)
        print("📊 Resultados:")
        print(f"   - Libros únicos: {len(dim_book)}")
        print(f"   - Registros de trazabilidad: {len(book_source_detail)}")
        print(
            f"   - Completitud título: "
            f"{quality_metrics['dim_book']['completeness']['titulo']:.1f}%"
        )
        print(
            f"   - ISBNs válidos: "
            f"{quality_metrics['dim_book']['validation']['isbn13_validos']['valid_percentage']:.1f}%"
        )
        print("=" * 60)

    except Exception as e:
        print()
        print("=" * 60)
        print("  ❌ ERROR EN EL PIPELINE")
        print("=" * 60)
        print(f"  {str(e)}")
        print("=" * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
