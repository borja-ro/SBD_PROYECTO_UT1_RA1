# Schema - dim_book.parquet

Documentación del modelo de datos generado el 2025-11-17 23:25:55

## Tabla: dim_book.parquet

Tabla canónica: **1 fila por libro**, deduplicada y normalizada.

### Campos (21 columnas)

| Campo | Tipo | Nulos | Ejemplo | Descripción |
|-------|------|-------|---------|-------------|
| `book_id` | object | 0.0% | 9780760392744 | |
| `titulo` | object | 3.2% | Barbacoa | |
| `titulo_normalizado` | object | 3.2% | barbacoa | |
| `subtitulo` | object | 87.1% | The Heart of Tex-Mex Barbecue | |
| `autor_principal` | object | 3.2% | Brandon Hurtado | |
| `autor_normalizado` | object | 3.2% | brandon hurtado | |
| `autores` | object | 64.5% | ['Brandon Hurtado'] | |
| `editorial` | object | 71.0% | Harvard Common Press | |
| `anio_publicacion` | float64 | 38.7% | 2025.0 | |
| `fecha_publicacion` | object | 100.0% | N/A | |
| `idioma` | object | 64.5% | en | |
| `isbn10` | object | 67.7% | 0760392749 | |
| `isbn13` | object | 67.7% | 97807603927440 | |
| `paginas` | float64 | 64.5% | 242.0 | |
| `categorias` | object | 71.0% | ['Cooking'] | |
| `precio` | float64 | 71.0% | 35.99 | |
| `moneda` | object | 71.0% | EUR | |
| `rating` | float64 | 38.7% | 3.7 | |
| `ratings_count` | float64 | 58.1% | 591.0 | |
| `fuente_ganadora` | object | 0.0% | googlebooks | |
| `ts_ultima_actualizacion` | object | 0.0% | 2025-11-17T23:25:55.203309 | |

## Estadísticas

- Total de libros: 31
- Libros con ISBN13: 10 (32.3%)
- Libros con precio: 9 (29.0%)
- Idiomas únicos: 2
- Fuentes: googlebooks, goodreads

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
