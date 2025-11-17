# Mini-Pipeline de Libros — Proyecto UT1 RA1  
### Tema elegido: Libros de Barbacoa / Parrilla / BBQ  
**Asignatura: SBD — Sistemas de Big Data**  
**Curso de Especialización en IA & Big Data**

---

# 1. Descripción general del proyecto

Este proyecto desarrolla un **mini‑pipeline de integración de datos** dividido en tres bloques:

1. **Scraping desde Goodreads → JSON (landing/)**  
2. **Enriquecimiento con Google Books API → CSV (landing/)**  
3. **Integración, normalización y deduplicación → Parquet (standard/)**

El objetivo es producir un **modelo canónico de libros** totalmente normalizado, deduplicado, con trazabilidad por fuente y métricas de calidad.

---

# 2. Estructura del repositorio

```
books-pipeline/
├─ README.md
├─ requirements.txt
├─ .env.example
├─ landing/
│  ├─ goodreads_books.json
│  └─ googlebooks_books.csv
├─ standard/
│  ├─ dim_book.parquet
│  └─ book_source_detail.parquet
├─ docs/
│  ├─ schema.md
│  └─ quality_metrics.json
└─ src/
   ├─ scrape_goodreads.py
   ├─ enrich_googlebooks.py
   ├─ integrate_pipeline.py
   ├─ utils_quality.py
   └─ utils_isbn.py
```

Esta estructura cumple exactamente la solicitada en el enunciado.

---

# 3. Bloque 1 — Scraping (Goodreads → JSON)

### Fuente utilizada
Se utilizó la búsqueda pública **“barbacoa”** en Goodreads.  
La primera página completa incluía libros de cocina, novelas, traducciones, cuadernos vacíos y revistas.  
Esta mezcla es deliberadamente útil para evaluar:

- deduplicación  
- calidad de datos  
- supervivencia entre fuentes

### Selectores / técnica
- Librería: `requests` + `beautifulsoup4` + parser `lxml`
- Selectores CSS documentados en `scrape_goodreads.py`
- User-Agent configurable vía `.env`
- Pausa de 0.5–1.0 s entre peticiones

### Campos extraídos
- `title`  
- `author`  
- `rating`  
- `ratings_count`  
- `book_url`  
- `isbn10` / `isbn13` (si aparecen en la ficha del libro)  
- `published_year` (si aparece en texto tipo “published YYYY”)

### Metadatos registrados
- Fecha/hora de extracción  
- URL de búsqueda  
- Nº de libros capturados (20)  
- User-Agent empleado  

### Salida generada
`landing/goodreads_books.json` (sin modificar, según enunciado)

---

# 4. Bloque 2 — Enriquecimiento (Google Books API → CSV)

Cada libro del JSON se consulta en Google Books siguiendo esta prioridad:

1. **ISBN13**  
2. **ISBN10**  
3. **título + autor** (fallback)

### Campos extraídos
- `gb_id`
- `title`, `subtitle`
- `authors` (lista)
- `publisher`
- `published_date`
- `language`
- `categories` (lista)
- `isbn10`, `isbn13`
- `price_amount`
- `price_currency`

### Decisiones de diseño
- Para listas (autores, categorías): se guardan como `|` para CSV.  
- Si hay múltiples ediciones, se selecciona la **más completa**.  
- Si no existe API Key, se usa el endpoint público (limitado).  
- En caso de error de red/timeout, el registro se marca como incompleto.

### Salida generada
`landing/googlebooks_books.csv`

---

# 5. Bloque 3 — Integración (JSON + CSV → Parquet)

Este bloque implementa:

- modelo canónico  
- normalización semántica  
- deduplicación  
- supervivencia  
- trazabilidad  
- métricas de calidad  
- aserciones bloqueantes

### 5.1 Normalización semántica

- **Fechas:** ISO‑8601 (`YYYY-MM-DD`).  
  - Si la fuente solo incluye año (`YYYY`) o año-mes (`YYYY-MM`),  
    se completa con día `01`.  
- **Idioma:** BCP‑47 (`es`, `en`, `pt-BR`, …).  
- **Moneda:** ISO‑4217 (`EUR`, `USD`, …).  
- **ISBN:** validación y checksum según estándar ISBN‑10/ISBN‑13.  
- **Autores / categorías:** listas únicas sin duplicados.  
- **Columnas:** formato `snake_case` consistente.

### 5.2 Modelo canónico

`book_id` se asigna así:
1. ISBN13 válido → **book_id = isbn13**  
2. ISBN10 válido → convertido a ISBN13  
3. Ningún ISBN válido → hash MD5 de  
   `titulo_normalizado + autor_normalizado + editorial + anio_publicacion`

### 5.3 Reglas de deduplicación y supervivencia

- **Clave principal:** ISBN13  
- **Clave secundaria:** hash de campos clave (si no hay ISBN)
- **Supervivencia:**
  - Título más completo  
  - Precio más reciente (prioridad Google Books > Goodreads)  
  - Unión de listas (autores/categorías)  
  - Fuente ganadora: la que aporta más información  

### 5.4 Salidas generadas

- `standard/dim_book.parquet`  
  → 1 fila por libro (31 libros únicos)  

- `standard/book_source_detail.parquet`  
  → Trazabilidad completa (40 registros)  

- `docs/quality_metrics.json`  
  → Nulos, completitud, rangos, duplicados, % fechas válidas, % idiomas válidos, etc.

- `docs/schema.md`  
  → Documentación estructurada del modelo final.

---

# 6. Cómo ejecutar el proyecto

## 6.1 Crear entorno (Conda recomendado)
```
conda create -n sbd python=3.11
conda activate sbd
conda install pyarrow pandas numpy requests beautifulsoup4 lxml
```

*(Evitar `pip install` para pyarrow, pandas y numpy.)*

## 6.2 Configurar `.env`
Copiar `.env.example` a `.env`  
y completar:

- `USER_AGENT`
- `GOODREADS_SEARCH_QUERY`
- `GOOGLE_BOOKS_API_KEY` (opcional)
- `GOODREADS_MAX_BOOKS`

## 6.3 Ejecutar pipeline
```
python src/scrape_goodreads.py
python src/enrich_googlebooks.py
python src/integrate_pipeline.py
```

---

# 7. Resultados finales del pipeline

- **Libros únicos:** 31  
- **Registros de trazabilidad:** 40  
- **Completitud de títulos:** 96.8 %  
- **Precios válidos:** 100 %  
- **ISBN13 válidos:** 0 % (esperado por el dataset de origen)  
- **Pipeline completado sin errores**

---

# 8. Limitaciones conocidas

- Goodreads entrega resultados muy heterogéneos para “barbacoa”  
  (ficción, cuadernos, traducciones difíciles).  
- Muchas entradas carecen de ISBN oficial.  
- Google Books no siempre devuelve precios actuales.  
- Puede haber ediciones antiguas o inconsistencias entre fuentes.

---

# 9. Mejoras futuras

- Similaridad avanzada (fuzzy‑matching) de títulos/autores.  
- Detener el scraping si cambia el HTML de Goodreads.  
- Retry/backoff inteligente en Google Books.  
- Gráfica interactiva de duplicados y supervivencia.

---

# 10. Autoría

Proyecto realizado por **Borja Ramos Oliva**  
para **SBD — Sistemas de Big Data**  
Curso de Especialización en Inteligencia Artificial y Big Data  
(Centro: Carlos III)
