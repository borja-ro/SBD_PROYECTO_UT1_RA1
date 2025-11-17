# enrich_googlebooks.py
from dotenv import load_dotenv
import requests
import json
import time
import random
import csv
import os
from datetime import datetime

load_dotenv()
GOOGLE_BOOKS_API_KEY = os.getenv("GOOGLE_BOOKS_API_KEY")
if not GOOGLE_BOOKS_API_KEY:
    print("⚠ AVISO: No se encontró GOOGLE_BOOKS_API_KEY en .env. Se utilizará la API sin clave (límites más estrictos de peticiones).")
BASE_URL = "https://www.googleapis.com/books/v1/volumes"

def search_by_isbn(isbn, api_key=None):
    """Busca un libro por ISBN en Google Books"""
    params = {'q': f'isbn:{isbn}'}
    if api_key:
        params['key'] = api_key
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('totalItems', 0) > 0:
                return data['items'][0]  # Retorna el primer resultado
        return None
    except Exception as e:
        print(f"      ⚠ Error buscando ISBN: {e}")
        return None

def search_by_title_author(title, author, api_key=None):
    """Busca un libro por título y autor en Google Books"""
    # Limpiar el título y autor para la búsqueda
    title_clean = title.split(':')[0].split('(')[0].strip()  # Quitar subtítulos y series
    author_clean = author.split(',')[0].strip() if author else ""  # Solo primer autor
    
    # Construir query
    query_parts = []
    if title_clean:
        query_parts.append(f'intitle:{title_clean}')
    if author_clean:
        query_parts.append(f'inauthor:{author_clean}')
    
    query = '+'.join(query_parts)
    
    params = {'q': query, 'maxResults': 5}  # Máximo 5 resultados para elegir el mejor
    if api_key:
        params['key'] = api_key
    
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get('totalItems', 0) > 0:
                # Retornar el resultado más completo (con más campos)
                return select_best_match(data['items'], title_clean, author_clean)
        return None
    except Exception as e:
        print(f"      ⚠ Error buscando título+autor: {e}")
        return None

def select_best_match(items, title, author):
    """Selecciona el mejor match de los resultados de Google Books"""
    # Criterio: preferir resultados con más información completa
    best = None
    best_score = 0
    
    for item in items:
        volume_info = item.get('volumeInfo', {})
        sale_info = item.get('saleInfo', {})
        
        # Calcular score basado en completitud de campos
        score = 0
        if volume_info.get('title'):
            score += 1
        if volume_info.get('authors'):
            score += 1
        if volume_info.get('publisher'):
            score += 1
        if volume_info.get('publishedDate'):
            score += 1
        if volume_info.get('industryIdentifiers'):
            score += 2  # Los ISBN son importantes
        if volume_info.get('categories'):
            score += 1
        if sale_info.get('listPrice'):
            score += 1
        
        if score > best_score:
            best_score = score
            best = item
    
    return best

def extract_book_data(book_item, row_number):
    """Extrae los campos relevantes de un resultado de Google Books"""
    if not book_item:
        return None
    
    volume_info = book_item.get('volumeInfo', {})
    sale_info = book_item.get('saleInfo', {})
    
    # Extraer ISBNs
    isbn10, isbn13 = None, None
    for identifier in volume_info.get('industryIdentifiers', []):
        if identifier['type'] == 'ISBN_10':
            isbn10 = identifier['identifier']
        elif identifier['type'] == 'ISBN_13':
            isbn13 = identifier['identifier']
    
    # Extraer precio
    price_amount = None
    price_currency = None
    if sale_info.get('listPrice'):
        price_amount = sale_info['listPrice'].get('amount')
        price_currency = sale_info['listPrice'].get('currencyCode')
    
    # Extraer autores (lista separada por pipe |)
    authors = volume_info.get('authors', [])
    authors_str = '|'.join(authors) if authors else None
    
    # Extraer categorías (lista separada por pipe |)
    categories = volume_info.get('categories', [])
    categories_str = '|'.join(categories) if categories else None
    
    return {
        'row_number': row_number,
        'gb_id': book_item.get('id'),
        'title': volume_info.get('title'),
        'subtitle': volume_info.get('subtitle'),
        'authors': authors_str,
        'publisher': volume_info.get('publisher'),
        'published_date': volume_info.get('publishedDate'),
        'language': volume_info.get('language'),
        'page_count': volume_info.get('pageCount'),
        'categories': categories_str,
        'isbn10': isbn10,
        'isbn13': isbn13,
        'price_amount': price_amount,
        'price_currency': price_currency,
        'thumbnail': volume_info.get('imageLinks', {}).get('thumbnail')
    }

def enrich_from_goodreads():
    """Lee el JSON de Goodreads y enriquece con Google Books"""
    print("=" * 60)
    print("  ENRIQUECIMIENTO GOOGLE BOOKS - Proyecto RA1")
    print("=" * 60)
    
    # Leer JSON de Goodreads
    goodreads_file = 'landing/goodreads_books.json'
    if not os.path.exists(goodreads_file):
        raise FileNotFoundError(f"❌ No se encuentra {goodreads_file}. Ejecuta scrape_goodreads.py primero.")
    
    with open(goodreads_file, 'r', encoding='utf-8') as f:
        goodreads_data = json.load(f)
    
    books = goodreads_data['books']
    print(f"📚 Procesando {len(books)} libros de Goodreads\n")
    
    enriched_books = []
    stats = {
        'total': len(books),
        'found': 0,
        'not_found': 0,
        'with_isbn': 0,
        'with_price': 0
    }
    
    for idx, book in enumerate(books, 1):
        print(f"[{idx}/{len(books)}] {book['title'][:50]}...")
        
        # Estrategia de búsqueda:
        # 1. Intentar por ISBN13 (si existe)
        # 2. Intentar por ISBN10 (si existe)
        # 3. Fallback: título + autor
        
        result = None
        search_method = None
        
        if book.get('isbn13'):
            print(f"  → Buscando por ISBN13: {book['isbn13']}")
            result = search_by_isbn(book['isbn13'], GOOGLE_BOOKS_API_KEY)
            search_method = 'isbn13'
        
        if not result and book.get('isbn10'):
            print(f"  → Buscando por ISBN10: {book['isbn10']}")
            result = search_by_isbn(book['isbn10'], GOOGLE_BOOKS_API_KEY)
            search_method = 'isbn10'
        
        if not result:
            print(f"  → Buscando por título + autor")
            result = search_by_title_author(book['title'], book.get('author'), GOOGLE_BOOKS_API_KEY)
            search_method = 'title_author'
        
        if result:
            enriched_data = extract_book_data(result, book['row_number'])
            if enriched_data:
                enriched_data['search_method'] = search_method
                enriched_data['goodreads_title'] = book['title']
                enriched_data['goodreads_author'] = book.get('author')
                enriched_books.append(enriched_data)
                
                stats['found'] += 1
                if enriched_data['isbn13']:
                    stats['with_isbn'] += 1
                if enriched_data['price_amount']:
                    stats['with_price'] += 1
                
                print(f"  ✓ Encontrado: {enriched_data['title'][:40]}... (ISBN13: {enriched_data['isbn13'] or 'N/A'})")
            else:
                stats['not_found'] += 1
                print(f"  ✗ No se pudo extraer datos")
        else:
            stats['not_found'] += 1
            print(f"  ✗ No encontrado en Google Books")
            # Guardar registro vacío para mantener trazabilidad
            enriched_books.append({
                'row_number': book['row_number'],
                'gb_id': None,
                'title': None,
                'subtitle': None,
                'authors': None,
                'publisher': None,
                'published_date': None,
                'language': None,
                'page_count': None,
                'categories': None,
                'isbn10': None,
                'isbn13': None,
                'price_amount': None,
                'price_currency': None,
                'thumbnail': None,
                'search_method': 'not_found',
                'goodreads_title': book['title'],
                'goodreads_author': book.get('author')
            })
        
        # Pausa para no saturar la API (más corta si tienes API key)
        if GOOGLE_BOOKS_API_KEY:
            time.sleep(random.uniform(0.2, 0.5))
        else:
            time.sleep(random.uniform(1.0, 1.5))
    
    return enriched_books, stats

def save_to_csv(enriched_books):
    """Guarda los datos enriquecidos en CSV"""
    output_file = 'landing/googlebooks_books.csv'
    
    fieldnames = [
        'row_number', 'gb_id', 'title', 'subtitle', 'authors', 'publisher',
        'published_date', 'language', 'page_count', 'categories', 
        'isbn10', 'isbn13', 'price_amount', 'price_currency', 'thumbnail',
        'search_method', 'goodreads_title', 'goodreads_author'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(enriched_books)
    
    return output_file

def main():
    # Enriquecer
    enriched_books, stats = enrich_from_goodreads()
    
    # Guardar CSV
    output_file = save_to_csv(enriched_books)
    
    # Estadísticas finales
    print("\n" + "=" * 60)
    print("📊 RESUMEN DEL ENRIQUECIMIENTO")
    print("=" * 60)
    print(f"Total de libros procesados: {stats['total']}")
    print(f"  ✓ Encontrados en Google Books: {stats['found']} ({stats['found']/stats['total']*100:.1f}%)")
    print(f"  ✗ No encontrados: {stats['not_found']} ({stats['not_found']/stats['total']*100:.1f}%)")
    print(f"  📖 Con ISBN13: {stats['with_isbn']} ({stats['with_isbn']/stats['total']*100:.1f}%)")
    print(f"  💰 Con precio: {stats['with_price']} ({stats['with_price']/stats['total']*100:.1f}%)")
    print(f"\n✅ Guardado en: {output_file}")
    print(f"📦 Tamaño: {os.path.getsize(output_file) / 1024:.2f} KB")
    print("=" * 60)
    
    # Documentar decisiones
    print("\n📝 DECISIONES Y SUPUESTOS:")
    print("  - Búsqueda prioritaria: ISBN13 > ISBN10 > Título+Autor")
    print("  - Se selecciona el resultado más completo (más campos)")
    print("  - Autores y categorías: separados por pipe '|'")
    print("  - Codificación: UTF-8")
    print("  - Separador CSV: coma ','")
    print("  - Rate limiting: 0.2-0.5s con API key, 1.0-1.5s sin API key")
    print("=" * 60)

if __name__ == '__main__':
    main()