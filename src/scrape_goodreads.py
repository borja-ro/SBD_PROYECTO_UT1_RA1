# scrape_goodreads.py
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import re
import json
import time
import random
from datetime import datetime
import os

# Obtenemos el User Agent de https://www.whatismybrowser.com/detect/what-is-my-user-agent/ y lo metemos en el .env
load_dotenv()
USER_AGENT = os.getenv("USER_AGENT")
if not USER_AGENT:
    raise ValueError("❌ USER_AGENT no encontrado en .env - Copia .env.example y configúralo")
BASE_URL = "https://www.goodreads.com"
GOODREADS_QUERY = os.getenv("GOODREADS_SEARCH_QUERY", "barbacoa")
GOODREADS_MAX_BOOKS = int(os.getenv("GOODREADS_MAX_BOOKS", "20"))
BASE_SEARCH_URL = f"{BASE_URL}/search"

def scrape_search_page(page=1, start_idx=1, max_books=None):
    """Scrapea una página de resultados de búsqueda de Goodreads.

    :param page: número de página a scrapear (1, 2, 3, ...)
    :param start_idx: índice inicial para row_number
    :param max_books: máximo de libros a procesar en esta página (None = todos)
    """
    search_url = f"{BASE_SEARCH_URL}?q={GOODREADS_QUERY}&page={page}"
    print(f"🔍 Scrapeando: {search_url}")
    
    headers = {'User-Agent': USER_AGENT}
    response = requests.get(search_url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Error al acceder a Goodreads: {response.status_code}")
    
    soup = BeautifulSoup(response.content, 'lxml')
    
    # Encuentra todos los libros (usando el selector que funciona)
    libros = soup.find_all('tr', attrs={'itemtype': re.compile(r'.*Book')})
    
    print(f"📚 Encontrados {len(libros)} libros en la página")
    
    # Si max_books está definido, limitamos el número de libros a procesar en esta página
    if max_books is not None:
        libros = libros[:max_books]
    
    books_data = []

### En este bloque puedes limitar el número de libros a procesar (entendía que eran 15 en el enunciado, pero era mínimo 15) 
    # for idx, libro in enumerate(libros[:15], 1):  # Limitar a 15 libros
    #     try:
    #         print(f"  [{idx}/15] Procesando libro...")
    for offset, libro in enumerate(libros, 0):  # Procesamos los libros encontrados en esta página
        idx = start_idx + offset
        try:
            print(f"  [{idx}] Procesando libro...")

            # TÍTULO
            titulo_elem = libro.find('a', class_='bookTitle')
            titulo = titulo_elem.get_text(strip=True) if titulo_elem else None
            book_url = BASE_URL + titulo_elem['href'] if titulo_elem else None
            
            # AUTOR
            autor_elem = libro.find('span', attrs={'itemprop': 'author'})
            if autor_elem:
                autor_text = autor_elem.get_text(strip=True)
                # Limpiar "(Goodreads Author)" del nombre
                autor = re.sub(r'\s*\(Goodreads Author\)\s*', '', autor_text).strip()
            else:
                autor = None
            
            # RATING (ejemplo: "3.70 avg rating — 591 ratings")
            rating_elem = libro.find('span', class_='minirating')
            rating = None
            ratings_count = None
            
            if rating_elem:
                rating_text = rating_elem.get_text(strip=True)
                
                # Extraer rating numérico
                rating_match = re.search(r'(\d+\.\d+)\s+avg rating', rating_text)
                if rating_match:
                    rating = float(rating_match.group(1))
                
                # Extraer número de ratings
                ratings_count_match = re.search(r'([\d,]+)\s+ratings', rating_text)
                if ratings_count_match:
                    # Quitar comas de números como "1,234"
                    ratings_count = int(ratings_count_match.group(1).replace(',', ''))
            
            # AÑO DE PUBLICACIÓN (si aparece en la página de búsqueda)
            published_year = None
            year_elem = libro.find(string=re.compile(r'published\s+(\d{4})', re.IGNORECASE))
            if year_elem:
                year_match = re.search(r'(\d{4})', year_elem)
                if year_match:
                    try:
                        published_year = int(year_match.group(1))
                    except ValueError:
                        published_year = None
            
            # Ir a la página individual del libro para obtener ISBN
            isbn10, isbn13 = None, None
            if book_url:
                print(f"    → Obteniendo ISBN de {titulo[:40]}...")
                isbn10, isbn13 = scrape_book_page(book_url)
                time.sleep(random.uniform(0.5, 1.0))  # Pausa entre peticiones
            
            books_data.append({
                'row_number': idx,
                'title': titulo,
                'author': autor,
                'rating': rating,
                'ratings_count': ratings_count,
                'published_year': published_year,
                'book_url': book_url,
                'isbn10': isbn10,
                'isbn13': isbn13
            })
            
            print(f"    ✓ {titulo[:50]}... (Rating: {rating}, ISBN13: {isbn13 or 'N/A'})")
            
        except Exception as e:
            print(f"    ✗ Error procesando libro: {e}")
            continue
    
    return books_data, start_idx + len(books_data)

def scrape_book_page(book_url):
    """Scrapea la página individual del libro para obtener ISBN"""
    headers = {'User-Agent': USER_AGENT}
    
    try:
        response = requests.get(book_url, headers=headers)
        if response.status_code != 200:
            return None, None
        
        soup = BeautifulSoup(response.content, 'lxml')
        
        isbn10, isbn13 = None, None
        
        # Buscar ISBN en diferentes posibles ubicaciones
        # Opción 1: Buscar texto que contenga "ISBN"
        for text in soup.find_all(string=re.compile(r'ISBN', re.IGNORECASE)):
            parent_text = text.parent.get_text()
            
            # Buscar ISBN13
            isbn13_match = re.search(r'ISBN13[:\s]+(\d{13})', parent_text)
            if isbn13_match:
                isbn13 = isbn13_match.group(1)
            
            # Buscar ISBN10
            isbn10_match = re.search(r'ISBN[:\s]+(\d{9}[\dXx])', parent_text)
            if isbn10_match:
                isbn10 = isbn10_match.group(1)
            
            if isbn13 or isbn10:
                break
        
        # Opción 2: Buscar en divs con class que contenga "isbn" o "bookData"
        if not isbn13 and not isbn10:
            isbn_divs = soup.find_all(['div', 'span'], class_=re.compile(r'isbn|bookData', re.IGNORECASE))
            for div in isbn_divs:
                text = div.get_text()
                
                isbn13_match = re.search(r'(\d{13})', text)
                if isbn13_match:
                    isbn13 = isbn13_match.group(1)
                
                isbn10_match = re.search(r'(\d{9}[\dXx])', text)
                if isbn10_match:
                    isbn10 = isbn10_match.group(1)
                
                if isbn13 or isbn10:
                    break
        
        return isbn10, isbn13
        
    except Exception as e:
        print(f"      ⚠ Error obteniendo ISBN: {e}")
        return None, None

def main():
    print("=" * 60)
    print("  SCRAPER DE GOODREADS - Proyecto RA1")
    print("=" * 60)
    
    # Crear carpeta landing/ si no existe
    os.makedirs('landing', exist_ok=True)
    
    # Scrapear varias páginas hasta un máximo de GOODREADS_MAX_BOOKS libros
    max_books = GOODREADS_MAX_BOOKS
    page = 1
    all_books = []
    current_idx = 1

    while len(all_books) < max_books:
        remaining = max_books - len(all_books)
        books_page, current_idx = scrape_search_page(page=page, start_idx=current_idx, max_books=remaining)

        if not books_page:
            break  # No hay más libros en esta página

        all_books.extend(books_page)
        print(f"➡️ Acumulados {len(all_books)} libros tras la página {page}")
        page += 1

    books = all_books

    print(f"\n📊 Total de libros scrapeados: {len(books)}")
    print(f"   - Con ISBN13: {sum(1 for b in books if b['isbn13'])}")
    print(f"   - Con ISBN10: {sum(1 for b in books if b['isbn10'])}")
    print(f"   - Sin ISBN: {sum(1 for b in books if not b['isbn13'] and not b['isbn10'])}")
    
    # Metadatos del scraping
    metadata = {
        'source': 'Goodreads',
        'search_query': GOODREADS_QUERY,
        'search_url': f"{BASE_SEARCH_URL}?q={GOODREADS_QUERY}&page=1",
        'user_agent': USER_AGENT,
        'scraping_timestamp': datetime.now().isoformat(),
        'total_books_scraped': len(books),
        'selectors_used': {
            'book_container': 'tr[itemtype*="Book"]',
            'title': 'a.bookTitle',
            'author': 'span[itemprop="author"]',
            'rating': 'span.minirating',
            'isbn_location': 'individual book pages'
        },
        'rate_limiting': {
            'pause_between_requests': '0.5-1.0 seconds',
            'implemented': True
        }
    }
    
    # Estructura final
    output = {
        'metadata': metadata,
        'books': books
    }
    
    # Guardar en landing/
    output_file = 'landing/goodreads_books.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n✅ Scraping completado!")
    print(f"📁 Guardado en: {output_file}")
    print(f"📦 Tamaño: {os.path.getsize(output_file) / 1024:.2f} KB")
    print("=" * 60)

if __name__ == '__main__':
    main()