import requests
import time
import logging
from typing import Optional, Dict
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

#===== GOOGLE BOOKS API CALLS ==================================
class GoogleBooksClient:    
#FREE: 1000 requests/day (no key needed)    
    BASE_URL = "https://www.googleapis.com/books/v1/volumes"
    def __init__(self, api_key: str = None, rate_limit_delay: float = 0.1):
        self.api_key = api_key or os.getenv('GOOGLE_API_KEY')
        self.rate_limit_delay = rate_limit_delay
        self.requests_made = 0
        self.successful_requests = 0
        self.failed_requests = 0
    
    def test_connection(self) -> bool:
        """Test API connection with a known ISBN"""
        test_isbn = "9780547928227"  # The Hobbit
        result = self.search_by_isbn(test_isbn)
        
        if result and result.get('title'):
            logger.info(" Google Books API connection successful!")
            return True
        
        logger.error("Connection test failed")
        return False
    
    def search_by_isbn(self, isbn: str) -> Optional[Dict]: 
        #sending request to google books by isbn
        #Search for a book by ISBN
        #Returns: Book data dict or None if not found  
        isbn_clean = isbn.replace('-', '').replace(' ', '').strip()
        params = {
            'q': f'isbn:{isbn_clean}',
            'key': self.api_key  # Always include (even if None, requests handles it)
        }
        
        try:
            self.requests_made += 1
            
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Check if book found
            if data.get('totalItems', 0) == 0:
                logger.warning(f" ISBN {isbn} not found")
                self.failed_requests += 1
                return None
            
            # Parse first result
            book_data = data['items'][0]['volumeInfo']
            
            self.successful_requests += 1
            logger.info(f"✅ Found: {book_data.get('title')}")
            
            return self._parse_book_data(book_data, isbn)
            
        except requests.exceptions.RequestException as e:
            logger.error(f" X Request error for ISBN {isbn}: {e}")
            self.failed_requests += 1
            return None

        except Exception as e:
            logger.error(f"X Error for ISBN {isbn}: {e}")
            self.failed_requests += 1
            return None

        finally:
            time.sleep(self.rate_limit_delay)
    
    def _parse_book_data(self, book: Dict, original_isbn: str) -> Dict:        
        isbn_10 = None
        isbn_13 = None
        
        for identifier in book.get('industryIdentifiers', []):
            if identifier['type'] == 'ISBN_10':
                isbn_10 = identifier['identifier']

            elif identifier['type'] == 'ISBN_13':
                isbn_13 = identifier['identifier']
        
        # Get cover images
        image_links = book.get('imageLinks', {})
        cover_url = (
            image_links.get('large') or 
            image_links.get('medium') or 
            image_links.get('small') or 
            image_links.get('thumbnail')
        )
        
        return {
            'isbn': original_isbn,
            'title': book.get('title'),
            'description': book.get('description', ''),  
            'authors': book.get('authors', []),          
            'categories': book.get('categories', []),    
            'publisher': book.get('publisher'),
            'published_date': book.get('publishedDate'),
            'page_count': book.get('pageCount'),
            'language': book.get('language', 'en')
            }
    
    def get_stats(self) -> Dict:
        total = max(self.requests_made, 1)
        return {
            "total_requests": self.requests_made,
            "successful": self.successful_requests,
            "failed": self.failed_requests,
            "success_rate": f"{(self.successful_requests / total) * 100:.2f}%"
        }


# ========== TEST ==========
if __name__ == "__main__": 
    client = GoogleBooksClient(rate_limit_delay=0.1)
    
    if not client.test_connection():
        print("Connection test failed!")
        exit(1)
    
    print("TESTING ISBN SEARCHES")    
    test_isbns = [
        ("9780547928227", "The Hobbit"),
        ("9780439708180", "Harry Potter"),
        ("0451524935", "1984"),
        ("0195153448", "Classical Mythology"),
    ]
    
    for isbn, expected in test_isbns:
        print(f"\n Testing: {expected} ({isbn})")
        
        book = client.search_by_isbn(isbn)
        
        if book:
            print(f"SUCCESS!")
            print(f"      Title: {book['title']}")
        else:
            print(f"NOT FOUND")
    
    print("API STATISTICS")
    stats = client.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")