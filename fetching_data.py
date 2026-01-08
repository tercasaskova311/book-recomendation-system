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


class GoogleBooksClient:
    """
    Google Books API client
    
    FREE: 1000 requests/day (no key needed)
    With API key: 1000 requests/day (but tracked per project)
    """
    
    BASE_URL = "https://www.googleapis.com/books/v1/volumes"
    
    def __init__(self, api_key: str = None, rate_limit_delay: float = 0.1):
        self.api_key = api_key or os.getenv('GOOGLE_API_KEY')

        self.rate_limit_delay = rate_limit_delay
        
        self.requests_made = 0
        self.successful_requests = 0
        self.failed_requests = 0
    
    def test_connection(self) -> bool:
        """Test API connection"""
        try:
            response = requests.get(
                f"{self.BASE_URL}?q=isbn:9780547928227",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get('totalItems', 0) > 0:
                    logger.info("✅ Google Books API connection successful!")
                    return True
            
            logger.error("❌ Connection failed")
            return False
            
        except Exception as e:
            logger.error(f"❌ Connection error: {e}")
            return False
    
    def search_by_isbn(self, isbn: str) -> Optional[Dict]:
        """
        Search for a book by ISBN
        
        Args:
            isbn: ISBN-10 or ISBN-13
            
        Returns:
            Book data dict or None if not found
        """
        
        # Clean ISBN
        isbn_clean = isbn.replace('-', '').replace(' ', '').strip()
        
        # Build URL
        url = f"{self.BASE_URL}?q=isbn:{isbn_clean}"
        
        if self.api_key:
            url += f"&key={self.api_key}"
        
        try:
            self.requests_made += 1
            
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if book found
            if data.get('totalItems', 0) == 0:
                logger.warning(f"⚠️  ISBN {isbn} not found")
                self.failed_requests += 1
                return None
            
            # Parse first result
            book_data = data['items'][0]['volumeInfo']
            
            self.successful_requests += 1
            logger.info(f"✅ Found: {book_data.get('title')}")
            
            return self._parse_book_data(book_data, isbn)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request error for ISBN {isbn}: {e}")
            self.failed_requests += 1
            return None
        except Exception as e:
            logger.error(f"❌ Error for ISBN {isbn}: {e}")
            self.failed_requests += 1
            return None
        finally:
            time.sleep(self.rate_limit_delay)
    
    def _parse_book_data(self, book: Dict, original_isbn: str) -> Dict:
        """Parse Google Books API response"""
        
        # Extract ISBNs
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
            'isbn_10': isbn_10,
            'isbn_13': isbn_13,
            'google_id': book.get('id'),
            'title': book.get('title'),
            'subtitle': book.get('subtitle'),
            'description': book.get('description', ''),
            'authors': book.get('authors', []),
            'publisher': book.get('publisher'),
            'published_date': book.get('publishedDate'),
            'page_count': book.get('pageCount'),
            'categories': book.get('categories', []),
            'language': book.get('language'),
            'preview_link': book.get('previewLink'),
            'info_link': book.get('infoLink'),
            'cover_url': cover_url,
            'average_rating': book.get('averageRating'),
            'ratings_count': book.get('ratingsCount'),
        }
    
    def get_stats(self) -> Dict:
        """Get API usage statistics"""
        total = max(self.requests_made, 1)
        return {
            "total_requests": self.requests_made,
            "successful": self.successful_requests,
            "failed": self.failed_requests,
            "success_rate": f"{(self.successful_requests / total) * 100:.2f}%"
        }


# ========== TEST ==========
if __name__ == "__main__":
    print("="*70)
    print("TESTING GOOGLE BOOKS API")
    print("="*70)
    
    client = GoogleBooksClient(rate_limit_delay=0.1)
    
    if not client.test_connection():
        print("❌ Connection test failed!")
        exit(1)
    
    print("\n" + "="*70)
    print("TESTING ISBN SEARCHES")
    print("="*70)
    
    # Test with various ISBNs
    test_isbns = [
        ("9780547928227", "The Hobbit"),
        ("9780439708180", "Harry Potter"),
        ("0451524935", "1984"),
        ("0195153448", "Classical Mythology"),
    ]
    
    for isbn, expected in test_isbns:
        print(f"\n📚 Testing: {expected} ({isbn})")
        
        book = client.search_by_isbn(isbn)
        
        if book:
            print(f"   ✅ SUCCESS!")
            print(f"      Title: {book['title']}")
            print(f"      Authors: {book['authors']}")
            print(f"      Categories: {book['categories']}")
            print(f"      Pages: {book['page_count']}")
            print(f"      Rating: {book['average_rating']} ({book['ratings_count']} ratings)")
            
            desc_len = len(book.get('description', ''))
            print(f"      Description: {desc_len} chars")
            
            if desc_len > 0:
                print(f"      Preview: {book['description'][:100]}...")
        else:
            print(f"   ❌ NOT FOUND")
    
    print("\n" + "="*70)
    print("API STATISTICS")
    print("="*70)
    stats = client.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")