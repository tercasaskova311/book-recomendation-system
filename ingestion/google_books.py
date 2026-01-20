import requests
import time
import logging
from typing import Optional, Dict
import os
from dotenv import load_dotenv
import re

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GoogleBooksClient:
    """
    Google Books API Client with improved ISBN handling
    FREE: 1000 requests/day (no key needed)
    """
    BASE_URL = "https://www.googleapis.com/books/v1/volumes"
    
    def __init__(self, api_key: str = None, rate_limit_delay: float = 0.1):
        self.api_key = api_key or os.getenv('GOOGLE_API_KEY')
        self.rate_limit_delay = rate_limit_delay
        self.requests_made = 0
        self.successful_requests = 0
        self.failed_requests = 0
    
    def clean_isbn(self, isbn: str) -> str:
        """
        Clean and validate ISBN
        - Remove hyphens, spaces
        - Keep only digits and X (for ISBN-10 check digit)
        - Validate length (10 or 13)
        """
        if not isbn:
            return None
        
        # Remove all non-alphanumeric except X
        cleaned = re.sub(r'[^0-9X]', '', str(isbn).upper())
        
        # Valid ISBNs are 10 or 13 characters
        if len(cleaned) not in [10, 13]:
            logger.warning(f"Invalid ISBN length: {isbn} -> {cleaned}")
            return None
        
        return cleaned
    
    def test_connection(self) -> bool:
        """Test API connection with a known ISBN"""
        test_isbn = "9780547928227"  # The Hobbit
        result = self.search_by_isbn(test_isbn)
        
        if result and result.get('title'):
            logger.info("✓ Google Books API connection successful!")
            return True
        
        logger.error("❌ Connection test failed")
        return False
    
    def search_by_isbn(self, isbn: str, retry_with_title: bool = False, 
                      title: str = None, author: str = None) -> Optional[Dict]:
        """
        Search for a book by ISBN with fallback strategies
        
        Args:
            isbn: ISBN to search
            retry_with_title: If True, fallback to title+author search
            title: Book title (for fallback)
            author: Book author (for fallback)
        
        Returns:
            Book data dict or None if not found
        """
        # Clean ISBN first
        isbn_clean = self.clean_isbn(isbn)
        
        if not isbn_clean:
            logger.warning(f"❌ Invalid ISBN format: {isbn}")
            # Try fallback if enabled
            if retry_with_title and title:
                return self.search_by_title_author(title, author, isbn)
            return None
        
        # Try ISBN-13 first, then ISBN-10
        for search_isbn in [isbn_clean]:
            params = {
                'q': f'isbn:{search_isbn}',
            }
            
            # Only add key if it exists
            if self.api_key:
                params['key'] = self.api_key
            
            try:
                self.requests_made += 1
                
                response = requests.get(self.BASE_URL, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                # Check if book found
                if data.get('totalItems', 0) > 0:
                    book_data = data['items'][0]['volumeInfo']
                    self.successful_requests += 1
                    logger.info(f" Found: {book_data.get('title')} (ISBN: {search_isbn})")
                    return self._parse_book_data(book_data, isbn)
                
            except requests.exceptions.RequestException as e:
                logger.error(f" Request error for ISBN {search_isbn}: {e}")
                self.failed_requests += 1
            
            except Exception as e:
                logger.error(f" Error for ISBN {search_isbn}: {e}")
                self.failed_requests += 1
            
            finally:
                time.sleep(self.rate_limit_delay)
        
        # If ISBN search failed, try title+author fallback
        if retry_with_title and title:
            logger.info(f" ISBN not found, trying title search: {title}")
            return self.search_by_title_author(title, author, isbn)
        
        logger.warning(f" ISBN {isbn} not found")
        self.failed_requests += 1
        return None
    
    def search_by_title_author(self, title: str, author: str = None, 
                               original_isbn: str = None) -> Optional[Dict]:
        """
        Fallback: Search by title and author when ISBN fails
        """
        if not title:
            return None
        
        # Build query
        query_parts = [f'intitle:{title}']
        if author:
            query_parts.append(f'inauthor:{author}')
        
        params = {
            'q': '+'.join(query_parts),
            'maxResults': 1
        }
        
        if self.api_key:
            params['key'] = self.api_key
        
        try:
            self.requests_made += 1
            
            response = requests.get(self.BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('totalItems', 0) > 0:
                book_data = data['items'][0]['volumeInfo']
                self.successful_requests += 1
                logger.info(f" Found by title: {book_data.get('title')}")
                return self._parse_book_data(book_data, original_isbn)
            
        except Exception as e:
            logger.error(f" Title search error: {e}")
            self.failed_requests += 1
        
        finally:
            time.sleep(self.rate_limit_delay)
        
        return None
    
    def _parse_book_data(self, book: Dict, original_isbn: str) -> Dict:
        """Parse Google Books API response"""
        
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
            'isbn': original_isbn,  # Keep original ISBN for database matching
            'title': book.get('title'),
            'description': book.get('description', ''),
            'authors': book.get('authors', []),
            'categories': book.get('categories', []),
            'publisher': book.get('publisher'),
            'published_date': book.get('publishedDate'),
            'page_count': book.get('pageCount'),
            'language': book.get('language', 'en'),
            'cover_url': cover_url,
            'google_isbn_10': isbn_10,
            'google_isbn_13': isbn_13
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


# ========== DIAGNOSTIC TOOL ==========
def diagnose_isbn_issues(csv_path: str, sample_size: int = 100):
    """
    Analyze ISBN quality in your Kaggle dataset
    """
    import pandas as pd
    
    df = pd.read_csv(csv_path, encoding='latin-1', on_bad_lines='skip')
    sample = df.head(sample_size)
    
    print(f"\n ISBN QUALITY ANALYSIS (sample: {sample_size})")
    
    # Check ISBN column
    if 'ISBN' in df.columns:
        isbn_col = 'ISBN'
    elif 'isbn' in df.columns:
        isbn_col = 'isbn'
    else:
        print(" No ISBN column found!")
        return
    
    total = len(sample)
    
    # 1. Missing ISBNs
    missing = sample[isbn_col].isna().sum()
    print(f"Missing ISBNs: {missing}/{total} ({missing/total*100:.1f}%)")
    
    # 2. ISBN length distribution
    sample['isbn_clean'] = sample[isbn_col].astype(str).str.replace(r'[^0-9X]', '', regex=True)
    sample['isbn_length'] = sample['isbn_clean'].str.len()
    
    print(f"\nISBN Length Distribution:")
    print(sample['isbn_length'].value_counts().sort_index())
    
    # 3. Valid ISBNs (10 or 13 digits)
    valid = sample[sample['isbn_length'].isin([10, 13])]
    print(f"\nValid ISBNs (10 or 13 chars): {len(valid)}/{total} ({len(valid)/total*100:.1f}%)")
    
    # 4. Sample of invalid ISBNs
    invalid = sample[~sample['isbn_length'].isin([10, 13])]
    if len(invalid) > 0:
        print(f"\n Sample of INVALID ISBNs:")
        for idx, row in invalid.head(5).iterrows():
            print(f"   {row[isbn_col]} -> cleaned: '{row['isbn_clean']}' (len={row['isbn_length']})")
    
    # 5. Sample of valid ISBNs
    if len(valid) > 0:
        print(f"\n Sample of VALID ISBNs:")
        for idx, row in valid.head(5).iterrows():
            print(f"   {row[isbn_col]} -> {row['isbn_clean']}")


# ========== TEST ==========
if __name__ == "__main__":
    import sys
    
    # Option 1: Run diagnostics on your Kaggle dataset
    if len(sys.argv) > 1 and sys.argv[1] == 'diagnose':
        diagnose_isbn_issues('data/Books.csv', sample_size=1000)
        sys.exit(0)
    
    # Option 2: Test API connection
    client = GoogleBooksClient(rate_limit_delay=0.1)
    
    if not client.test_connection():
        print("Connection test failed!")
        exit(1)
    
    print("\n TESTING ISBN SEARCHES")
    
    test_cases = [
        # (isbn, title, author) - for fallback testing
        ("9780547928227", "The Hobbit", "J.R.R. Tolkien"),
        ("0451524935", "1984", "George Orwell"),
        ("0195153448", "Classical Mythology", None),
        ("INVALID123", "Test Title", "Test Author"),  # Invalid ISBN
    ]
    
    for isbn, title, author in test_cases:
        print(f"\n Testing: {title} ({isbn})")
        
        # Try with fallback enabled
        book = client.search_by_isbn(
            isbn, 
            retry_with_title=True, 
            title=title, 
            author=author
        )
        
        if book:
            print(f"   SUCCESS!")
            print(f"      Title: {book['title']}")
            print(f"      Authors: {book['authors']}")
            print(f"      Description: {book['description'][:100]}..." if book['description'] else "      No description")
        else:
            print(f"    NOT FOUND")
    
    print("\n📊 API STATISTICS")
    stats = client.get_stats()
    for key, value in stats.items():
        print(f"   {key}: {value}")