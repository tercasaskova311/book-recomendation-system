import os
import time
import logging
import requests
import re
from typing import Optional, Dict

logger = logging.getLogger(__name__)

class GoogleBooksClient:
    BASE_URL = "https://www.googleapis.com/books/v1/volumes"

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 10,
        max_retries: int = 3,
        base_delay: float = 0.5,
    ):
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY")
        self.timeout = timeout
        self.max_retries = max_retries
        self.base_delay = base_delay

        self.requests_made = 0
        self.successful = 0
        self.failed = 0

    # ---------- ISBN ----------

    def clean_isbn(self, isbn: str) -> Optional[str]:
        if not isbn:
            return None

        cleaned = re.sub(r"[^0-9X]", "", str(isbn).upper())
        return cleaned if len(cleaned) in (10, 13) else None

    # ---------- Connectivity ----------

    def test_connection(self) -> bool:
        try:
            r = requests.get(
                self.BASE_URL,
                params={"q": "isbn:9780547928227"},
                timeout=5,
            )
            return r.status_code == 200
        except Exception:
            logger.exception("Google Books connection test failed")
            return False

    # ---------- Public API ----------

    def search_by_isbn(
        self,
        isbn: str,
        retry_with_title: bool = False,
        title: str | None = None,
        author: str | None = None,
    ) -> Optional[Dict]:

        isbn_clean = self.clean_isbn(isbn)
        if not isbn_clean:
            logger.warning(f"Invalid ISBN: {isbn}")
            return (
                self.search_by_title_author(title, author, isbn)
                if retry_with_title and title
                else None
            )

        params = {"q": f"isbn:{isbn_clean}"}
        if self.api_key:
            params["key"] = self.api_key

        data = self._request_with_retries(params)
        if not data or data.get("totalItems", 0) == 0:
            return (
                self.search_by_title_author(title, author, isbn)
                if retry_with_title and title
                else None
            )

        self.successful += 1
        return self._parse_book(data["items"][0]["volumeInfo"], isbn)

    def search_by_title_author(
        self,
        title: str,
        author: str | None = None,
        original_isbn: str | None = None,
    ) -> Optional[Dict]:

        if not title:
            return None

        query = [f"intitle:{title}"]
        if author:
            query.append(f"inauthor:{author}")

        params = {"q": "+".join(query), "maxResults": 1}
        if self.api_key:
            params["key"] = self.api_key

        data = self._request_with_retries(params)
        if not data or data.get("totalItems", 0) == 0:
            return None

        self.successful += 1
        return self._parse_book(data["items"][0]["volumeInfo"], original_isbn)

    # ---------- Internals ----------

    def _request_with_retries(self, params: Dict) -> Optional[Dict]:
        for attempt in range(1, self.max_retries + 1):
            try:
                self.requests_made += 1

                response = requests.get(
                    self.BASE_URL,
                    params=params,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException:
                self.failed += 1
                logger.exception(
                    f"Google Books request failed "
                    f"(attempt {attempt}/{self.max_retries})"
                )

                if attempt == self.max_retries:
                    raise

                sleep = self.base_delay * (2 ** (attempt - 1))
                time.sleep(sleep)

        return None

    def _parse_book(self, book: Dict, original_isbn: str) -> Dict:
        identifiers = {i["type"]: i["identifier"]
                       for i in book.get("industryIdentifiers", [])}

        images = book.get("imageLinks", {})
        cover = (
            images.get("large")
            or images.get("medium")
            or images.get("small")
            or images.get("thumbnail")
        )

        return {
            "isbn": original_isbn,
            "title": book.get("title"),
            "authors": book.get("authors", []),
            "description": book.get("description", ""),
            "categories": book.get("categories", []),
            "publisher": book.get("publisher"),
            "published_date": book.get("publishedDate"),
            "page_count": book.get("pageCount"),
            "language": book.get("language", "en"),
            "cover_url": cover,
            "google_isbn_10": identifiers.get("ISBN_10"),
            "google_isbn_13": identifiers.get("ISBN_13"),
        }

    def stats(self) -> Dict:
        total = max(self.requests_made, 1)
        return {
            "total_requests": self.requests_made,
            "successful": self.successful,
            "failed": self.failed,
            "success_rate": f"{100 * self.successful / total:.2f}%",
        }

# ========== TEST ==========
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    client = GoogleBooksClient() 

    if client.test_connection():
        logger.info("Google Books API connection successful")
    else:
        logger.error("Google Books API connection failed")
