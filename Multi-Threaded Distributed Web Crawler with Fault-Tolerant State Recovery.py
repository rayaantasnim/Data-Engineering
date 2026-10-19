# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import time
import hashlib
import threading
import queue
import logging
import traceback
import json
import http.server
import socketserver
from typing import List, Set, Dict, Tuple, Optional, Any
import urllib.request
import urllib.parse
import urllib.error
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("ResilientCrawler")

class BloomFilter:
    """
    High-Performance Memory-Optimized Bloom Filter using NumPy binary bit-arrays.
    """
    def __init__(self, capacity: int = 100000, error_rate: float = 0.01):
        self.capacity = capacity
        self.error_rate = error_rate
        # Optimal bit array size m = - (n * ln(p)) / (ln(2)^2)
        self.size = int(-self.capacity * np.log(self.error_rate) / (np.log(2) ** 2))
        # Optimal number of hash functions k = (m / n) * ln(2)
        self.hash_count = int((self.size / self.capacity) * np.log(2))
        self.hash_count = max(1, self.hash_count)
        
        # Initialize bit array as a boolean NumPy array
        self.bit_array = np.zeros(self.size, dtype=bool)
        self.lock = threading.Lock()
        logger.info(f"Initialized BloomFilter: size={self.size} bits, hash_count={self.hash_count}")

    def _hashes(self, item: str) -> List[int]:
        """Generate k distinct hash positions using double hashing strategy."""
        encoded = item.encode('utf-8')
        h1 = int(hashlib.md5(encoded).hexdigest(), 16)
        h2 = int(hashlib.sha256(encoded).hexdigest(), 16)
        
        hashes = []
        for i in range(self.hash_count):
            combined = (h1 + i * h2) % self.size
            hashes.append(combined)
        return hashes

    def add(self, item: str) -> None:
        """Add an item to the Bloom filter."""
        with self.lock:
            indices = self._hashes(item)
            self.bit_array[indices] = True

    def __contains__(self, item: str) -> bool:
        """Check if an item might be in the set."""
        with self.lock:
            indices = self._hashes(item)
            return bool(np.all(self.bit_array[indices]))

    def serialize(self) -> Dict[str, Any]:
        """Serialize state for checkpointing."""
        with self.lock:
            return {
                "capacity": self.capacity,
                "error_rate": self.error_rate,
                "size": self.size,
                "hash_count": self.hash_count,
                "bit_array": self.bit_array.astype(int).tolist()
            }

    @classmethod
    def deserialize(cls, data: Dict[str, Any]) -> 'BloomFilter':
        """Restore state from checkpoint data."""
        bf = cls(capacity=data["capacity"], error_rate=data["error_rate"])
        bf.size = data["size"]
        bf.hash_count = data["hash_count"]
        bf.bit_array = np.array(data["bit_array"], dtype=bool)
        return bf


class AtomicCheckpointManager:
    """
    Manages atomic write-ahead transactional logging and crash state recovery.
    """
    def __init__(self, checkpoint_path: str):
        self.checkpoint_path = checkpoint_path
        self.lock = threading.Lock()

    def save_state(self, state: Dict[str, Any]) -> None:
        """Atomically write state using a write-ahead temp file and atomic rename."""
        with self.lock:
            temp_path = f"{self.checkpoint_path}.tmp"
            try:
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(state, f, indent=4)
                os.replace(temp_path, self.checkpoint_path)
                logger.info(f"Atomic checkpoint successfully saved to {self.checkpoint_path}")
            except Exception as e:
                logger.error(f"Failed to save atomic checkpoint: {e}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                raise

    def load_state(self) -> Optional[Dict[str, Any]]:
        """Load state from disk if checkpoint exists."""
        with self.lock:
            if not os.path.exists(self.checkpoint_path):
                logger.info("No existing checkpoint found. Starting fresh.")
                return None
            try:
                with open(self.checkpoint_path, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                logger.info(f"Checkpoint successfully loaded from {self.checkpoint_path}")
                return state
            except Exception as e:
                logger.error(f"Failed to load checkpoint file: {e}")
                return None


class ResilientDistributedCrawler:
    """
    Multi-Threaded Distributed Web Crawler with Fault-Tolerant State Recovery.
    """
    def __init__(
        self,
        seed_urls: List[str],
        checkpoint_file: Optional[str] = None,
        max_threads: int = 4,
        max_pages: int = 50,
        request_timeout: float = 5.0
    ):
        self.seed_urls = seed_urls
        self.max_threads = max_threads
        self.max_pages = max_pages
        self.request_timeout = request_timeout
        
        # Configuration safety via environment variables with fallbacks
        checkpoint_dir = os.environ.get("CRAWLER_CHECKPOINT_DIR", ".")
        os.makedirs(checkpoint_dir, exist_ok=True)
        self.checkpoint_file = checkpoint_file or os.path.join(checkpoint_dir, "crawler_state.json")
        
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0"
        ]
        
        self.url_queue: queue.Queue = queue.Queue()
        self.bloom_filter = BloomFilter(capacity=10000)
        self.checkpoint_manager = AtomicCheckpointManager(self.checkpoint_file)
        
        self.visited_count = 0
        self.crawl_results: List[Dict[str, Any]] = []
        self.results_lock = threading.Lock()
        self.is_running = threading.Event()

    def restore_or_initialize(self) -> None:
        """Initialize state from checkpoint or seed URLs."""
        state = self.checkpoint_manager.load_state()
        if state:
            try:
                self.bloom_filter = BloomFilter.deserialize(state["bloom_filter"])
                self.visited_count = state["visited_count"]
                self.crawl_results = state["crawl_results"]
                
                # Re-queue unvisited URLs from queue state
                for url in state["queue_items"]:
                    self.url_queue.put(url)
                logger.info(f"State restored successfully. Resuming queue with {self.url_queue.qsize()} items.")
                return
            except Exception as e:
                logger.error(f"Error restoring state, falling back to seed URLs: {e}")
                traceback.print_exc()

        # Fallback to seeds
        for url in self.seed_urls:
            if url not in self.bloom_filter:
                self.bloom_filter.add(url)
                self.url_queue.put(url)
        logger.info(f"Initialized crawler with {len(self.seed_urls)} seed URLs.")

    def checkpoint(self) -> None:
        """Create a full transactional state snapshot and save atomically."""
        try:
            # Drain queue items for snapshot without losing them
            queue_items = []
            temp_list = []
            while not self.url_queue.empty():
                try:
                    item = self.url_queue.get_nowait()
                    temp_list.append(item)
                    queue_items.append(item)
                except queue.Empty:
                    break
            for item in temp_list:
                self.url_queue.put(item)

            state = {
                "visited_count": self.visited_count,
                "crawl_results": self.crawl_results,
                "queue_items": queue_items,
                "bloom_filter": self.bloom_filter.serialize()
            }
            self.checkpoint_manager.save_state(state)
        except Exception as e:
            logger.error(f"Failed during checkpoint snapshot creation: {e}")

    def fetch_page(self, url: str) -> Optional[str]:
        """Perform HTTP request with timeout, User-Agent rotation, and robust exception handling."""
        ua = np.random.choice(self.user_agents)
        headers = {"User-Agent": ua}
        
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=self.request_timeout) as response:
                status_code = response.getcode()
                if 200 <= status_code < 300:
                    html_content = response.read().decode('utf-8', errors='ignore')
                    return html_content
                else:
                    logger.warning(f"Non-200 status code {status_code} received for {url}")
        except urllib.error.HTTPError as e:
            logger.error(f"HTTP error {e.code} while accessing {url}: {e.reason}")
        except urllib.error.URLError as e:
            logger.error(f"URL error while accessing {url}: {e.reason}")
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
        return None

    def parse_links(self, base_url: str, html_content: str) -> List[str]:
        """Extract and normalize absolute hyperlink paths from anchor tags using BeautifulSoup."""
        discovered_links = []
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for anchor in soup.find_all('a', href=True):
                href = anchor['href']
                absolute_url = urllib.parse.urljoin(base_url, href)
                # Parse and validate scheme
                parsed = urllib.parse.urlparse(absolute_url)
                if parsed.scheme in ('http', 'https'):
                    # Strip fragment identifiers for clean deduplication
                    clean_url = parsed._replace(fragment='').geturl()
                    discovered_links.append(clean_url)
        except Exception as e:
            logger.error(f"Error parsing links from {base_url}: {e}")
        return discovered_links

    def worker(self) -> None:
        """Worker thread loop consuming queue and processing link traversal."""
        while self.is_running.is_set():
            try:
                try:
                    url = self.url_queue.get(timeout=1.0)
                except queue.Empty:
                    continue

                if self.visited_count >= self.max_pages:
                    self.url_queue.task_done()
                    break

                logger.info(f"Crawling target: {url}")
                html_content = self.fetch_page(url)
                
                with self.results_lock:
                    self.visited_count += 1
                    status = "SUCCESS" if html_content else "FAILED"
                    self.crawl_results.append({
                        "url": url,
                        "status": status,
                        "timestamp": time.time()
                    })

                if html_content:
                    links = self.parse_links(url, html_content)
                    for link in links:
                        if link not in self.bloom_filter:
                            self.bloom_filter.add(link)
                            self.url_queue.put(link)

                self.url_queue.task_done()
                
                # Periodic checkpointing every 5 visited pages
                if self.visited_count % 5 == 0:
                    self.checkpoint()

            except Exception as e:
                logger.error(f"Unhandled exception in crawler worker thread: {e}")
                traceback.print_exc()

    def start_crawl(self) -> None:
        """Start the multi-threaded crawling engine."""
        self.restore_or_initialize()
        self.is_running.set()

        threads = []
        for i in range(self.max_threads):
            t = threading.Thread(target=self.worker, name=f"CrawlerWorker-{i+1}")
            t.daemon = True
            threads.append(t)
            t.start()

        logger.info(f"Launched {self.max_threads} worker threads. Beginning crawl loop...")
        
        # Simulate active crawling until max pages reached or queue exhausted
        while self.is_running.is_set() and self.visited_count < self.max_pages:
            if self.url_queue.empty() and all(not t.is_alive() for t in threads):
                break
            time.sleep(0.5)

        # Stop workers gracefully
        self.is_running.clear()
        for t in threads:
            t.join(timeout=2.0)

        # Final checkpoint snapshot
        self.checkpoint()
        logger.info(f"Crawl completed. Total pages processed: {self.visited_count}")


# Simulated Local Web Server to provide a completely self-contained offline testbed
class MockWebHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        
        path = self.path
        html = f"""
        <html>
            <head><title>Mock Page {path}</title></head>
            <body>
                <h1>Welcome to Mock Server Node: {path}</h1>
                <p>Simulating robust distributed link traversal graph.</p>
                <ul>
                    <li><a href="/page-1">Page 1</a></li>
                    <li><a href="/page-2">Page 2</a></li>
                    <li><a href="/page-3">Page 3</a></li>
                    <li><a href="http://127.0.0.1:8989/external">External Mirror</a></li>
                </ul>
            </body>
        </html>
        """
        self.wfile.write(html.encode('utf-8'))
    
    def log_message(self, format, *args):
        # Suppress standard server request logging noise in stdout
        pass


def run_local_server(port: int = 8989) -> Tuple[http.server.HTTPServer, threading.Thread]:
    server = socketserver.TCPServer(("127.0.0.1", port), MockWebHandler)
    server_thread = threading.Thread(target=server.serve_forever, name="MockWebServer")
    server_thread.daemon = True
    server_thread.start()
    return server, server_thread


if __name__ == "__main__":
    logger.info("Starting Resilient Distributed Web Crawler Simulation...")

    # 1. Spin up a local mock web server for offline testing
    server, server_thread = run_local_server(8989)
    seed_url = "http://127.0.0.1:8989/"

    checkpoint_file = "test_crawler_checkpoint.json"
    # Clean up any prior test artifacts
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

    try:
        # Phase 1: Start crawler and run partially
        logger.info("=== PHASE 1: Initial Crawl Execution ===")
        crawler = ResilientDistributedCrawler(
            seed_urls=[seed_url],
            checkpoint_file=checkpoint_file,
            max_threads=2,
            max_pages=4,
            request_timeout=3.0
        )
        
        # Run crawler thread or inline for phase 1
        crawler_thread = threading.Thread(target=crawler.start_crawl, name="CrawlerManager")
        crawler_thread.start()
        crawler_thread.join(timeout=3.0)
        
        # Simulate accidental forced crash / interruption
        logger.info("=== SIMULATING ACCIDENTAL FORCED CRASH MID-FLIGHT ===")
        crawler.is_running.clear()
        
        # Phase 2: State Recovery and Resumption from Disk Checkpoint
        logger.info("=== PHASE 2: State Recovery & Resumption from Checkpoint ===")
        resumed_crawler = ResilientDistributedCrawler(
            seed_urls=[seed_url],
            checkpoint_file=checkpoint_file,
            max_threads=2,
            max_pages=10,
            request_timeout=3.0
        )
        
        resumed_crawler.start_crawl()

        # Phase 3: Visualization & Analysis via Pandas DataFrame
        logger.info("=== PHASE 3: Mapping Results via Pandas Visualization ===")
        if resumed_crawler.crawl_results:
            df = pd.DataFrame(resumed_crawler.crawl_results)
            print("\n--- Crawl Metrics Summary DataFrame ---")
            print(df.to_string(index=False))
            print(f"\nTotal unique records processed & validated: {len(df)}")
        else:
            logger.warning("No crawl results recorded.")

    except Exception as e:
        logger.error(f"Fatal error during execution pipeline: {e}")
        traceback.print_exc()
    finally:
        # Shutdown mock server
        server.shutdown()
        server.server_close()
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        logger.info("Crawler simulation completed successfully and resources cleaned up.")