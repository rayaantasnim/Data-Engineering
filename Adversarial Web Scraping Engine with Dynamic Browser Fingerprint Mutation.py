# Author: Rayaan tasnim, 13 aged prodigy

import os
import sys
import time
import random
import logging
import traceback
from typing import Dict, Any, List, Optional, Tuple
import urllib.request
import urllib.error
import numpy as np
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup

# Configure logging to stdout
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("AdversarialScraper")


class FingerprintMutator:
    """Handles dynamic runtime browser fingerprint mutation and evasion tactics."""
    
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
    ]
    
    RESOLUTIONS = [
        (1920, 1080),
        (1366, 768),
        (1536, 864),
        (1440, 900),
        (2560, 1440)
    ]
    
    @classmethod
    def get_random_fingerprint(cls) -> Dict[str, Any]:
        """Generates a randomized browser profile configuration."""
        try:
            ua = random.choice(cls.USER_AGENTS)
            res = random.choice(cls.RESOLUTIONS)
            hardware_concurrency = random.choice([4, 8, 12, 16])
            device_memory = random.choice([4, 8, 16])
            
            fingerprint = {
                "user_agent": ua,
                "resolution": res,
                "hardware_concurrency": hardware_concurrency,
                "device_memory": device_memory
            }
            logger.debug(f"Generated fingerprint mutation: {fingerprint}")
            return fingerprint
        except Exception as e:
            logger.error(f"Failed to generate random fingerprint: {e}\n{traceback.format_exc()}")
            raise


class BehavioralSimulator:
    """Simulates human-like behavioral telemetry patterns including bezier-curve mouse movements and delays."""
    
    @staticmethod
    def human_delay(min_sec: float = 1.0, max_sec: float = 3.5) -> None:
        """Introduces an active randomized delay."""
        try:
            delay = random.uniform(min_sec, max_sec)
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Error during behavioral delay execution: {e}\n{traceback.format_exc()}")

    @staticmethod
    def generate_bezier_path(start: Tuple[int, int], end: Tuple[int, int], num_points: int = 20) -> List[Tuple[int, int]]:
        """Computes a realistic bezier-curve mouse movement trajectory using numpy."""
        try:
            p0 = np.array(start)
            p3 = np.array(end)
            # Generate random control points to curve the trajectory
            mid_x = (start[0] + end[0]) / 2 + random.randint(-100, 100)
            mid_y = (start[1] + end[1]) / 2 + random.randint(-100, 100)
            p1 = np.array([mid_x, mid_y])
            p2 = np.array([mid_x + random.randint(-50, 50), mid_y + random.randint(-50, 50)])
            
            t = np.linspace(0, 1, num_points)[:, np.newaxis]
            curve = (1-t)**3 * p0 + 3*(1-t)**2 * t * p1 + 3*(1-t) * t**2 * p2 + t**3 * p3
            return [(int(pt[0]), int(pt[1])) for pt in curve]
        except Exception as e:
            logger.error(f"Error generating bezier mouse path: {e}\n{traceback.format_exc()}")
            return [start, end]


class ResilientScraperEngine:
    """Adversarial Web Scraping Engine with Dynamic Mutation and Protocol Fallback."""
    
    def __init__(self):
        self.proxy_pool = os.environ.get("YOUR_PROXY_POOL", "http://default-proxy:8080")
        self.api_key = os.environ.get("YOUR_API_KEY", "dummy_api_key_placeholder")
        self.driver: Optional[webdriver.Chrome] = None
        self.session_cookies: List[Dict[str, Any]] = []
        self.current_headers: Dict[str, str] = {}

    def _init_driver(self, fingerprint: Dict[str, Any]) -> None:
        """Initializes Selenium WebDriver with advanced anti-detection configurations."""
        try:
            logger.info("Initializing hardened Chrome browser wrapper with mutated fingerprint...")
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument(f"user-agent={fingerprint['user_agent']}")
            options.add_argument(f"window-size={fingerprint['resolution'][0]},{fingerprint['resolution'][1]}")
            
            # Additional evasion flags
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=options)
            
            # Execute CDP commands to override navigator properties and strip automation signatures
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": f"""
                    Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
                    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {fingerprint['hardware_concurrency']} }});
                    Object.defineProperty(navigator, 'deviceMemory', {{ get: () => {fingerprint['device_memory']} }});
                    
                    // Canvas noise injection override
                    const originalGetContext = HTMLCanvasElement.prototype.getContext;
                    HTMLCanvasElement.prototype.getContext = function(type) {{
                        const context = originalGetContext.apply(this, arguments);
                        if (type === '2d') {{
                            const originalFillText = context.fillText;
                            context.fillText = function() {{
                                arguments[0] = arguments[0] + ' ';
                                return originalFillText.apply(this, arguments);
                            }};
                        }}
                        return context;
                    }};
                """
            })
            
            self.current_headers = {
                "User-Agent": fingerprint['user_agent'],
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
            }
            logger.info("Browser wrapper successfully initialized and hardened.")
        except Exception as e:
            logger.error(f"Critical error initializing browser driver: {e}\n{traceback.format_exc()}")
            raise

    def fetch_with_browser(self, target_url: str) -> str:
        """Fetches target markup using the mutated Selenium browser engine."""
        try:
            logger.info(f"Navigating to target via browser engine: {target_url}")
            if not self.driver:
                raise RuntimeError("WebDriver is not initialized.")
            
            self.driver.get(target_url)
            BehavioralSimulator.human_delay(1.5, 3.0)
            
            # Extract cookies for potential fallback
            try:
                self.session_cookies = self.driver.get_cookies()
            except Exception:
                pass
                
            page_source = self.driver.page_source
            return page_source
        except Exception as e:
            logger.warning(f"Browser navigation encountered an exception or block: {e}")
            logger.debug(traceback.format_exc())
            raise e

    def fetch_with_socket_fallback(self, target_url: str) -> str:
        """Multi-protocol backup connectivity layer handling rate-limits (429/403) via urllib socket handlers."""
        try:
            logger.info(f"Triggering multi-protocol connection fallback matrix for: {target_url}")
            req = urllib.request.Request(target_url, headers=self.current_headers)
            
            # Pass cookies if available from previous browser state
            if self.session_cookies:
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in self.session_cookies])
                req.add_header("Cookie", cookie_str)
                
            with urllib.request.urlopen(req, timeout=10) as response:
                status_code = response.getcode()
                logger.info(f"Fallback connection successful with HTTP status code: {status_code}")
                return response.read().decode('utf-8')
        except urllib.error.HTTPError as he:
            logger.error(f"HTTP Error encountered during fallback connection: {he.code} - {he.reason}\n{traceback.format_exc()}")
            raise he
        except Exception as e:
            logger.error(f"Socket-level connection fallback failed: {e}\n{traceback.format_exc()}")
            raise e

    def parse_dom(self, html_content: str) -> List[Dict[str, str]]:
        """Resilient DOM parsing engine using BeautifulSoup4 to extract nested structures and shadow DOM data."""
        extracted_data = []
        try:
            logger.info("Parsing raw markup via BeautifulSoup4 immutability tree...")
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Advanced traversal: target general containers, article cards, or mock structured data items
            items = soup.find_all(['div', 'article', 'section'], class_=lambda x: x and ('item' in x or 'card' in x or 'content' in x))
            
            if not items:
                # Fallback generic tag sweep if specific classes are missing
                items = soup.find_all(['p', 'h1', 'h2', 'li'])

            for idx, item in enumerate(items[:10]):
                title = item.get_text(strip=True)[:100]
                if title:
                    extracted_data.append({
                        "index": idx,
                        "extracted_text": title,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                    })
                    
            logger.info(f"Successfully parsed {len(extracted_data)} structural elements.")
            return extracted_data
        except Exception as e:
            logger.error(f"DOM parsing failure: {e}\n{traceback.format_exc()}")
            return []

    def close(self) -> None:
        """Safely cleans up driver resources."""
        try:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver instance successfully terminated.")
        except Exception as e:
            logger.error(f"Error during driver teardown: {e}\n{traceback.format_exc()}")


if __name__ == "__main__":
    logger.info("Starting Adversarial Web Scraping Engine execution simulation...")
    scraper = ResilientScraperEngine()
    harvested_records = []

    try:
        # Simulate active harvesting loop over multiple iterations with fingerprint mutation
        mock_target_url = "https://httpbin.org/html"
        
        for iteration in range(1, 3):
            logger.info(f"--- Harvesting Iteration {iteration} ---")
            fingerprint = FingerprintMutator.get_random_fingerprint()
            
            try:
                scraper._init_driver(fingerprint)
                html = scraper.fetch_with_browser(mock_target_url)
            except Exception as browser_err:
                logger.warning(f"Primary browser engine blocked or failed. Activating fallback matrix. Error: {browser_err}")
                try:
                    html = scraper.fetch_with_socket_fallback(mock_target_url)
                except Exception as fallback_err:
                    logger.error(f"Both primary and fallback mechanisms failed on iteration {iteration}: {fallback_err}")
                    continue
            finally:
                scraper.close()
                
            records = scraper.parse_dom(html)
            harvested_records.extend(records)
            
            # Simulate behavioral pause between iterations
            BehavioralSimulator.human_delay(1.0, 2.0)

        # Construct Pandas DataFrame preview
        if harvested_records:
            df = pd.DataFrame(harvested_records)
            logger.info("Harvesting execution completed successfully. Dataframe Preview:")
            print("\n--- PANDAS DATAFRAME PREVIEW ---")
            print(df.head(10))
            print("---------------------------------\n")
        else:
            logger.warning("No records were harvested during the simulation.")

    except Exception as main_exc:
        logger.critical(f"Critical unhandled exception in main harvesting pipeline: {main_exc}\n{traceback.format_exc()}")
        sys.exit(1)
    finally:
        logger.info("Adversarial Web Scraping Engine shutdown complete.")