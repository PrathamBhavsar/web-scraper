# file_downloader.py
import os
import time
import logging
import requests
import threading
import asyncio
import shutil
import tempfile
from pathlib import Path
from urllib.parse import urlparse
from typing import List, Optional, Callable, Dict, Any

# Retry utilities
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional crawl4ai imports will be attempted at runtime. If not present, code will fallback.
try:
    from crawl4ai import AsyncWebCrawler
    from crawl4ai.async_configs import BrowserConfig, CrawlerRunConfig
    CRAWL4AI_AVAILABLE = True
except Exception:
    AsyncWebCrawler = None
    BrowserConfig = None
    CrawlerRunConfig = None
    CRAWL4AI_AVAILABLE = False


class FileDownloader:
    """
    Improved FileDownloader that:
    - Uses crawl4ai (if available) for parallel and browser-based file downloads.
    - Falls back to requests + threads streaming when crawl4ai isn't installed.
    - Reports per-file progress via logger and optional progress_callback(filename, percent).
    - Handles Crawl4AI wait_for correctly (only strings/lists), uses numeric timeouts as wait_for_timeout.
    - Implements resume/retry on read timeouts for requests-based streaming.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config or {}
        self.base_url = self.config.get("general", {}).get("base_url", "https://rule34video.com")
        self.logger = logging.getLogger('Rule34Scraper')
        # Ensure download directory exists
        download_path = Path(self.config.get("general", {}).get("download_path", "C:\\scraper_downloads\\"))
        download_path.mkdir(parents=True, exist_ok=True)

    # ------------------------
    # Utility helpers
    # ------------------------
    def _get_headers(self):
        return {
            'User-Agent': self.config.get("general", {}).get("user_agent", "Mozilla/5.0"),
            'Referer': self.base_url
        }

    def _ensure_parent(self, filepath: Path):
        filepath.parent.mkdir(parents=True, exist_ok=True)

    def _get_file_name_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        name = os.path.basename(parsed.path) or parsed.netloc
        return name

    def _get_expected_size(self, url: str, timeout: int = 5) -> Optional[int]:
        """
        Try to get Content-Length via HEAD. Returns int bytes or None.
        """
        try:
            h = self._get_headers()
            r = requests.head(url, headers=h, allow_redirects=True, timeout=timeout)
            if r.status_code == 200:
                cl = r.headers.get('Content-Length') or r.headers.get('content-length')
                if cl and cl.isdigit():
                    return int(cl)
        except Exception:
            pass
        return None

    # ------------------------
    # Crawl4AI-based download helpers (wait_for handling fixed)
    # ------------------------
    async def _crawl4ai_arun_single(self, url: str, downloads_path: str, run_config_args: dict):
        """
        Run a single crawl that allows downloads. Returns the CrawlResult (dict-like).
        run_config_args may include 'wait_for', 'wait_for_timeout', etc.
        """
        config = BrowserConfig(accept_downloads=True, downloads_path=downloads_path)
        async with AsyncWebCrawler(config=config) as crawler:
            run_conf = CrawlerRunConfig(**run_config_args)
            result = await crawler.arun(url=url, config=run_conf)
            return result

    async def _crawl4ai_arun_many(self, urls: List[str], downloads_path: str, run_config_args: dict):
        """
        Run many crawls concurrently. Returns list/iterable of results.
        """
        config = BrowserConfig(accept_downloads=True, downloads_path=downloads_path)
        async with AsyncWebCrawler(config=config) as crawler:
            run_conf = CrawlerRunConfig(**run_config_args)
            results = await crawler.arun_many(urls, config=run_conf)
            return results

    # ------------------------
    # Monitoring thread for progress
    # ------------------------
    def _start_monitor_thread(self, downloads_dir: Path, expected_map: Dict[str, Optional[int]],
                              stop_event: threading.Event,
                              progress_callback: Optional[Callable[[str, float], None]] = None,
                              poll_interval: float = 0.5):
        """
        Starts a background thread to monitor files appearing/ growing in downloads_dir.
        expected_map: mapping filename -> expected_bytes (or None)
        Calls progress_callback(filename_or_path, percent_or_mb) if provided and logs progress.
        """

        def monitor():
            observed_done = set()
            while not stop_event.is_set():
                for name, expected in expected_map.items():
                    try:
                        candidate = downloads_dir / name
                        if not candidate.exists():
                            continue
                        current = candidate.stat().st_size
                    except Exception:
                        continue

                    if expected and expected > 0:
                        percent = (current / expected) * 100.0
                        percent = min(100.0, percent)
                        percent_label = f"{percent:.2f}%"
                    else:
                        # if expected unknown, show MB downloaded
                        percent = float(current) / (1024 * 1024)
                        percent_label = f"{percent:.2f} MB"

                    self.logger.info(f"[PROGRESS] {name}: {percent_label}")
                    if progress_callback:
                        try:
                            progress_callback(str(candidate), percent)
                        except Exception:
                            self.logger.debug("progress_callback raised an exception", exc_info=True)

                    if expected and percent >= 99.9:
                        self.logger.info(f"[DONE] {name}")
                        observed_done.add(name)
                time.sleep(poll_interval)

        t = threading.Thread(target=monitor, daemon=True)
        t.start()
        return t

    # ------------------------
    # Main download_file (keeps original signature)
    # ------------------------
    def download_file(self, url: str, filepath: Path, progress_callback: Optional[Callable[[str, float], None]] = None) -> bool:
        """
        Download a single file and verify it. Uses Crawl4AI when available (in a blocking way).
        progress_callback(filepath_or_name, percent_or_mb) - optional.
        """
        filepath = Path(filepath)
        self._ensure_parent(filepath)

        download_conf = self.config.get("download", {})
        timeout_seconds = int(download_conf.get("timeout_seconds", 60))
        max_retries = int(download_conf.get("max_retries", 2))

        # Prepare run_config_args for Crawl4AI: pass wait_for only when config specifies a string/list
        scraping_conf = self.config.get("scraping", {})
        run_config_args = {}
        configured_wait_for = scraping_conf.get("wait_for", None)
        # Accept only string or list/tuple for wait_for
        if isinstance(configured_wait_for, (str, list, tuple)):
            run_config_args['wait_for'] = configured_wait_for
        # Always pass wait_for_timeout if numeric (ms)
        wait_time_ms = scraping_conf.get("wait_time_ms", None)
        if isinstance(wait_time_ms, (int, float)):
            run_config_args['wait_for_timeout'] = int(wait_time_ms)

        # If Crawl4AI is available, try to use it to trigger browser-based downloads
        if CRAWL4AI_AVAILABLE:
            try:
                configured_download_dir = Path(self.config.get("general", {}).get("download_path", tempfile.mkdtemp()))
                configured_download_dir.mkdir(parents=True, exist_ok=True)

                # Determine expected filename (best effort)
                expected_name = self._get_file_name_from_url(url)
                if not expected_name:
                    expected_name = filepath.name

                expected_size = self._get_expected_size(url, timeout=5)
                expected_map = {expected_name: expected_size}

                stop_event = threading.Event()
                monitor_thread = self._start_monitor_thread(configured_download_dir, expected_map, stop_event, progress_callback)

                # Run crawl4ai in its own event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result = loop.run_until_complete(self._crawl4ai_arun_single(url, str(configured_download_dir), run_config_args))
                finally:
                    try:
                        loop.stop()
                        loop.close()
                    except Exception:
                        pass

                time.sleep(0.2)
                stop_event.set()
                monitor_thread.join(timeout=2)

                # Collect downloaded files info
                downloaded = []
                try:
                    downloaded = list(getattr(result, "downloaded_files", []) or [])
                except Exception:
                    try:
                        downloaded = result.get("downloaded_files", []) or []
                    except Exception:
                        downloaded = []

                if not downloaded:
                    self.logger.warning("Crawl4AI didn't report downloaded files; falling back to requests for URL: %s", url)
                    return self._requests_stream_download(url, filepath, progress_callback=progress_callback)

                # Pick candidate and move/copy to destination
                chosen = None
                for d in downloaded:
                    if os.path.basename(d) == expected_name:
                        chosen = d
                        break
                if not chosen:
                    chosen = downloaded[0]

                try:
                    shutil.move(chosen, str(filepath))
                except Exception:
                    try:
                        shutil.copy2(chosen, str(filepath))
                        os.remove(chosen)
                    except Exception as e:
                        self.logger.error("Failed moving/copying downloaded file: %s", e)
                        return False

                if self.verify_download_integrity(filepath):
                    self.logger.info("Successfully downloaded (crawl4ai): %s", filepath)
                    return True
                else:
                    self.logger.warning("Downloaded file failed verification (crawl4ai): %s", filepath)
                    try:
                        filepath.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return False

            except Exception as e:
                # If any error occurs using Crawl4AI, log and fallback to requests-based streaming.
                self.logger.exception("Error using Crawl4AI download for %s: %s", url, e)
                return self._requests_stream_download(url, filepath, progress_callback=progress_callback)

        # No Crawl4AI or fallback
        return self._requests_stream_download(url, filepath, progress_callback=progress_callback)

    # ------------------------
    # Parallel downloads (multi-file) - uses crawl4ai if available, else threaded requests
    # ------------------------
    def download_files_parallel(self, urls: List[str], filepaths: List[Path],
                                progress_callback: Optional[Callable[[str, float], None]] = None) -> List[bool]:
        """
        Download multiple files in parallel. Returns list of booleans (success per file).
        """
        assert len(urls) == len(filepaths), "urls and filepaths must be same length"
        for p in filepaths:
            self._ensure_parent(Path(p))

        if CRAWL4AI_AVAILABLE:
            configured_download_dir = Path(self.config.get("general", {}).get("download_path", tempfile.mkdtemp()))
            configured_download_dir.mkdir(parents=True, exist_ok=True)

            expected_map = {}
            for url, fp in zip(urls, filepaths):
                name = self._get_file_name_from_url(url) or fp.name
                expected_map[name] = self._get_expected_size(url, timeout=5)

            scraping_conf = self.config.get("scraping", {})
            run_config_args = {}
            configured_wait_for = scraping_conf.get("wait_for", None)
            if isinstance(configured_wait_for, (str, list, tuple)):
                run_config_args['wait_for'] = configured_wait_for
            wait_time_ms = scraping_conf.get("wait_time_ms", None)
            if isinstance(wait_time_ms, (int, float)):
                run_config_args['wait_for_timeout'] = int(wait_time_ms)

            stop_event = threading.Event()
            monitor_thread = self._start_monitor_thread(configured_download_dir, expected_map, stop_event, progress_callback)

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                results_iterable = loop.run_until_complete(self._crawl4ai_arun_many(urls, str(configured_download_dir), run_config_args))
                results = []
                try:
                    async def collect_async_iter(it):
                        res = []
                        async for x in it:
                            res.append(x)
                        return res
                    results = loop.run_until_complete(collect_async_iter(results_iterable))
                except Exception:
                    try:
                        results = list(results_iterable)
                    except Exception:
                        results = results_iterable or []
            finally:
                try:
                    loop.stop()
                    loop.close()
                except Exception:
                    pass

            time.sleep(0.1)
            stop_event.set()
            monitor_thread.join(timeout=2)

            success_list = [False] * len(urls)
            downloaded_files_map = {}
            for res in results:
                downloaded_files = getattr(res, "downloaded_files", None)
                if downloaded_files is None:
                    try:
                        downloaded_files = res.get("downloaded_files", []) or []
                    except Exception:
                        downloaded_files = []
                for d in downloaded_files:
                    b = os.path.basename(d)
                    downloaded_files_map.setdefault(b, []).append(d)

            used = set()
            for idx, (url, fp) in enumerate(zip(urls, filepaths)):
                name = self._get_file_name_from_url(url) or fp.name
                candidates = downloaded_files_map.get(name) or []
                chosen = None
                for c in candidates:
                    if c not in used:
                        chosen = c
                        break
                if not chosen and downloaded_files_map:
                    # pick any unused file
                    for k, arr in downloaded_files_map.items():
                        for c in arr:
                            if c not in used:
                                chosen = c
                                break
                        if chosen:
                            break

                if chosen:
                    try:
                        shutil.move(chosen, str(fp))
                        used.add(chosen)
                        if self.verify_download_integrity(fp):
                            self.logger.info("Downloaded and verified: %s", fp)
                            success_list[idx] = True
                        else:
                            self.logger.warning("Verification failed: %s", fp)
                            try:
                                fp.unlink(missing_ok=True)
                            except Exception:
                                pass
                    except Exception as e:
                        self.logger.exception("Error moving downloaded file %s -> %s: %s", chosen, fp, e)
                        try:
                            shutil.copy2(chosen, str(fp))
                            if self.verify_download_integrity(fp):
                                success_list[idx] = True
                        except Exception:
                            self.logger.exception("Fallback copy failed for %s", fp)
                else:
                    self.logger.warning("No downloaded file found for %s", url)
                    success_list[idx] = False

            return success_list

        # Fallback to threaded requests downloads with resume support
        results = []
        threads = []
        results_lock = threading.Lock()

        def worker(u, p, out_idx):
            ok = self._requests_stream_download(u, p, progress_callback=progress_callback)
            with results_lock:
                results.append((out_idx, ok))

        for i, (u, p) in enumerate(zip(urls, filepaths)):
            t = threading.Thread(target=worker, args=(u, p, i), daemon=True)
            threads.append(t)
            t.start()
            par = int(self.config.get("general", {}).get("parallel_downloads", 3) or 3)
            while threading.active_count() > par + 5:
                time.sleep(0.05)

        for t in threads:
            t.join()

        results_sorted = sorted(results, key=lambda x: x[0])
        return [r[1] for r in results_sorted]

    # ------------------------
    # requests streaming download (blocking) with resume and retry
    # ------------------------
    def _requests_stream_download(self, url: str, filepath: Path,
                                  progress_callback: Optional[Callable[[str, float], None]] = None) -> bool:
        """
        Stream file via requests and report progress. Supports resume via Range header + retries.
        """
        headers_base = self._get_headers()
        download_conf = self.config.get("download", {})
        max_retries = int(download_conf.get("max_retries", 3))
        chunk_size = int(download_conf.get("chunk_size", 8192) or 8192)
        connect_timeout = int(download_conf.get("connect_timeout_seconds", 10) or 10)
        read_timeout = int(download_conf.get("read_timeout_seconds", 120) or 120)
        session = requests.Session()

        # Configure retries for connection-level errors
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            backoff_factor=1
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_maxsize=10)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        tmp_path = filepath.with_suffix(filepath.suffix + ".part")
        self._ensure_parent(tmp_path)

        attempt = 0
        while attempt <= max_retries:
            attempt += 1
            try:
                # If we already have partial bytes, attempt to resume
                existing = 0
                if tmp_path.exists():
                    try:
                        existing = tmp_path.stat().st_size
                    except Exception:
                        existing = 0

                headers = dict(headers_base)  # copy
                if existing > 0:
                    # Use Range header to request remainder (server must support it)
                    headers['Range'] = f'bytes={existing}-'
                    mode = 'ab'
                else:
                    mode = 'wb'

                # Stream with connect/read timeout tuple
                with session.get(url, stream=True, timeout=(connect_timeout, read_timeout), headers=headers) as r:
                    r.raise_for_status()

                    # If server responded 200 while we requested Range, we need to rewrite file (server doesn't accept Range)
                    if existing > 0 and r.status_code == 200:
                        # restart saving from scratch (overwrite tmp)
                        self.logger.debug("Server ignored Range header; restarting download from zero for %s", filepath)
                        existing = 0
                        mode = 'wb'

                    total = None
                    content_length = r.headers.get('Content-Length')
                    if content_length and content_length.isdigit():
                        total = int(content_length)
                        if 'Range' in headers and r.status_code == 206:
                            # When 206 Partial, total is the remaining bytes; add existing to get full size
                            total = existing + total

                    downloaded = existing
                    # Write data
                    with open(tmp_path, mode) as f:
                        for chunk in r.iter_content(chunk_size=chunk_size):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)
                                if total:
                                    percent = (downloaded / total) * 100.0
                                    percent = min(percent, 100.0)
                                    self.logger.info(f"[PROGRESS] {filepath.name}: {percent:.2f}%")
                                    if progress_callback:
                                        try:
                                            progress_callback(str(filepath), percent)
                                        except Exception:
                                            self.logger.debug("progress_callback raised", exc_info=True)
                                else:
                                    mb = downloaded / (1024 * 1024)
                                    self.logger.info(f"[PROGRESS] {filepath.name}: {mb:.2f} MB")
                                    if progress_callback:
                                        try:
                                            progress_callback(str(filepath), mb)
                                        except Exception:
                                            self.logger.debug("progress_callback raised", exc_info=True)

                # Move tmp to final atomically
                try:
                    os.replace(tmp_path, filepath)
                except Exception:
                    try:
                        shutil.move(str(tmp_path), str(filepath))
                    except Exception as e:
                        self.logger.exception("Failed to move temp file to final: %s", e)
                        return False

                if self.verify_download_integrity(filepath):
                    self.logger.info("[DONE] %s", filepath)
                    return True
                else:
                    self.logger.warning("Downloaded file failed integrity check: %s", filepath)
                    try:
                        filepath.unlink(missing_ok=True)
                    except Exception:
                        pass
                    # If integrity failed, maybe transient - retry
                    if attempt <= max_retries:
                        backoff = 2 ** attempt
                        time.sleep(backoff)
                        continue
                    return False

            except requests.exceptions.ReadTimeout as e:
                # read timed out: try to resume if possible
                self.logger.warning("Read timeout while downloading %s: attempt %d/%d", url, attempt, max_retries)
                if attempt <= max_retries:
                    backoff = 2 ** attempt
                    time.sleep(backoff)
                    continue
                else:
                    self.logger.exception("Exceeded read-timeout retries for %s", url)
                    try:
                        tmp_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return False

            except requests.exceptions.ConnectionError as e:
                self.logger.warning("Connection error downloading %s: attempt %d/%d - %s", url, attempt, max_retries, e)
                if attempt <= max_retries:
                    time.sleep(2 ** attempt)
                    continue
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return False

            except Exception as e:
                self.logger.exception("Error during requests download for %s: %s", url, e)
                # cleanup and retry if possible
                if attempt <= max_retries:
                    time.sleep(1 + attempt)
                    continue
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                return False

        # If we exit loop without returning success
        return False

    # ------------------------
    # Convenience wrappers (keep signatures)
    # ------------------------
    def download_video(self, video_url: str, video_file_path: Path) -> bool:
        self.logger.info(f"Starting video download: {video_file_path}")
        return self.download_file(video_url, video_file_path)

    def download_thumbnail(self, thumbnail_url: str, video_id: str, video_dir: Path) -> bool:
        if not thumbnail_url:
            return False

        ext = self.get_file_extension(thumbnail_url)
        thumbnail_path = Path(video_dir) / f"{video_id}{ext}"

        if self.download_file(thumbnail_url, thumbnail_path):
            validation_config = self.config.get("validation", {})
            min_thumb_size = int(validation_config.get("min_thumbnail_size_bytes", 100) or 100)
            try:
                if thumbnail_path.stat().st_size >= min_thumb_size:
                    self.logger.info(f"Thumbnail downloaded and validated: {thumbnail_path}")
                    return True
                else:
                    self.logger.warning(f"Thumbnail too small, removing: {thumbnail_path}")
                    thumbnail_path.unlink(missing_ok=True)
                    return False
            except Exception as e:
                self.logger.warning(f"Thumbnail validation error {thumbnail_path}: {e}")
                return False
        else:
            self.logger.warning(f"Failed to download thumbnail for {video_id}")
            return False

    def get_file_extension(self, url: str) -> str:
        parsed_url = urlparse(url)
        url_ext = os.path.splitext(parsed_url.path)[1].lower()
        if url_ext in ['.jpg', '.jpeg', '.png', '.webp']:
            return url_ext
        else:
            return '.jpg'

    def verify_download_integrity(self, filepath: Path) -> bool:
        try:
            if not filepath.exists():
                self.logger.error(f"File does not exist: {filepath}")
                return False

            file_size = os.path.getsize(filepath)
            validation_config = self.config.get("validation", {})

            if filepath.suffix.lower() == '.mp4':
                min_size = int(validation_config.get("min_video_size_bytes", 1024) or 1024)
            else:
                min_size = int(validation_config.get("min_thumbnail_size_bytes", 100) or 100)

            if file_size < min_size:
                self.logger.warning(f"File too small: {filepath} ({file_size} bytes, minimum {min_size})")
                return False

            if filepath.suffix.lower() == '.mp4':
                return self._verify_mp4_header_lenient(filepath)

            self.logger.info(f"File integrity verified: {filepath} ({file_size} bytes)")
            return True

        except Exception as e:
            self.logger.error(f"Error verifying download integrity {filepath}: {e}")
            return False

    def _verify_mp4_header_lenient(self, filepath: Path) -> bool:
        try:
            with open(filepath, 'rb') as f:
                header = f.read(32)
            if len(header) < 8:
                self.logger.warning(f"Video file too small to have valid header: {filepath}")
                return False
            video_indicators = [b'ftyp', b'mp4', b'MP4', b'\x00\x00\x00', b'mdat', b'moov']
            for indicator in video_indicators:
                if indicator in header:
                    self.logger.debug(f"Valid video indicator found in {filepath}: {indicator}")
                    return True
            file_size = os.path.getsize(filepath)
            if file_size > 10240:
                self.logger.info(f"Video file large enough to be valid: {filepath} ({file_size} bytes)")
                return True
            self.logger.warning(f"Video file may not be valid MP4: {filepath}")
            return False
        except Exception as e:
            self.logger.error(f"Error verifying MP4 header: {e}")
            return False

    def get_download_stats(self, filepath: Path) -> float:
        try:
            if filepath.exists():
                return filepath.stat().st_size / (1024 * 1024)
            return 0.0
        except Exception:
            return 0.0
