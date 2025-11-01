import asyncio
import httpx
import re
import os
import json
import time
import html
import gc
import logging
import aioshutil
import random
from urllib.parse import urlencode, urljoin
from selectolax.parser import HTMLParser
from datetime import datetime
from pathlib import Path
from io import BytesIO
from pyrogram import Client, filters
from pyrogram.types import Update, Message, InputMediaPhoto
from pyrogram.raw.functions.channels import CreateForumTopic
from pyrogram.errors import FloodWait
import aiosqlite
from fastapi import FastAPI
import uvicorn
import threading
from PIL import Image

# Health check app
app = FastAPI()

@app.get('/')
def root():
    return {"status": "OK"}

@app.get('/health')
def health():
    return {"status": "OK"}

def run_fastapi():
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv("PORT", 10000)))

# Disable FastAPI logs if needed, but for now keep
logging.getLogger('uvicorn').disabled = True  # optional

threading.Thread(target=run_fastapi, daemon=True).start()

# ───────────────────────────────
# ⚙️ PERFORMANCE CONFIG
# ───────────────────────────────
# HTTP & Download Configuration
MAX_CONCURRENT_WORKERS = 30      # Concurrent download workers - increased for speed
DELAY_BETWEEN_REQUESTS = 0.03    # Delay between requests (seconds) - optimized for speed
TIMEOUT = 12.0                   # HTTP request timeout (seconds) - increased for better success
MAX_DOWNLOAD_RETRIES = 4         # Download retry attempts per URL - increased
RETRY_DELAY = 0.8                # Delay between retries (seconds) - slightly increased

# Batch Processing Configuration
BATCH_SIZE = 40                  # URLs processed per download batch - increased for speed
CHUNK_SIZE = 60                  # URLs per processing chunk - increased for speed

# Telegram Sending Configuration
SEND_SEMAPHORE = asyncio.Semaphore(6)  # Concurrent sends - increased for speed
SEND_DELAY = 0.15                # Delay between sends (seconds) - optimized for speed
MEDIA_GROUP_SIZE = 10            # Images per media group (max 10 for Telegram)
MAX_SEND_RETRIES = 3             # Send retry attempts per batch

# Progress Update Configuration
PROGRESS_UPDATE_INTERVAL = 20    # Seconds between progress updates
PROGRESS_PERCENT_THRESHOLD = 10   # Minimum % change to trigger update

# Content Filtering Configuration
EXCLUDED_DOMAINS = ["pornbb.xyz"]
VALID_IMAGE_EXTS = ["jpg", "jpeg", "png", "webp", "bmp", "tiff", "svg", "ico", "avif", "jfif"]  # Removed GIF
EXCLUDED_MEDIA_EXTS = ["mp4", "avi", "mov", "webm", "mkv", "flv", "wmv", "gif"]  # Added GIF to excluded
MIN_IMAGE_SIZE = 100            # Minimum image size in bytes (reduced to accept tiny images)
MAX_IMAGE_SIZE = 20 * 1024 * 1024  # Maximum image size (20MB - Telegram's actual limit)

# Image Validation Configuration (More permissive for maximum compatibility)
MIN_IMAGE_WIDTH = 1             # Accept 1px minimum width
MIN_IMAGE_HEIGHT = 1            # Accept 1px minimum height  
MAX_IMAGE_WIDTH = 20000         # Increased max width
MAX_IMAGE_HEIGHT = 20000        # Increased max height
MAX_ASPECT_RATIO = 50           # More lenient aspect ratio (was 20)

# Memory Management Configuration
TEMP_DIR_NAME = "temp_images"    # Temporary directory for downloads
CONNECTION_POOL_SIZE = 80        # HTTP keepalive connections - increased for speed
MAX_CONNECTIONS = 300            # Maximum HTTP connections - increased for speed

# Database Configuration
DB_NAME = "bot_cache.db"         # SQLite database for caching and tracking
DB_CACHE_EXPIRY = 24 * 60 * 60   # Cache expiry in seconds (24 hours)
USE_DATABASE_TRACKING = True     # Enable database-based tracking for memory efficiency

# Access Control
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

API_ID = int(os.getenv("API_ID", 24536446))
API_HASH = os.getenv("API_HASH", "baee9dd189e1fd1daf0fb7239f7ae704")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7841933095:AAEz5SLNiGzWanheul1bwZL4HJbQBOBROqw")

bot = Client("image_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ───────────────────────────────
# 🧩 LOGGING SETUP  
# ───────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress Pyrogram connection logs and reduce logging overhead
logging.getLogger('pyrogram').setLevel(logging.ERROR)  # Changed from WARNING to ERROR
logging.getLogger('httpx').setLevel(logging.WARNING)  # Suppress HTTP logs

def log_memory():
    """Log detailed memory usage with garbage collection info"""
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        
        # Get detailed memory statistics
        rss_mb = memory_info.rss / 1024 / 1024  # Resident Set Size
        vms_mb = memory_info.vms / 1024 / 1024  # Virtual Memory Size
        
        # Get garbage collection statistics
        gc_stats = gc.get_stats()
        gc_counts = gc.get_count()
        
        logger.info(f"💾 Memory: RSS={rss_mb:.2f}MB, VMS={vms_mb:.2f}MB | GC: {gc_counts} | Objects: {len(gc.get_objects())}")
        
        # Log detailed GC stats for each generation
        for i, stats in enumerate(gc_stats):
            if stats['collections'] > 0:
                logger.debug(f"GC Gen{i}: {stats['collections']} collections, {stats['collected']} collected, {stats['uncollectable']} uncollectable")
                
    except ImportError:
        # Fallback to basic GC info without psutil
        gc_counts = gc.get_count()
        logger.info(f"💾 Memory tracking limited (no psutil) | GC: {gc_counts} | Objects: {len(gc.get_objects())}")

def force_garbage_collection():
    """Force aggressive garbage collection and log results"""
    try:
        # Get initial object count
        initial_objects = len(gc.get_objects())
        initial_counts = gc.get_count()
        
        # Force collection for all generations
        collected = 0
        for generation in range(3):
            collected += gc.collect(generation)
        
        # Get final counts
        final_objects = len(gc.get_objects())
        final_counts = gc.get_count()
        objects_freed = initial_objects - final_objects
        
        logger.info(f"🧹 GC: Collected {collected} objects, freed {objects_freed} references | {initial_counts} → {final_counts}")
        
        return collected, objects_freed
        
    except Exception as e:
        logger.warning(f"⚠️ Garbage collection error: {str(e)}")
        # Fallback to basic collection
        collected = gc.collect()
        logger.info(f"🧹 GC: Basic collection freed {collected} objects")
        return collected, 0

def generate_bar(percentage):
    filled = int(percentage / 10)
    empty = 10 - filled
    return "●" * filled + "○" * (empty // 2) + "◌" * (empty - empty // 2)

def validate_image_for_telegram(filepath):
    """Validate image dimensions and format for Telegram compatibility - PERMISSIVE MODE with enhanced checking"""
    try:
        with Image.open(filepath) as img:
            width, height = img.size
            file_size = os.path.getsize(filepath)
            
            logger.debug(f"🔍 Validating: {width}x{height}, {file_size} bytes, format: {img.format}")
            
            # Check file size limits
            if file_size < MIN_IMAGE_SIZE:
                logger.debug(f"Image too small: {file_size} bytes < {MIN_IMAGE_SIZE}")
                return False, "file_too_small"
            
            if file_size > MAX_IMAGE_SIZE:
                logger.debug(f"Image too large: {file_size} bytes > {MAX_IMAGE_SIZE}")
                return False, "file_too_large"
            
            # Very permissive dimension checks
            if width < MIN_IMAGE_WIDTH or height < MIN_IMAGE_HEIGHT:
                logger.debug(f"Image dimensions too small: {width}x{height}")
                return False, "dimensions_too_small"
            
            # Only check extreme cases that would definitely fail
            if width > MAX_IMAGE_WIDTH or height > MAX_IMAGE_HEIGHT:
                logger.debug(f"Image dimensions too large: {width}x{height}")
                return False, "dimensions_too_large"
            
            # More lenient aspect ratio check
            if min(width, height) > 0:  # Avoid division by zero
                aspect_ratio = max(width, height) / min(width, height)
                if aspect_ratio > MAX_ASPECT_RATIO:
                    logger.debug(f"Aspect ratio too extreme: {aspect_ratio}")
                    return False, "aspect_ratio_invalid"
            
            # Enhanced format checking with better compatibility detection
            if img.format:
                # Reject GIF format entirely - not supported as photos
                if img.format == 'GIF':
                    logger.debug(f"GIF format not supported for photos: {filepath}")
                    return False, "gif_not_supported"
                
                # Check for problematic formats that often cause PHOTO_SAVE_FILE_INVALID
                problematic_formats = ['WEBP', 'TIFF', 'BMP', 'ICO']
                if img.format in problematic_formats:
                    logger.debug(f"Potentially problematic format for Telegram: {img.format}")
                    return False, "format_needs_conversion"
                
                # Check for formats that need special handling
                if img.format in ['PNG'] and img.mode in ['RGBA', 'LA', 'P']:
                    logger.debug(f"PNG with transparency/palette mode: {img.mode}")
                    return False, "transparency_needs_conversion"
                
                # JPEG is generally safe, but check for CMYK or other exotic modes
                if img.format == 'JPEG' and img.mode not in ['RGB', 'L']:
                    logger.debug(f"JPEG with unsupported mode: {img.mode}")
                    return False, "color_mode_needs_conversion"
            
            # Check for corrupted or unusual image data
            try:
                img.verify()
            except Exception as e:
                logger.debug(f"Image verification failed: {str(e)}")
                return False, "image_corrupted"
            
            return True, "valid"
            
    except Exception as e:
        logger.debug(f"Image validation error for {filepath}: {str(e)}")
        # Don't reject on validation errors - but flag for conversion
        return False, "validation_error_needs_conversion"

def convert_image_for_telegram(filepath):
    """Convert/optimize image for better Telegram compatibility with multiple strategies"""
    try:
        with Image.open(filepath) as img:
            # Reject GIF format - don't try to convert
            if img.format == 'GIF':
                logger.warning(f"⚠️ GIF files cannot be sent as photos: {filepath}")
                return False
            
            # Get original info
            width, height = img.size
            file_size = os.path.getsize(filepath)
            
            logger.info(f"🔄 Converting image: {width}x{height}, {file_size} bytes, format: {img.format}")
            
            # Check if conversion is needed
            needs_conversion = False
            target_width, target_height = width, height
            
            # If dimensions are too large, scale down proportionally
            if width > MAX_IMAGE_WIDTH or height > MAX_IMAGE_HEIGHT:
                scale = min(MAX_IMAGE_WIDTH / width, MAX_IMAGE_HEIGHT / height)
                target_width = int(width * scale)
                target_height = int(height * scale)
                needs_conversion = True
                logger.info(f"📏 Resizing from {width}x{height} to {target_width}x{target_height}")
            
            # If file is too large, we'll compress it
            if file_size > MAX_IMAGE_SIZE:
                needs_conversion = True
                logger.info(f"📦 Compressing large file: {file_size} bytes")
            
            # Always convert to ensure Telegram compatibility
            needs_conversion = True
            
            # Convert with multiple strategies
            if needs_conversion:
                # Strategy 1: Try to preserve as much quality as possible
                success = _try_conversion_strategy(img, filepath, target_width, target_height, file_size, strategy=1)
                if success:
                    return True
                
                # Strategy 2: More aggressive compression
                logger.warning("🔄 First conversion failed, trying more aggressive compression")
                success = _try_conversion_strategy(img, filepath, target_width, target_height, file_size, strategy=2)
                if success:
                    return True
                
                # Strategy 3: Very aggressive - minimal quality but guaranteed compatibility
                logger.warning("🔄 Second conversion failed, trying maximum compression")
                success = _try_conversion_strategy(img, filepath, target_width, target_height, file_size, strategy=3)
                if success:
                    return True
            
            return False  # No conversion needed or all strategies failed
            
    except Exception as e:
        logger.error(f"❌ Image conversion failed for {filepath}: {str(e)}")
        return False

def _try_conversion_strategy(img, filepath, target_width, target_height, original_size, strategy=1):
    """Try different conversion strategies with increasing aggressiveness"""
    try:
        # Create a copy to work with
        working_img = img.copy()
        
        # Resize if needed
        if target_width != img.width or target_height != img.height:
            working_img = working_img.resize((target_width, target_height), Image.Resampling.LANCZOS)
        
        # Convert to RGB for maximum compatibility
        if working_img.mode in ('RGBA', 'LA', 'P', 'CMYK'):
            if working_img.mode == 'P':
                working_img = working_img.convert('RGBA')
            
            # Create white background
            background = Image.new('RGB', working_img.size, (255, 255, 255))
            if working_img.mode in ('RGBA', 'LA'):
                background.paste(working_img, mask=working_img.split()[-1])
            else:
                background.paste(working_img)
            working_img = background
        elif working_img.mode != 'RGB':
            working_img = working_img.convert('RGB')
        
        # Set quality based on strategy
        if strategy == 1:
            quality = 95 if original_size < MAX_IMAGE_SIZE else 85
            optimize = True
        elif strategy == 2:
            quality = 75
            optimize = True
            # Additional size reduction if still too large
            if original_size > MAX_IMAGE_SIZE // 2:
                new_width = int(target_width * 0.8)
                new_height = int(target_height * 0.8)
                working_img = working_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        else:  # strategy == 3
            quality = 60
            optimize = True
            # Aggressive size reduction
            new_width = min(int(target_width * 0.6), 1920)
            new_height = min(int(target_height * 0.6), 1080)
            working_img = working_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        # Save with specified quality
        working_img.save(filepath, 'JPEG', quality=quality, optimize=optimize, progressive=True)
        
        new_size = os.path.getsize(filepath)
        logger.info(f"✅ Strategy {strategy} successful: {original_size} → {new_size} bytes (Q{quality})")
        
        # Validate the result
        if new_size > MAX_IMAGE_SIZE:
            logger.warning(f"⚠️ File still too large after conversion: {new_size} bytes")
            if strategy < 3:
                return False  # Try next strategy
        
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ Conversion strategy {strategy} failed: {str(e)}")
        return False

# ───────────────────────────────
# 🗄️ DATABASE FUNCTIONS
# ───────────────────────────────
async def init_database():
    """Initialize SQLite database with required tables"""
    async with aiosqlite.connect(DB_NAME) as db:
        # URL tracking table for download/send status
        await db.execute('''
            CREATE TABLE IF NOT EXISTS url_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                username TEXT NOT NULL,
                download_status TEXT DEFAULT 'pending',
                send_status TEXT DEFAULT 'pending',
                file_path TEXT,
                file_size INTEGER,
                error_reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Media data cache table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS media_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url_hash TEXT UNIQUE NOT NULL,
                media_data TEXT NOT NULL,
                usernames TEXT NOT NULL,
                year_counts TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Processing sessions table
        await db.execute('''
            CREATE TABLE IF NOT EXISTS processing_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                total_urls INTEGER,
                downloaded INTEGER DEFAULT 0,
                sent INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                status TEXT DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        await db.execute('CREATE INDEX IF NOT EXISTS idx_url_tracking_url ON url_tracking(url)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_url_tracking_status ON url_tracking(download_status, send_status)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_media_cache_hash ON media_cache(url_hash)')
        await db.execute('CREATE INDEX IF NOT EXISTS idx_sessions_id ON processing_sessions(session_id)')
        
        await db.commit()
        logger.info("📁 Database initialized successfully")

async def cache_media_data(url, media_data, usernames, year_counts):
    """Cache extracted media data to avoid reprocessing"""
    import hashlib
    url_hash = hashlib.md5(url.encode()).hexdigest()
    
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT OR REPLACE INTO media_cache 
            (url_hash, media_data, usernames, year_counts, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            url_hash,
            json.dumps(media_data),
            json.dumps(usernames),
            json.dumps(year_counts) if year_counts else None
        ))
        await db.commit()

async def get_cached_media_data(url):
    """Retrieve cached media data if available and not expired"""
    import hashlib
    url_hash = hashlib.md5(url.encode()).hexdigest()
    
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('''
            SELECT media_data, usernames, year_counts, created_at 
            FROM media_cache 
            WHERE url_hash = ? AND 
                  created_at > datetime('now', '-{} seconds')
        '''.format(DB_CACHE_EXPIRY), (url_hash,))
        
        row = await cursor.fetchone()
        if row:
            media_data = json.loads(row[0])
            usernames = json.loads(row[1])
            year_counts = json.loads(row[2]) if row[2] else {}
            logger.info(f"📁 Using cached media data for: {url}")
            return media_data, usernames, year_counts
    
    return None, None, None

async def create_processing_session(session_id, total_urls):
    """Create a new processing session"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            INSERT OR REPLACE INTO processing_sessions 
            (session_id, total_urls, created_at, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ''', (session_id, total_urls))
        await db.commit()

async def update_session_progress(session_id, downloaded=None, sent=None, failed=None):
    """Update processing session progress"""
    updates = []
    params = []
    
    if downloaded is not None:
        updates.append("downloaded = ?")
        params.append(downloaded)
    if sent is not None:
        updates.append("sent = ?")
        params.append(sent)
    if failed is not None:
        updates.append("failed = ?")
        params.append(failed)
    
    if updates:
        updates.append("updated_at = CURRENT_TIMESTAMP")
        params.append(session_id)
        
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(f'''
                UPDATE processing_sessions 
                SET {", ".join(updates)}
                WHERE session_id = ?
            ''', params)
            await db.commit()

async def batch_insert_urls(urls_data, session_id):
    """Efficiently insert multiple URLs for tracking"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.executemany('''
            INSERT OR IGNORE INTO url_tracking 
            (url, username, download_status, created_at, updated_at)
            VALUES (?, ?, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ''', urls_data)
        await db.commit()

async def get_pending_urls(limit=None):
    """Get URLs that need to be downloaded"""
    async with aiosqlite.connect(DB_NAME) as db:
        query = '''
            SELECT id, url, username 
            FROM url_tracking 
            WHERE download_status = 'pending'
            ORDER BY created_at
        '''
        if limit:
            query += f' LIMIT {limit}'
            
        cursor = await db.execute(query)
        return await cursor.fetchall()

async def update_url_download_status(url, status, file_path=None, file_size=None, error_reason=None):
    """Update download status for a URL"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            UPDATE url_tracking 
            SET download_status = ?, file_path = ?, file_size = ?, 
                error_reason = ?, updated_at = CURRENT_TIMESTAMP
            WHERE url = ?
        ''', (status, file_path, file_size, error_reason, url))
        await db.commit()

async def update_url_send_status(url, status, error_reason=None):
    """Update send status for a URL"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            UPDATE url_tracking 
            SET send_status = ?, error_reason = ?, updated_at = CURRENT_TIMESTAMP
            WHERE url = ?
        ''', (status, error_reason, url))
        await db.commit()

async def get_downloaded_images_by_username(username, limit=None):
    """Get downloaded images for a specific username"""
    async with aiosqlite.connect(DB_NAME) as db:
        query = '''
            SELECT url, file_path, file_size 
            FROM url_tracking 
            WHERE username = ? AND download_status = 'completed' AND send_status = 'pending'
            ORDER BY updated_at
        '''
        if limit:
            query += f' LIMIT {limit}'
            
        cursor = await db.execute(query, (username,))
        rows = await cursor.fetchall()
        return [{'url': row[0], 'path': row[1], 'size': row[2]} for row in rows]

async def get_session_stats(session_id):
    """Get current session statistics"""
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute('''
            SELECT total_urls, downloaded, sent, failed 
            FROM processing_sessions 
            WHERE session_id = ?
        ''', (session_id,))
        return await cursor.fetchone()

async def cleanup_old_cache():
    """Clean up expired cache entries"""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''
            DELETE FROM media_cache 
            WHERE created_at < datetime('now', '-{} seconds')
        '''.format(DB_CACHE_EXPIRY * 2))  # Clean entries older than 2x expiry
        
        # Clean up old completed sessions (keep for 7 days)
        await db.execute('''
            DELETE FROM processing_sessions 
            WHERE status = 'completed' AND 
                  updated_at < datetime('now', '-7 days')
        ''')
        
        await db.commit()

# ───────────────────────────────
# 🧩 UTILITIES
# ───────────────────────────────
async def fetch_html(url: str):
    try:
        # Check cache first
        cached_data, cached_usernames, cached_year_counts = await get_cached_media_data(url)
        if cached_data:
            return None  # Will be handled by extract_media_data_from_html_cached
            
        async with httpx.AsyncClient() as client:
            r = await client.get(url, follow_redirects=True, timeout=TIMEOUT)
            return r.text if r.status_code == 200 else ""
    except Exception as e:
        logger.error(f"Fetch error for {url}: {e}")
        return ""

def extract_media_data_from_html(html_str: str, source_url: str = None):
    """Extract mediaData, usernames, yearCounts from HTML with caching"""
    try:
        # Check cache first if we have a source URL
        if source_url:
            cached_data, cached_usernames, cached_year_counts = None, None, None
            try:
                import asyncio
                cached_data, cached_usernames, cached_year_counts = asyncio.run(get_cached_media_data(source_url))
            except:
                pass
            
            if cached_data:
                logger.info(f"📁 Using cached data for {source_url}")
                return cached_data, cached_usernames, cached_year_counts

        tree = HTMLParser(html_str)
        script_tags = tree.css("script")
        media_data = {}
        usernames = []
        year_counts = {}

        for script in script_tags:
            script_text = script.text()
            if "const mediaData =" in script_text:
                # Extract mediaData JSON
                match = re.search(r'const mediaData = (\{.*?\});', script_text, re.DOTALL)
                if match:
                    media_data = json.loads(match.group(1))
            if "const usernames =" in script_text:
                # Extract usernames JSON
                match = re.search(r'const usernames = (\[.*?\]);', script_text, re.DOTALL)
                if match:
                    usernames = json.loads(match.group(1))
            if "const yearCounts =" in script_text:
                # Extract yearCounts JSON
                match = re.search(r'const yearCounts = (\{.*?\});', script_text, re.DOTALL)
                if match:
                    year_counts = json.loads(match.group(1))

        # Cache the results if we have a source URL
        if source_url and media_data:
            try:
                import asyncio
                asyncio.run(cache_media_data(source_url, media_data, usernames, year_counts))
            except:
                pass
        
        return media_data, usernames, year_counts
    except Exception as e:
        logger.error(f"Error extracting media data: {str(e)}")
        return {}, [], {}

def create_username_images(media_data, usernames):
    """Create username_images dict from mediaData"""
    username_images = {}
    for username in usernames:
        safe_username = username.replace(' ', '_')
        if safe_username in media_data:
            urls = [item['src'] for item in media_data[safe_username]]
            username_images[username] = urls
    return username_images

def filter_and_deduplicate_urls(username_images):
    """Filter URLs, exclude domains, remove duplicates, keep only images"""
    all_urls = []
    seen_urls = set()
    filtered_username_images = {}

    for username, urls in username_images.items():
        filtered_urls = []
        for url in urls:
            if not url or not url.startswith(('http://', 'https://')):
                continue
            # Exclude domains
            if any(domain in url.lower() for domain in EXCLUDED_DOMAINS):
                continue
            # Check if image extension
            url_lower = url.lower()
            has_image_ext = any(f".{ext}" in url_lower for ext in VALID_IMAGE_EXTS)
            is_excluded = any(f".{ext}" in url_lower for ext in EXCLUDED_MEDIA_EXTS)
            if not is_excluded and has_image_ext:
                if url not in seen_urls:
                    seen_urls.add(url)
                    all_urls.append(url)
                    filtered_urls.append(url)
        if filtered_urls:
            filtered_username_images[username] = filtered_urls

    return filtered_username_images, all_urls

async def download_batch(urls, temp_dir, base_timeout=TIMEOUT):
    """Download batch of URLs concurrently with connection pooling"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)
    
    # Use a single client for all downloads in this batch for better performance
    async with httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=CONNECTION_POOL_SIZE, max_connections=MAX_CONNECTIONS),
        timeout=httpx.Timeout(base_timeout)
    ) as client:
        async def download_with_client(url):
            return await download_image_with_client(url, temp_dir, semaphore, client)
        
        tasks = [download_with_client(url) for url in urls]
        results = await asyncio.gather(*tasks)
        
    successful = [r for r in results if r is not None]
    failed = [url for url, r in zip(urls, results) if r is None]
    return successful, failed

async def download_image_with_client(url, temp_dir, semaphore, client, max_retries=MAX_DOWNLOAD_RETRIES):
    """Download single image using provided client - memory efficient with detailed logging"""
    async with semaphore:
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"📥 Attempt {attempt}/{max_retries} downloading: {url}")
                
                # Use different timeout strategies for different attempts
                timeout_multiplier = 1 + (attempt - 1) * 0.5  # Increase timeout on retries
                current_timeout = TIMEOUT * timeout_multiplier
                
                # Create headers to mimic browser requests
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                
                r = await client.get(url, follow_redirects=True, timeout=current_timeout, headers=headers)
                
                logger.info(f"📊 HTTP {r.status_code} - Content-Length: {len(r.content)} bytes - {url}")
                
                if r.status_code == 200:
                    content = r.content
                    content_size = len(content)
                    
                    if content_size > MIN_IMAGE_SIZE:
                        # Detect GIF files by content (magic bytes) - exclude animated GIFs
                        if content[:3] == b'GIF':
                            logger.warning(f"⚠️ Skipping GIF file (not supported for photos): {url}")
                            await update_url_download_status(url, 'failed', error_reason="gif_file_not_supported")
                            return None
                        
                        # Use a more unique filename to avoid conflicts
                        timestamp = int(time.time() * 1000000)
                        filename = f"img_{timestamp}_{content_size}_{hash(url) % 10000}.jpg"
                        filepath = os.path.join(temp_dir, filename)
                        
                        # Write file efficiently
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        logger.info(f"✅ Downloaded successfully: {content_size} bytes → {filepath}")
                        
                        # Clear content from memory immediately
                        del content
                        
                        # Validate and convert image for Telegram compatibility
                        is_valid, reason = validate_image_for_telegram(filepath)
                        if not is_valid:
                            # Try to convert/fix the image
                            logger.warning(f"🔄 Converting invalid image ({reason}): {url}")
                            converted = convert_image_for_telegram(filepath)
                            if converted:
                                # Re-validate after conversion
                                is_valid, new_reason = validate_image_for_telegram(filepath)
                                if is_valid:
                                    new_size = os.path.getsize(filepath)
                                    logger.info(f"✅ Image conversion successful: {content_size} → {new_size} bytes")
                                    # Update database with successful download
                                    await update_url_download_status(url, 'completed', filepath, new_size)
                                else:
                                    logger.error(f"❌ Image conversion failed ({new_reason}): {url}")
                                    await update_url_download_status(url, 'failed', error_reason=new_reason)
                                    try:
                                        os.remove(filepath)
                                    except:
                                        pass
                                    return None
                            else:
                                logger.error(f"❌ Cannot convert image ({reason}): {url}")
                                await update_url_download_status(url, 'failed', error_reason=reason)
                                try:
                                    os.remove(filepath)
                                except:
                                    pass
                                return None
                        else:
                            # Update database with successful download
                            await update_url_download_status(url, 'completed', filepath, os.path.getsize(filepath))
                        
                        return {'url': url, 'path': filepath, 'size': os.path.getsize(filepath)}
                    else:
                        logger.warning(f"⚠️ Image too small ({content_size} bytes < {MIN_IMAGE_SIZE}): {url}")
                        await update_url_download_status(url, 'failed', error_reason=f"image_too_small_{content_size}_bytes")
                        return None
                elif r.status_code == 404:
                    logger.warning(f"❌ 404 Not Found: {url}")
                    await update_url_download_status(url, 'failed', error_reason="404_not_found")
                    return None
                elif r.status_code == 403:
                    logger.warning(f"❌ 403 Forbidden: {url}")
                    await update_url_download_status(url, 'failed', error_reason="403_forbidden")
                    return None
                elif r.status_code >= 500:
                    if attempt == max_retries:
                        logger.error(f"❌ Server Error {r.status_code} after {max_retries} attempts: {url}")
                        await update_url_download_status(url, 'failed', error_reason=f"server_error_{r.status_code}")
                        return None
                    else:
                        logger.warning(f"⚠️ Server Error {r.status_code} on attempt {attempt}, retrying: {url}")
                else:
                    if attempt == max_retries:
                        logger.error(f"❌ HTTP {r.status_code} after {max_retries} attempts: {url}")
                        await update_url_download_status(url, 'failed', error_reason=f"http_{r.status_code}")
                        return None
                    else:
                        logger.warning(f"⚠️ HTTP {r.status_code} on attempt {attempt}, retrying: {url}")
                        
            except asyncio.TimeoutError:
                if attempt == max_retries:
                    logger.error(f"❌ Timeout after {max_retries} attempts: {url}")
                    await update_url_download_status(url, 'failed', error_reason="timeout")
                    return None
                else:
                    logger.warning(f"⚠️ Timeout on attempt {attempt}, retrying with longer timeout: {url}")
            except Exception as e:
                error_msg = str(e).lower()
                if "connection" in error_msg or "network" in error_msg:
                    if attempt == max_retries:
                        logger.error(f"❌ Network error after {max_retries} attempts: {url} - {str(e)}")
                        await update_url_download_status(url, 'failed', error_reason=f"network_error")
                        return None
                    else:
                        logger.warning(f"⚠️ Network error on attempt {attempt}, retrying: {url}")
                else:
                    if attempt == max_retries:
                        logger.error(f"❌ Download failed after {max_retries} attempts: {url} - {str(e)}")
                        await update_url_download_status(url, 'failed', error_reason=str(e)[:200])
                        return None
                    else:
                        logger.warning(f"⚠️ Download attempt {attempt} failed: {url} - {str(e)}")
            
            if attempt < max_retries:
                logger.info(f"🔄 Retrying in {RETRY_DELAY}s: {url}")
                await asyncio.sleep(RETRY_DELAY)
        
        return None

async def send_image_batch_pyrogram(images, username, chat_id, topic_id=None, batch_num=1):
    """Send batch of images using Pyrogram - memory efficient version with better error handling"""
    if not images:
        return False

    # Filter out invalid images before sending - but try to convert first
    valid_images = []
    for img in images:
        if isinstance(img, dict) and 'path' in img and os.path.exists(img['path']):
            is_valid, reason = validate_image_for_telegram(img['path'])
            if is_valid:
                valid_images.append(img)
            else:
                # Try to convert the image
                logger.info(f"Attempting to convert image before sending ({reason}): {img['path']}")
                converted = convert_image_for_telegram(img['path'])
                if converted:
                    # Re-validate after conversion
                    is_valid_after, new_reason = validate_image_for_telegram(img['path'])
                    if is_valid_after:
                        logger.info(f"✅ Pre-send conversion successful: {img['path']}")
                        # Update size after conversion
                        img['size'] = os.path.getsize(img['path'])
                        valid_images.append(img)
                    else:
                        logger.warning(f"❌ Pre-send conversion failed ({new_reason}): {img['path']}")
                        try:
                            os.remove(img['path'])
                        except:
                            pass
                else:
                    logger.warning(f"❌ Cannot convert image before sending ({reason}): {img['path']}")
                    try:
                        os.remove(img['path'])
                    except:
                        pass

    if not valid_images:
        logger.warning(f"No valid images found for {username} batch {batch_num}")
        return False

    # Split into chunks using configurable size
    chunks = [valid_images[i:i + MEDIA_GROUP_SIZE] for i in range(0, len(valid_images), MEDIA_GROUP_SIZE)]
    
    successful_chunks = 0
    total_chunks = len(chunks)

    for idx, chunk in enumerate(chunks):
        async with SEND_SEMAPHORE:
            await asyncio.sleep(SEND_DELAY)  # Use configured send delay
            
            try:
                media = []
                current_batch_num = batch_num + idx
                
                # Create media group - double check each image
                for i, img in enumerate(chunk):
                    try:
                        # Final validation before adding to media group - very permissive
                        if not os.path.exists(img['path']):
                            logger.warning(f"Image file not found: {img['path']}")
                            continue
                            
                        # Try one more conversion attempt if needed
                        is_valid, reason = validate_image_for_telegram(img['path'])
                        if not is_valid:
                            logger.warning(f"🔄 Final conversion attempt ({reason}): {img['path']}")
                            converted = convert_image_for_telegram(img['path'])
                            if not converted:
                                logger.warning(f"❌ Final validation failed ({reason}): {img['path']}")
                                continue
                            
                        if i == 0:
                            media.append(InputMediaPhoto(img['path'], caption=f"{username.replace('_', ' ')} - B{current_batch_num}"))
                        else:
                            media.append(InputMediaPhoto(img['path']))
                    except Exception as e:
                        logger.warning(f"Error creating media for {img['path']}: {str(e)}")
                        continue

                if not media:
                    logger.warning(f"No valid media created for {username} chunk {idx}")
                    continue

                # Send with improved retry mechanism
                max_send_retries = 3
                retry_delay = 1
                
                for attempt in range(max_send_retries):
                    try:
                        if topic_id:
                            await bot.send_media_group(chat_id, media, reply_to_message_id=topic_id)
                        else:
                            await bot.send_media_group(chat_id, media)
                        successful_chunks += 1
                        logger.debug(f"✅ Sent chunk {idx+1}/{total_chunks} for {username}")
                        break
                        
                    except FloodWait as e:
                        flood_wait_time = min(e.value, 120)  # Cap at 2 minutes
                        logger.info(f"🕐 FloodWait {flood_wait_time}s for {username} chunk {idx}")
                        await asyncio.sleep(flood_wait_time)
                        if attempt == max_send_retries - 1:
                            logger.warning(f"❌ FloodWait retry exhausted for {username} chunk {idx}")
                            # Don't return False, continue with next chunk
                            break
                            
                    except Exception as e:
                        error_msg = str(e).lower()
                        if "photo_invalid_dimensions" in error_msg or "photo_save_file_invalid" in error_msg:
                            logger.warning(f"Telegram rejected images in {username} chunk {idx}: {str(e)}")
                            # Try to convert and retry once more
                            logger.info(f"Attempting final conversion for rejected images: {username} chunk {idx}")
                            retry_media = []
                            for media_item in media:
                                try:
                                    # Get the file path from the media item
                                    file_path = media_item.media
                                    if os.path.exists(file_path):
                                        converted = convert_image_for_telegram(file_path)
                                        if converted:
                                            # Recreate media item with converted image
                                            if len(retry_media) == 0:
                                                retry_media.append(InputMediaPhoto(file_path, caption=media_item.caption))
                                            else:
                                                retry_media.append(InputMediaPhoto(file_path))
                                except Exception as conv_e:
                                    logger.debug(f"Conversion retry failed: {str(conv_e)}")
                            
                            if retry_media:
                                try:
                                    if topic_id:
                                        await bot.send_media_group(chat_id, retry_media, reply_to_message_id=topic_id)
                                    else:
                                        await bot.send_media_group(chat_id, retry_media)
                                    successful_chunks += 1
                                    logger.info(f"✅ Retry successful after conversion for {username} chunk {idx}")
                                    break
                                except Exception as retry_e:
                                    logger.warning(f"Retry after conversion failed: {str(retry_e)}")
                            
                            # Skip this chunk if conversion retry also failed
                            break
                        else:
                            logger.warning(f"Send attempt {attempt+1} failed for {username} chunk {idx}: {str(e)}")
                            if attempt == max_send_retries - 1:
                                logger.error(f"❌ All send attempts failed for {username} chunk {idx}")
                                break
                            await asyncio.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff

            except Exception as e:
                logger.error(f"❌ Critical error sending {username} chunk {idx}: {str(e)}")
                continue  # Continue with next chunk instead of failing entirely

    # Return True if at least some chunks were successful
    success_rate = successful_chunks / total_chunks if total_chunks > 0 else 0
    
    # Log memory usage before cleanup
    logger.info(f"📊 {username}: {successful_chunks}/{total_chunks} chunks sent successfully ({success_rate:.1%})")
    log_memory()
    
    # Force garbage collection after sending to free up memory
    collected, objects_freed = force_garbage_collection()
    
    # Log memory usage after cleanup
    log_memory()
    logger.info(f"🧹 Post-send cleanup: {collected} objects collected, {objects_freed} references freed")
    
    return success_rate > 0

def cleanup_images(images):
    """Remove temp image files with error handling and memory cleanup"""
    if not images:
        return
    
    cleaned_files = 0
    for img in images:
        try:
            if isinstance(img, dict) and 'path' in img and os.path.exists(img['path']):
                os.remove(img['path'])
                cleaned_files += 1
            elif isinstance(img, str) and os.path.exists(img):
                os.remove(img)
                cleaned_files += 1
        except Exception as e:
            logger.debug(f"Cleanup error for {img}: {str(e)}")
    
    # Clear the list to free memory references
    if isinstance(images, list):
        images.clear()
    
    # Force garbage collection after cleanup
    if cleaned_files > 0:
        logger.debug(f"🧹 Cleaned {cleaned_files} image files, forcing GC")
        collected = gc.collect()
        logger.debug(f"🧹 GC collected {collected} objects after file cleanup")

async def process_batches(username_images, chat_id, topic_id=None, user_topic_ids=None, progress_msg=None):
    """
    NEW LOGIC: Process URLs in batches of 10 with retry mechanism
    - Process 10 URLs at a time with retries
    - Failed URLs go to retry queue (max 2 attempts per URL total)
    - Send images in groups of 10 when accumulated >= 10
    - Clear memory after each send
    - Process one username completely before moving to next
    """
    import uuid
    session_id = str(uuid.uuid4())
    
    # Initialize database
    await init_database()
    await cleanup_old_cache()
    
    temp_dir = "temp_images"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Track progress
    total_downloaded = 0
    total_sent = 0
    total_failed_permanently = 0
    last_edit = [0]
    batch_num = 1
    
    # Log initial memory state
    log_memory()
    logger.info(f"🚀 Starting NEW batch processing logic for {len(username_images)} users")
    
    # Process each username one by one
    for user_idx, (username, urls) in enumerate(username_images.items(), 1):
        logger.info(f"\n{'='*60}")
        logger.info(f"👤 Processing User {user_idx}/{len(username_images)}: {username}")
        logger.info(f"📊 Total URLs for this user: {len(urls)}")
        logger.info(f"{'='*60}\n")
        
        # URLs for this user
        pending_urls = list(urls)  # Main queue
        failed_urls = []  # Failed URLs queue (for retry)
        retry_count = {}  # Track retry attempts per URL
        
        # Success image accumulator for this user
        success_images = []
        
        # Get topic for this user
        user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
        
        # Phase 1: Process all pending URLs in batches of 10
        round_num = 1
        while pending_urls or failed_urls:
            
            # Determine which queue to process
            if pending_urls:
                current_queue = pending_urls
                queue_name = "PENDING"
            elif failed_urls:
                current_queue = failed_urls
                queue_name = "RETRY"
                failed_urls = []  # Clear retry queue, failed ones will be added back
            else:
                break
            
            logger.info(f"\n🔄 Round {round_num} - Processing {queue_name} queue")
            logger.info(f"📝 Queue size: {len(current_queue)} URLs")
            
            # Process in batches of 10
            while current_queue:
                # Take first 10 URLs
                batch_urls = current_queue[:10]
                current_queue = current_queue[10:]
                
                logger.info(f"\n📦 Batch {batch_num} - Processing {len(batch_urls)} URLs from {queue_name}")
                
                # Download batch with retries
                successful_downloads, failed_downloads = await download_batch(batch_urls, temp_dir)
                
                success_count = len(successful_downloads)
                failed_count = len(failed_downloads)
                
                logger.info(f"✅ Success: {success_count}/{len(batch_urls)}")
                logger.info(f"❌ Failed: {failed_count}/{len(batch_urls)}")
                
                # Add successful downloads to accumulator
                success_images.extend(successful_downloads)
                total_downloaded += success_count
                
                # Handle failed URLs - check retry count
                for failed_url in failed_downloads:
                    if failed_url not in retry_count:
                        retry_count[failed_url] = 0
                    
                    retry_count[failed_url] += 1
                    
                    if retry_count[failed_url] < 2:  # Max 2 attempts (1 original + 1 retry)
                        logger.info(f"🔄 URL will be retried (attempt {retry_count[failed_url]}/2): {failed_url}")
                        failed_urls.append(failed_url)
                    else:
                        logger.warning(f"❌ URL permanently failed after 2 attempts: {failed_url}")
                        total_failed_permanently += 1
                
                # Update progress
                now = time.time()
                if progress_msg and (now - last_edit[0] > 5):
                    # Calculate progress percentage
                    total_urls_user = len(urls)
                    processed_urls = len(urls) - len(pending_urls) - len(failed_urls)
                    progress_percent = int((processed_urls / total_urls_user) * 100) if total_urls_user > 0 else 0
                    
                    # Generate progress bar
                    bar = generate_bar(progress_percent)
                    
                    progress = f"""👤 User: {username} ({user_idx}/{len(username_images)})
{bar} {progress_percent}%
📦 Batch: {batch_num} | Round: {round_num}
📥 Downloaded: {total_downloaded}
📤 Sent: {total_sent}
💾 Pending Send: {len(success_images)}
❌ Failed: {total_failed_permanently}
🔄 Retry Queue: {len(failed_urls)}"""
                    try:
                        await progress_msg.edit(progress)
                        last_edit[0] = now
                    except FloodWait as e:
                        # Handle Telegram flood wait
                        logger.warning(f"⚠️ Progress update FloodWait: {e.value}s")
                        last_edit[0] = now + e.value  # Skip updates for flood wait duration
                    except Exception as e:
                        # Ignore other errors (like message not modified)
                        logger.debug(f"Progress update skipped: {str(e)}")
                        pass
                
                # Send images if we have 10 or more
                while len(success_images) >= 10:
                    send_batch = success_images[:10]
                    success_images = success_images[10:]
                    
                    logger.info(f"\n📤 Sending group of 10 images for {username}")
                    
                    try:
                        success = await send_image_batch_pyrogram(send_batch, username, chat_id, user_topic, batch_num)
                        if success:
                            total_sent += 10
                            logger.info(f"✅ Successfully sent 10 images | Total sent: {total_sent}")
                        else:
                            logger.warning(f"⚠️ Failed to send batch")
                    except Exception as e:
                        logger.error(f"❌ Error sending batch: {str(e)}")
                    
                    # Clean up sent images and force GC
                    cleanup_images(send_batch)
                    collected, objects_freed = force_garbage_collection()
                    logger.info(f"🧹 Memory cleanup: {collected} objects collected, {objects_freed} freed")
                    log_memory()
                    
                    await asyncio.sleep(SEND_DELAY)
                
                batch_num += 1
                
                # Batch cleanup
                logger.info(f"🧹 Batch {batch_num-1} complete, cleaning memory...")
                collected, objects_freed = force_garbage_collection()
                log_memory()
            
            round_num += 1
        
        # Send remaining images for this user (less than 10)
        if success_images:
            logger.info(f"\n📤 Sending final {len(success_images)} images for {username}")
            
            try:
                success = await send_image_batch_pyrogram(success_images, username, chat_id, user_topic, batch_num)
                if success:
                    total_sent += len(success_images)
                    logger.info(f"✅ Sent final batch | Total sent: {total_sent}")
            except Exception as e:
                logger.error(f"❌ Error sending final batch: {str(e)}")
            
            # Cleanup
            cleanup_images(success_images)
            success_images.clear()
            collected, objects_freed = force_garbage_collection()
            logger.info(f"🧹 Final user cleanup: {collected} objects collected, {objects_freed} freed")
            log_memory()
        
        logger.info(f"\n✅ Completed user: {username}")
        logger.info(f"{'='*60}\n")
    
    # Final cleanup
    try:
        await aioshutil.rmtree(temp_dir)
        logger.info(f"🗂️ Removed temporary directory: {temp_dir}")
    except:
        pass
    
    # Final memory cleanup
    collected, objects_freed = force_garbage_collection()
    log_memory()
    logger.info(f"🏁 Final cleanup: {collected} objects collected, {objects_freed} references freed")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"📊 FINAL STATISTICS:")
    logger.info(f"✅ Total Downloaded: {total_downloaded}")
    logger.info(f"📤 Total Sent: {total_sent}")
    logger.info(f"❌ Total Failed Permanently: {total_failed_permanently}")
    logger.info(f"{'='*60}\n")
    
    return total_downloaded, total_sent, total_downloaded + total_failed_permanently

# ───────────────────────────────
# ✅ FIXED TOPIC CREATION FUNCTION
# ───────────────────────────────
async def create_forum_topic(client: Client, chat_id: int, topic_name: str):
    """Create a forum topic and return its ID"""
    try:
        # Verify bot can access the chat
        try:
            chat = await client.get_chat(chat_id)
            logger.info(f"📣 Connected to chat: {chat.title}")
        except Exception:
            logger.info("ℹ️ Chat not found in cache. Sending handshake message...")
            await client.send_message(chat_id, "👋 Bot connected successfully!")
            chat = await client.get_chat(chat_id)
        
        # Create the forum topic
        peer = await client.resolve_peer(chat_id)
        random_id = random.randint(100000, 999999999)
        
        result = await client.invoke(
            CreateForumTopic(
                channel=peer,
                title=topic_name,
                random_id=random_id,
                icon_color=0xFFD700  # optional: gold color
            )
        )
        
        # Extract topic_id
        topic_id = None
        for update in result.updates:
            if hasattr(update, "message") and hasattr(update.message, "id"):
                topic_id = update.message.id
                break
        
        if not topic_id:
            logger.error("⚠️ Could not detect topic_id. Check permissions.")
            return None
        
        logger.info(f"🆕 Topic created: {topic_name} (ID: {topic_id})")
        return topic_id
        
    except Exception as e:
        logger.error(f"❌ Error creating topic '{topic_name}': {str(e)}")
        return None

# ───────────────────────────────
# 🔍 HELPER: GET CHAT ID
# ───────────────────────────────
@bot.on_message(filters.command("getid"))
async def get_chat_id(client: Client, message: Message):
    """Get the chat ID of current chat or forwarded message"""
    if message.forward_from_chat:
        chat = message.forward_from_chat
        await message.reply(
            f"**Forwarded Chat Info:**\n"
            f"• Title: {chat.title}\n"
            f"• ID: `{chat.id}`\n"
            f"• Type: {chat.type}\n"
            f"• Is Forum: {getattr(chat, 'is_forum', False)}"
        )
    else:
        chat = message.chat
        await message.reply(
            f"**Current Chat Info:**\n"
            f"• Title: {getattr(chat, 'title', 'Private Chat')}\n"
            f"• ID: `{chat.id}`\n"
            f"• Type: {chat.type}\n"
            f"• Is Forum: {getattr(chat, 'is_forum', False)}"
        )

# ───────────────────────────────
# BOT HANDLER
# ───────────────────────────────
@bot.on_message(filters.command("down") & filters.private)
async def handle_down(client: Client, message: Message):
    if message.chat.id not in ALLOWED_CHAT_IDS:
        return  # Silently ignore if not allowed

    text = message.text.strip()
    args = text.split()[1:] if len(text.split()) > 1 else []

    # Parse arguments
    url = None
    target_chat_id = message.chat.id
    target_topic_id = None
    create_topic_name = None
    create_topics_per_user = False

    i = 0
    while i < len(args):
        if args[i] == '-g' and i + 1 < len(args):
            target_chat_id = int(args[i + 1])
            i += 2
        elif args[i] == '-t' and i + 1 < len(args):
            target_topic_id = int(args[i + 1])
            i += 2
        elif args[i] == '-ct' and i + 1 < len(args):
            create_topic_name = args[i + 1]
            i += 2
        elif args[i] == '-u':
            create_topics_per_user = True
            i += 1
        else:
            if not url:
                url = args[i]
            i += 1

    # Get HTML content
    html_content = ""
    if message.reply_to_message:
        if message.reply_to_message.document:
            # Download document
            file_path = await message.reply_to_message.download()
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            os.remove(file_path)
        elif message.reply_to_message.text:
            # Assume URLs in text
            urls = re.findall(r'https?://[^\s]+', message.reply_to_message.text)
            if urls:
                # Fetch first URL as HTML
                html_content = await fetch_html(urls[0])
    elif url:
        html_content = await fetch_html(url)

    if not html_content:
        await message.reply("No valid HTML content found.")
        return

    # Extract data
    media_data, usernames, year_counts = extract_media_data_from_html(html_content, url if url else urls[0] if 'urls' in locals() and urls else None)
    if not media_data:
        await message.reply("Failed to extract media data from HTML.")
        return

    username_images = create_username_images(media_data, usernames)
    username_images, all_urls = filter_and_deduplicate_urls(username_images)

    total_media = sum(len(urls) for urls in username_images.values())
    total_images = len(all_urls)

    if total_images == 0:
        await message.reply("No valid images found.")
        return

    # Handle topic creation with improved logic
    user_topic_ids = {}
    if create_topics_per_user:
        logger.info(f"Creating {len(username_images)} topics for users...")
        for username in username_images.keys():
            topic_name = f"{username.replace('_', ' ')}"
            topic_id = await create_forum_topic(client, target_chat_id, topic_name)
            user_topic_ids[username] = topic_id
            await asyncio.sleep(0.5)  # Small delay between topic creations
    elif create_topic_name:
        logger.info(f"Creating single topic: {create_topic_name}")
        target_topic_id = await create_forum_topic(client, target_chat_id, create_topic_name)
        if not target_topic_id:
            await message.reply(f"Failed to create topic '{create_topic_name}'. Check bot permissions.")
            return

    # Send initial progress
    progress_msg = await message.reply("Starting download process...")

    # Process batches
    total_downloaded, total_sent, total_filtered = await process_batches(
        username_images, target_chat_id, target_topic_id, user_topic_ids, progress_msg
    )

    # Final stats with detailed breakdown
    stats = f"""✅ Download Complete!

📊 Statistics:
• Total Media Items: {total_media}
• Total Unique URLs: {total_filtered}
• Successfully Downloaded: {total_downloaded}
• Successfully Sent: {total_sent}
• Download Success Rate: {(total_downloaded/total_filtered)*100:.1f}%
• Send Success Rate: {(total_sent/total_downloaded)*100:.1f}% (of downloaded)

🎯 Process completed efficiently with optimized memory usage!"""

    await progress_msg.edit(stats)

# ───────────────────────────────
# MAIN
# ───────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=run_fastapi, daemon=True).start()
    bot.run()
