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
MAX_CONCURRENT_WORKERS = 20      # Concurrent download workers
DELAY_BETWEEN_REQUESTS = 0.05    # Delay between requests (seconds) - optimized
TIMEOUT = 8.0                    # HTTP request timeout (seconds)
MAX_DOWNLOAD_RETRIES = 2         # Download retry attempts per URL
RETRY_DELAY = 0.5                # Delay between retries (seconds)

# Batch Processing Configuration
BATCH_SIZE = 30                  # URLs processed per download batch
CHUNK_SIZE = 50                  # URLs per processing chunk - memory management

# Telegram Sending Configuration
SEND_SEMAPHORE = asyncio.Semaphore(4)  # Concurrent sends - increased for speed
SEND_DELAY = 0.2                 # Delay between sends (seconds) - optimized
MEDIA_GROUP_SIZE = 10            # Images per media group (max 10 for Telegram)
MAX_SEND_RETRIES = 3             # Send retry attempts per batch

# Progress Update Configuration
PROGRESS_UPDATE_INTERVAL = 20    # Seconds between progress updates
PROGRESS_PERCENT_THRESHOLD = 10   # Minimum % change to trigger update

# Content Filtering Configuration
EXCLUDED_DOMAINS = ["pornbb.xyz"]
VALID_IMAGE_EXTS = ["jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "svg", "ico", "avif", "jfif"]
EXCLUDED_MEDIA_EXTS = ["mp4", "avi", "mov", "webm", "mkv", "flv", "wmv"]
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
CONNECTION_POOL_SIZE = 50        # HTTP keepalive connections
MAX_CONNECTIONS = 200            # Maximum HTTP connections

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
    try:
        import psutil
        mem = psutil.Process().memory_info().rss / 1024 / 1024
        logger.info(f"Memory usage: {mem:.2f} MB")
    except ImportError:
        logger.info("psutil not available for memory tracking")

def generate_bar(percentage):
    filled = int(percentage / 10)
    empty = 10 - filled
    return "●" * filled + "○" * (empty // 2) + "◌" * (empty - empty // 2)

def validate_image_for_telegram(filepath):
    """Validate image dimensions and format for Telegram compatibility - PERMISSIVE MODE"""
    try:
        with Image.open(filepath) as img:
            width, height = img.size
            file_size = os.path.getsize(filepath)
            
            # Only check absolute minimum requirements for Telegram
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
            
            # Accept most common image formats (don't be too strict)
            supported_formats = ['JPEG', 'PNG', 'GIF', 'WEBP', 'BMP', 'TIFF', 'ICO']
            if img.format and img.format not in supported_formats:
                logger.debug(f"Potentially unsupported format: {img.format} - trying anyway")
                # Don't reject, just log - let Telegram decide
            
            return True, "valid"
            
    except Exception as e:
        logger.debug(f"Image validation error for {filepath}: {str(e)}")
        # Don't reject on validation errors - try to send anyway
        return True, "validation_error_accepted"

def convert_image_for_telegram(filepath):
    """Convert/optimize image for better Telegram compatibility"""
    try:
        with Image.open(filepath) as img:
            # Get original info
            width, height = img.size
            file_size = os.path.getsize(filepath)
            
            # Check if conversion is needed
            needs_conversion = False
            target_width, target_height = width, height
            
            # If dimensions are too large, scale down proportionally
            if width > MAX_IMAGE_WIDTH or height > MAX_IMAGE_HEIGHT:
                scale = min(MAX_IMAGE_WIDTH / width, MAX_IMAGE_HEIGHT / height)
                target_width = int(width * scale)
                target_height = int(height * scale)
                needs_conversion = True
                logger.info(f"Resizing image from {width}x{height} to {target_width}x{target_height}")
            
            # If file is too large, we'll compress it
            if file_size > MAX_IMAGE_SIZE:
                needs_conversion = True
                logger.info(f"Compressing large image: {file_size} bytes")
            
            # Convert if needed
            if needs_conversion:
                # Create new image
                if target_width != width or target_height != height:
                    img = img.resize((target_width, target_height), Image.Resampling.LANCZOS)
                
                # Convert to RGB if needed (for JPEG compatibility)
                if img.mode in ('RGBA', 'LA', 'P'):
                    background = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = background
                
                # Save with compression
                quality = 85 if file_size > MAX_IMAGE_SIZE else 95
                img.save(filepath, 'JPEG', quality=quality, optimize=True)
                
                new_size = os.path.getsize(filepath)
                logger.info(f"Converted image: {file_size} -> {new_size} bytes")
                
                return True
            
            return False  # No conversion needed
            
    except Exception as e:
        logger.warning(f"Image conversion failed for {filepath}: {str(e)}")
        return False

# ───────────────────────────────
# 🧩 UTILITIES
# ───────────────────────────────
async def fetch_html(url: str):
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(url, follow_redirects=True, timeout=TIMEOUT)
            return r.text if r.status_code == 200 else ""
    except Exception as e:
        logger.error(f"Fetch error for {url}: {e}")
        return ""

def extract_media_data_from_html(html_str: str):
    """Extract mediaData, usernames, yearCounts from HTML"""
    try:
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
    """Download single image using provided client - memory efficient"""
    async with semaphore:
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
        
        for attempt in range(1, max_retries + 1):
            try:
                r = await client.get(url, follow_redirects=True)
                if r.status_code == 200:
                    content = r.content
                    if len(content) > MIN_IMAGE_SIZE:
                        # Use a more unique filename to avoid conflicts
                        timestamp = int(time.time() * 1000000)
                        size = len(content)
                        filename = f"img_{timestamp}_{size}_{hash(url) % 10000}.jpg"
                        filepath = os.path.join(temp_dir, filename)
                        
                        # Write file efficiently
                        with open(filepath, 'wb') as f:
                            f.write(content)
                        
                        # Clear content from memory immediately
                        del content
                        
                        # Validate and convert image for Telegram compatibility
                        is_valid, reason = validate_image_for_telegram(filepath)
                        if not is_valid and reason not in ["validation_error_accepted"]:
                            # Try to convert/fix the image
                            logger.info(f"Attempting to convert invalid image ({reason}): {url}")
                            converted = convert_image_for_telegram(filepath)
                            if converted:
                                # Re-validate after conversion
                                is_valid, new_reason = validate_image_for_telegram(filepath)
                                if is_valid:
                                    logger.info(f"✅ Image conversion successful: {url}")
                                else:
                                    logger.warning(f"❌ Image conversion failed ({new_reason}): {url}")
                                    try:
                                        os.remove(filepath)
                                    except:
                                        pass
                                    return None
                            else:
                                logger.warning(f"❌ Cannot convert image ({reason}): {url}")
                                try:
                                    os.remove(filepath)
                                except:
                                    pass
                                return None
                        
                        return {'url': url, 'path': filepath, 'size': os.path.getsize(filepath)}
                    else:
                        logger.debug(f"Image too small ({len(content)} bytes): {url}")
                        return None
                elif r.status_code == 404:
                    logger.debug(f"404 Not Found: {url}")
                    return None
                else:
                    if attempt == max_retries:
                        logger.debug(f"HTTP {r.status_code} for {url}")
            except Exception as e:
                if attempt == max_retries:
                    logger.debug(f"Download failed for {url}: {str(e)}")
                else:
                    logger.debug(f"Download attempt {attempt} failed for {url}")
            
            if attempt < max_retries:
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
                        if not is_valid and reason not in ["validation_error_accepted"]:
                            logger.info(f"Final conversion attempt ({reason}): {img['path']}")
                            converted = convert_image_for_telegram(img['path'])
                            if not converted:
                                logger.warning(f"Final validation failed ({reason}): {img['path']}")
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
    logger.info(f"📊 {username}: {successful_chunks}/{total_chunks} chunks sent successfully ({success_rate:.1%})")
    return success_rate > 0

def cleanup_images(images):
    """Remove temp image files with error handling"""
    if not images:
        return
    
    for img in images:
        try:
            if isinstance(img, dict) and 'path' in img and os.path.exists(img['path']):
                os.remove(img['path'])
            elif isinstance(img, str) and os.path.exists(img):
                os.remove(img)
        except Exception as e:
            logger.debug(f"Cleanup error for {img}: {str(e)}")
    
    # Clear the list to free memory references
    if isinstance(images, list):
        images.clear()

async def process_batches(username_images, chat_id, topic_id=None, user_topic_ids=None, progress_msg=None):
    """Process all URLs with better memory management and no image loss"""
    total_images = sum(len(urls) for urls in username_images.values())
    total_downloaded = 0
    total_sent = 0

    temp_dir = "temp_images"
    os.makedirs(temp_dir, exist_ok=True)

    # Collect all unique URLs with tracking
    all_urls = []
    url_to_username = {}  # Track which username each URL belongs to
    for username, urls in username_images.items():
        for url in urls:
            if url not in url_to_username:  # Avoid duplicates
                all_urls.append(url)
                url_to_username[url] = username

    downloaded_urls = set()
    failed_urls = []
    successfully_sent_urls = set()
    
    last_edit = [0]
    last_progress_percent = [0]
    batch_num = 1

    logger.info(f"📊 Starting download of {len(all_urls)} unique URLs from {len(username_images)} users")

    # Process URLs in manageable chunks
    url_index = 0
    
    while url_index < len(all_urls) or failed_urls:
        # Get current chunk of URLs to process
        current_urls = []
        if url_index < len(all_urls):
            take = min(CHUNK_SIZE, len(all_urls) - url_index)
            current_urls = all_urls[url_index:url_index + take]
            url_index += take
        elif failed_urls:
            # Process failed URLs in smaller batches
            take = min(20, len(failed_urls))
            current_urls = failed_urls[:take]
            failed_urls = failed_urls[take:]

        if not current_urls:
            break

        # Download current chunk
        logger.info(f"📥 Downloading batch {batch_num}: {len(current_urls)} URLs")
        successful_downloads, new_failures = await download_batch(current_urls, temp_dir)
        
        # Track what failed for retry
        failed_urls.extend(new_failures)
        
        # Group successful downloads by username
        username_groups = {}
        for download in successful_downloads:
            username = url_to_username.get(download['url'], 'unknown')
            if username not in username_groups:
                username_groups[username] = []
            username_groups[username].append(download)
            downloaded_urls.add(download['url'])

        # Send images immediately after downloading to free memory
        send_success_count = 0
        for username, images in username_groups.items():
            if images:
                # Split large groups into batches using configurable size
                for i in range(0, len(images), MEDIA_GROUP_SIZE):
                    batch_images = images[i:i + MEDIA_GROUP_SIZE]
                    user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
                    
                    try:
                        success = await send_image_batch_pyrogram(batch_images, username, chat_id, user_topic, batch_num)
                        if success:
                            send_success_count += len(batch_images)
                            successfully_sent_urls.update(img['url'] for img in batch_images)
                            logger.info(f"✅ Sent {len(batch_images)} images for {username}")
                        else:
                            logger.warning(f"⚠️ Partial/failed send for {username} - continuing process")
                    except Exception as e:
                        logger.error(f"❌ Error sending {username} batch: {str(e)}")
                    
                    # Clean up immediately after sending
                    cleanup_images(batch_images)
                    
                    # Small delay to prevent rate limits
                    await asyncio.sleep(SEND_DELAY)

        total_downloaded += len(successful_downloads)
        total_sent += send_success_count
        batch_num += 1

        # Update progress
        now = time.time()
        progress_percent = int((total_downloaded / len(all_urls)) * 100) if all_urls else 100
        if (now - last_edit[0] > 10) or (progress_percent - last_progress_percent[0] >= 5):
            bar = generate_bar(progress_percent)
            progress = f"Batch {batch_num-1} ✅\n{bar} {progress_percent}%\n📥 Downloaded: {total_downloaded}\n📤 Sent: {total_sent}\n❌ Failed: {len(failed_urls)}"
            if progress_msg:
                try:
                    await progress_msg.edit(progress)
                except:
                    pass
            last_edit[0] = now
            last_progress_percent[0] = progress_percent

        # Memory cleanup after each batch
        del successful_downloads, username_groups
        gc.collect()

    # Final retry for remaining failed URLs
    if failed_urls:
        logger.info(f"🔄 Final retry for {len(failed_urls)} failed URLs")
        retry_successful, still_failed = await download_batch(failed_urls[:20], temp_dir)  # Limit final retry
        
        if retry_successful:
            # Group and send retry downloads
            username_groups = {}
            for download in retry_successful:
                username = url_to_username.get(download['url'], 'unknown')
                if username not in username_groups:
                    username_groups[username] = []
                username_groups[username].append(download)
                downloaded_urls.add(download['url'])

            for username, images in username_groups.items():
                for i in range(0, len(images), MEDIA_GROUP_SIZE):
                    batch_images = images[i:i + MEDIA_GROUP_SIZE]
                    user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
                    try:
                        success = await send_image_batch_pyrogram(batch_images, username, chat_id, user_topic, batch_num)
                        if success:
                            total_sent += len(batch_images)
                            successfully_sent_urls.update(img['url'] for img in batch_images)
                        else:
                            logger.warning(f"⚠️ Partial/failed retry send for {username}")
                    except Exception as e:
                        logger.error(f"❌ Retry send error for {username}: {str(e)}")
                    cleanup_images(batch_images)

            total_downloaded += len(retry_successful)

    # Final cleanup
    try:
        await aioshutil.rmtree(temp_dir)
    except:
        pass

    # Verify no images were missed
    expected_urls = set(all_urls)
    missing_urls = expected_urls - downloaded_urls
    unsent_urls = downloaded_urls - successfully_sent_urls
    
    if missing_urls:
        logger.warning(f"⚠️ {len(missing_urls)} URLs failed to download completely")
    if unsent_urls:
        logger.warning(f"⚠️ {len(unsent_urls)} downloaded images failed to send")
    
    logger.info(f"📊 Final Stats: {total_downloaded} downloaded, {total_sent} sent, {len(missing_urls)} failed")
    gc.collect()

    return total_downloaded, total_sent, len(all_urls)

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
    media_data, usernames, year_counts = extract_media_data_from_html(html_content)
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
