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
from urllib.parse import urlencode, urljoin
from selectolax.parser import HTMLParser
from datetime import datetime
from pathlib import Path
from io import BytesIO
from pyrogram import Client, filters
from pyrogram.types import Update, Message, InputMediaPhoto
import aiosqlite
from fastapi import FastAPI
import uvicorn
import threading

# Health check app
app = FastAPI()

@app.get('/')
def root():
    return {"status": "OK"}

@app.get('/health')
def health():
    return {"status": "OK"}

def run_fastapi():
    uvicorn.run(app, host='0.0.0.0', port=int(os.getenv("PORT", 10001)))

# Disable FastAPI logs if needed, but for now keep
logging.getLogger('uvicorn').disabled = True  # optional

threading.Thread(target=run_fastapi, daemon=True).start()

# ───────────────────────────────
# ⚙️ CONFIG
# ───────────────────────────────
TIMEOUT = 15.0
DELAY_BETWEEN_REQUESTS = 0.2
TEMP_DB = "Scraping/tempImages.db"
MAX_CONCURRENT_WORKERS = 10
MAX_RETRIES = 3
RETRY_DELAY = 2
DOWNLOAD_TIMEOUT = 20.0
MAX_DOWNLOAD_RETRIES = 3
BATCH_SIZE = 10
EXCLUDED_DOMAINS = ["pornbb.xyz"]
VALID_IMAGE_EXTS = ["jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "svg", "ico", "avif", "jfif"]
EXCLUDED_MEDIA_EXTS = ["mp4", "avi", "mov", "webm", "mkv", "flv", "wmv"]

API_ID = int(os.getenv("API_ID", 24536446))
API_HASH = os.getenv("API_HASH", "baee9dd189e1fd1daf0fb7239f7ae704")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7564190075:AAEV0Uz3DRuIAAfNJKF2IpzaOQBRYxSo4eg")

bot = Client("image_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ───────────────────────────────
# 🧩 LOGGING SETUP
# ───────────────────────────────
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress Pyrogram connection logs
logging.getLogger('pyrogram').setLevel(logging.WARNING)

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

async def download_image(url, temp_dir, semaphore, max_retries=MAX_DOWNLOAD_RETRIES, timeout=DOWNLOAD_TIMEOUT):
    """Download single image with retries"""
    async with semaphore:
        for attempt in range(1, max_retries + 1):
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.get(url, timeout=timeout)
                    if r.status_code == 200:
                        content = r.content
                        if len(content) > 100:  # Basic size check
                            # Save to temp file
                            filename = f"temp_{int(time.time() * 1000000)}_{len(content)}.jpg"
                            filepath = os.path.join(temp_dir, filename)
                            with open(filepath, 'wb') as f:
                                f.write(content)
                            return {'url': url, 'path': filepath, 'content': content}
                        else:
                            logger.warning(f"Image too small: {url}")
                            return None
                    elif r.status_code == 404:
                        logger.info(f"404 Not Found: {url} - skipping")
                        return None  # Not a failed URL, just not available
                    else:
                        logger.warning(f"HTTP {r.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Download attempt {attempt} failed for {url}: {str(e)}")
            if attempt < max_retries:
                await asyncio.sleep(RETRY_DELAY)
        logger.error(f"Failed to download after {max_retries} attempts: {url}")
        return None

async def download_batch(urls, temp_dir):
    """Download batch of URLs concurrently"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)
    tasks = [download_image(url, temp_dir, semaphore) for url in urls]
    results = await asyncio.gather(*tasks)
    successful = [r for r in results if r is not None]
    failed = [url for url, r in zip(urls, results) if r is None]
    return successful, failed

async def send_image_batch_pyrogram(images, username, chat_id, topic_id=None, batch_num=1):
    """Send batch of images using Pyrogram, split into groups of 10"""
    if not images:
        return False

    # Split into chunks of 10
    chunk_size = 10
    chunks = [images[i:i + chunk_size] for i in range(0, len(images), chunk_size)]

    # Send chunks concurrently
    send_tasks = []
    for idx, chunk in enumerate(chunks):
        try:
            media = []
            current_batch_num = batch_num + idx
            for i, img in enumerate(chunk):
                if i == 0:
                    media.append(InputMediaPhoto(img['path'], caption=f"{username.replace('_', ' ')} - {current_batch_num}"))
                else:
                    media.append(InputMediaPhoto(img['path']))

            if topic_id:
                send_tasks.append(bot.send_media_group(chat_id, media, message_thread_id=topic_id))
            else:
                send_tasks.append(bot.send_media_group(chat_id, media))
        except Exception as e:
            logger.error(f"Error preparing chunk {idx} for {username}: {str(e)}")
            return False

    # Execute all sends concurrently
    results = await asyncio.gather(*send_tasks, return_exceptions=True)
    for idx, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Error sending chunk {idx} for {username}: {str(result)}")
            return False
    return True

def cleanup_images(images):
    """Remove temp image files"""
    for img in images:
        try:
            if os.path.exists(img['path']):
                os.remove(img['path'])
        except Exception as e:
            logger.warning(f"Error cleaning up {img['path']}: {str(e)}")

async def process_batches(username_images, chat_id, topic_id=None, user_topic_ids=None, progress_msg=None):
    """Process all URLs in batches of BATCH_SIZE"""
    total_images = sum(len(urls) for urls in username_images.values())
    total_downloaded = 0
    total_sent = 0
    failed_urls = []
    username_batch_nums = {}

    temp_dir = "temp_images"
    os.makedirs(temp_dir, exist_ok=True)

    last_edit = [0]

    # Collect all URLs in order
    all_urls = []
    for username in username_images:
        all_urls.extend(username_images[username])

    # Process in batches
    for i in range(0, len(all_urls), BATCH_SIZE):
        batch_urls = all_urls[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (len(all_urls) + BATCH_SIZE - 1) // BATCH_SIZE

        # Update progress
        now = time.time()
        if now - last_edit[0] > 3:
            progress_percent = int((i / len(all_urls)) * 100)
            bar = generate_bar(progress_percent)
            progress = f"completed {batch_num}/{total_batches}\n{bar} {progress_percent}%\n📥 Downloading batch {batch_num}..."
            if progress_msg:
                await progress_msg.edit(progress)
            last_edit[0] = now

        # Download batch
        downloaded, batch_failed = await download_batch(batch_urls, temp_dir)
        failed_urls.extend(batch_failed)
        total_downloaded += len(downloaded)

        if downloaded:
            # Group by username for sending
            username_groups = {}
            for img in downloaded:
                # Find username for this URL
                username = None
                for u, urls in username_images.items():
                    if img['url'] in urls:
                        username = u
                        break
                if username:
                    if username not in username_groups:
                        username_groups[username] = []
                    username_groups[username].append(img)

            # Send groups concurrently
            send_tasks = []
            for username, imgs in username_groups.items():
                user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
                batch_num_user = username_batch_nums.get(username, 1)
                send_tasks.append(send_image_batch_pyrogram(imgs, username, chat_id, user_topic, batch_num_user))
            results = await asyncio.gather(*send_tasks, return_exceptions=True)
            for (username, imgs), success in zip(username_groups.items(), results):
                if isinstance(success, bool) and success:
                    num_chunks = (len(imgs) + 9) // 10
                    username_batch_nums[username] = username_batch_nums.get(username, 1) + num_chunks
                    total_sent += len(imgs)
                else:
                    failed_urls.extend([img['url'] for img in imgs])

            # Cleanup
            cleanup_images(downloaded)
            del downloaded, username_groups, send_tasks, results
            log_memory()
            gc.collect()

    # Now process failed URLs
    if failed_urls:
        logger.info(f"Processing {len(failed_urls)} failed URLs")
        now = time.time()
        if now - last_edit[0] > 3:
            progress = f"🔄 Retrying {len(failed_urls)} failed URLs..."
            if progress_msg:
                await progress_msg.edit(progress)
            last_edit[0] = now

        for i in range(0, len(failed_urls), BATCH_SIZE):
            batch_urls = failed_urls[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (len(failed_urls) + BATCH_SIZE - 1) // BATCH_SIZE

            downloaded, _ = await download_batch(batch_urls, temp_dir)

            if downloaded:
                # Group and send
                username_groups = {}
                for img in downloaded:
                    username = None
                    for u, urls in username_images.items():
                        if img['url'] in urls:
                            username = u
                            break
                    if username:
                        if username not in username_groups:
                            username_groups[username] = []
                        username_groups[username].append(img)

                for username, imgs in username_groups.items():
                    user_topic = user_topic_ids.get(username) if user_topic_ids else topic_id
                    batch_num_user = username_batch_nums.get(username, 1)
                    success = await send_image_batch_pyrogram(imgs, username, chat_id, user_topic, batch_num_user)
                    if success:
                        num_chunks = (len(imgs) + 9) // 10
                        username_batch_nums[username] = username_batch_nums.get(username, 1) + num_chunks
                        total_sent += len(imgs)

                cleanup_images(downloaded)
                del downloaded, username_groups
                log_memory()
                gc.collect()

    # Cleanup temp dir
    try:
        await aioshutil.rmtree(temp_dir)
    except:
        pass
    log_memory()
    gc.collect()

    return total_downloaded, total_sent, total_images

# ───────────────────────────────
# BOT HANDLER
# ───────────────────────────────
@bot.on_message(filters.command("down") & filters.private)
async def handle_down(client: Client, message: Message):
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

    # Handle topic creation
    user_topic_ids = {}
    if create_topics_per_user:
        for username in username_images.keys():
            try:
                topic = await client.create_forum_topic(target_chat_id, f"Media - {username}")
                user_topic_ids[username] = topic.message_thread_id
            except Exception as e:
                logger.error(f"Failed to create topic for {username}: {str(e)}")
                user_topic_ids[username] = None
    elif create_topic_name:
        try:
            topic = await client.create_forum_topic(target_chat_id, create_topic_name)
            target_topic_id = topic.message_thread_id
        except Exception as e:
            await message.reply(f"Failed to create topic: {str(e)}")
            return

    # Send initial progress
    progress_msg = await message.reply("Starting download process...")

    # Process batches
    total_downloaded, total_sent, total_filtered = await process_batches(
        username_images, target_chat_id, target_topic_id, user_topic_ids, progress_msg
    )

    # Final stats
    stats = f"""✅ Download Complete!

📊 Statistics:
• Total Media Items: {total_media}
• Total Images Filtered: {total_filtered}
• Total Download Attempts: {total_images}
• Successful Downloads: {total_downloaded}
• Successfully Sent: {total_sent}"""

    await progress_msg.edit(stats)

# ───────────────────────────────
# MAIN
# ───────────────────────────────
if __name__ == "__main__":
    threading.Thread(target=run_fastapi, daemon=True).start()
    bot.run()