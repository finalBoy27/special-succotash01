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
# ⚙️ CONFIG
# ───────────────────────────────
TIMEOUT = 15.0
DELAY_BETWEEN_REQUESTS = 0.3
TEMP_DB = "Scraping/tempImages.db"
MAX_CONCURRENT_WORKERS = 10  # Reduced for lower CPU usage on free tier
MAX_RETRIES = 3
RETRY_DELAY = 2
DOWNLOAD_TIMEOUT = 10
MAX_DOWNLOAD_RETRIES = 3
BATCH_SIZE = 10  # Reduced for lower memory usage
SEND_SEMAPHORE = asyncio.Semaphore(1)  # Allow 2 concurrent sends for speed without overloading
EXCLUDED_DOMAINS = ["pornbb.xyz"]
VALID_IMAGE_EXTS = ["jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "svg", "ico", "avif", "jfif"]
EXCLUDED_MEDIA_EXTS = ["mp4", "avi", "mov", "webm", "mkv", "flv", "wmv"]
ALLOWED_CHAT_IDS = {5809601894, 1285451259}

API_ID = int(os.getenv("API_ID", 24536446))
API_HASH = os.getenv("API_HASH", "baee9dd189e1fd1daf0fb7239f7ae704")
BOT_TOKEN = os.getenv("BOT_TOKEN", "7841933095:AAEz5SLNiGzWanheul1bwZL4HJbQBOBROqw")

bot = Client("image_downloader_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# ───────────────────────────────
# 🧩 LOGGING SETUP
# ───────────────────────────────
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# logging.getLogger('pyrogram').setLevel(logging.WARNING)

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

async def download_image(url, semaphore, max_retries=MAX_DOWNLOAD_RETRIES, base_timeout=5):
    """Download single image with retries, return bytes"""
    async with semaphore:
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)  # Add delay between requests
        for attempt in range(1, max_retries + 1):
            current_timeout = 5 + (attempt - 1) * 5  # 5s, 10s, 15s for attempts
            try:
                async with httpx.AsyncClient() as client:
                    r = await client.get(url, timeout=current_timeout)
                    if r.status_code == 200:
                        content = r.content
                        if len(content) > 100:  # Basic size check
                            return {'url': url, 'content': content}
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

async def download_batch(urls, base_timeout=5):
    """Download batch of URLs concurrently"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_WORKERS)
    tasks = [download_image(url, semaphore, base_timeout=base_timeout) for url in urls]
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

    # Send chunks sequentially to avoid rate limits
    for idx, chunk in enumerate(chunks):
        async def send_chunk(idx, chunk):
            async with SEND_SEMAPHORE:
                await asyncio.sleep(1.0)  # Balanced delay for speed without floods
                try:
                    media = []
                    current_batch_num = batch_num + idx
                    for i, img in enumerate(chunk):
                        if i == 0:
                            media.append(InputMediaPhoto(BytesIO(img['content']), caption=f"{username.replace('_', ' ')} - {current_batch_num}"))
                        else:
                            media.append(InputMediaPhoto(BytesIO(img['content'])))

                    if topic_id:
                        await bot.send_media_group(chat_id, media, reply_to_message_id=topic_id)
                    else:
                        await bot.send_media_group(chat_id, media)
                except FloodWait as e:
                    logger.warning(f"FloodWait on send for {username} chunk {idx}: waiting {e.value} seconds")
                    await asyncio.sleep(e.value)
                    # Retry once after wait
                    if topic_id:
                        await bot.send_media_group(chat_id, media, reply_to_message_id=topic_id)
                    else:
                        await bot.send_media_group(chat_id, media)
                except Exception as e:
                    logger.error(f"Error sending chunk {idx} for {username}: {str(e)}")
                    raise

        await send_chunk(idx, chunk)  # Await sequentially

    return True

def cleanup_images(images):
    """No cleanup needed for in-memory images"""
    pass

async def process_batches(username_images, chat_id, topic_id=None, user_topic_ids=None, progress_msg=None):
    """Process all URLs in dynamic send batches of BATCH_SIZE successful downloads"""
    total_images = sum(len(urls) for urls in username_images.values())
    total_downloaded = 0
    total_sent = 0
    failed_urls = []
    username_batch_nums = {}

    last_edit = [0]
    last_progress_percent = [0]  # Track last reported percentage

    # Collect all URLs in order
    pending_urls = []
    for username in username_images:
        pending_urls.extend(username_images[username])

    send_batch_num = 1

    while pending_urls or failed_urls:
        successful_for_send = []
        while len(successful_for_send) < BATCH_SIZE and (pending_urls or failed_urls):
            if pending_urls:
                num_to_take = min(BATCH_SIZE - len(successful_for_send), len(pending_urls))
                batch_urls = pending_urls[:num_to_take]
                pending_urls = pending_urls[num_to_take:]
                is_from_failed = False
            else:
                num_to_take = min(BATCH_SIZE - len(successful_for_send), len(failed_urls))
                batch_urls = failed_urls[:num_to_take]
                failed_urls = failed_urls[num_to_take:]
                is_from_failed = True

            successful, failed = await download_batch(batch_urls)
            successful_for_send.extend(successful)
            if not is_from_failed:
                failed_urls.extend(failed)
            # Else, discard failed from failed_urls

        if not successful_for_send:
            break

        # Update progress less frequently to save CPU
        processed = total_downloaded + len(successful_for_send)
        progress_percent = int((processed / total_images) * 100) if total_images else 100
        now = time.time()
        if (now - last_edit[0] > 30) and (progress_percent - last_progress_percent[0] >= 20):  # Less frequent updates
            bar = generate_bar(progress_percent)
            progress = f"completed {send_batch_num}\n{bar} {progress_percent}%\n📥 Processing send batch {send_batch_num}..."
            if progress_msg:
                try:
                    await progress_msg.edit(progress)
                except FloodWait as e:
                    logger.warning(f"FloodWait on progress edit: waiting {e.value} seconds")
                    await asyncio.sleep(e.value)  # Wait and retry once
                    await progress_msg.edit(progress)
                except Exception as e:
                    logger.error(f"Error editing progress message: {str(e)}")
            last_edit[0] = now
            last_progress_percent[0] = progress_percent

        # Group by username for sending
        username_groups = {}
        for img in successful_for_send:
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

        # Send groups concurrently but limited
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

        total_downloaded += len(successful_for_send)
        send_batch_num += 1

        # Aggressive memory cleanup
        del successful_for_send, username_groups, send_tasks, results
        gc.collect()

    # No temp dir to cleanup
    gc.collect()

    return total_downloaded, total_sent, total_images

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
    while True:
        try:
            bot.run()
            break
        except FloodWait as e:
            logger.warning(f"FloodWait on bot start: waiting {e.value} seconds")
            time.sleep(e.value)
        except Exception as e:
            logger.error(f"Bot failed to start: {e}")
            break
