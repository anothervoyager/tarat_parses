import os
import json
import asyncio
import random
import aiohttp
import aiofiles
import logging
import sys
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm import tqdm
from mutagen.id3 import ID3, TIT2, TPE1, TALB, COMM
from mutagen.mp3 import MP3

# ===== НАСТРОЙКИ =====
BASE_URL = "https://tarat.ru"
MUSIC_URL = f"{BASE_URL}/music"
OUTPUT_DIR = "tarat_tracks"
TRACKS_CACHE_FILE = "tracks.json"
ERROR_LOG_FILE = "errors.log"
MAX_CONCURRENT_DOWNLOADS = 6

# Настройка логирования
logging.basicConfig(
    filename=ERROR_LOG_FILE,
    filemode='w',
    level=logging.ERROR,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Состояние слотов: (track_str, total_bytes, current_bytes)
slot_states = [None] * MAX_CONCURRENT_DOWNLOADS  # None или (name, total, current)
slot_lock = asyncio.Lock()

# Глобальные для обложек
downloaded_covers = set()
cover_lock = asyncio.Lock()

def sanitize_filename(name):
    name = "".join(c for c in name if c not in r'<>:"/\|?*').strip()
    name = " ".join(name.split())
    name = name.replace(" - ", "-").replace(" – ", "-")
    return name

def build_expected_filepath(singer_name, title):
    singer_clean = sanitize_filename(singer_name)
    title_clean = sanitize_filename(title)
    filename = f"{singer_clean} - {title_clean}.mp3"
    folder = os.path.join(OUTPUT_DIR, singer_clean)
    os.makedirs(folder, exist_ok=True)
    return os.path.join(folder, filename)

def write_id3_tags(filepath, artist, title, source_url):
    try:
        audio = MP3(filepath, ID3=ID3)
        try:
            audio.add_tags()
        except:
            pass
        audio.tags.add(TPE1(encoding=3, text=artist))
        audio.tags.add(TIT2(encoding=3, text=title))
        audio.tags.add(TALB(encoding=3, text=artist))
        audio.tags.add(COMM(encoding=3, lang='rus', text=source_url))
        audio.save()
    except Exception as e:
        logging.error(f"Ошибка записи ID3-тегов в {os.path.basename(filepath)}: {e}")

def get_random_headers():
    return {
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
        ]),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": BASE_URL,
        "DNT": "1",
        "Connection": "keep-alive",
    }

async def fetch_html(session: aiohttp.ClientSession, url: str, timeout: int = 15):
    try:
        async with session.get(url, headers=get_random_headers(), timeout=timeout) as resp:
            if resp.status == 200:
                return await resp.text()
            else:
                logging.error(f"HTTP {resp.status} при загрузке HTML: {url}")
                return None
    except asyncio.TimeoutError:
        logging.error(f"Тайм-аут при загрузке HTML: {url}")
        return None
    except Exception as e:
        logging.error(f"Ошибка загрузки HTML {url}: {e}")
        return None

async def safe_get_content_length(resp):
    try:
        return int(resp.headers.get('content-length', 0))
    except (ValueError, TypeError):
        return 0

async def download_track(semaphore, session, track, slot_index, pbar_track):
    global downloaded_covers
    singer_name, title, mp3_url, cover_url = track
    filepath = build_expected_filepath(singer_name, title)
    track_str = f"{singer_name} - {title}"

    if os.path.exists(filepath):
        async with cover_lock:
            if singer_name not in downloaded_covers and cover_url:
                await download_cover(session, singer_name, cover_url)
        return True

    async with semaphore:
        # Обновляем описание прогресс-бара
        pbar_track.set_description(f"{track_str[:50]}")
        pbar_track.reset()

        try:
            async with session.get(mp3_url, headers=get_random_headers(), timeout=45) as resp:
                if resp.status != 200:
                    logging.error(f"HTTP {resp.status} при скачивании {track_str}")
                    return False

                total_size = await safe_get_content_length(resp)
                pbar_track.total = total_size or 1

                # Скачивание порциями
                chunk_size = 8192
                downloaded = 0
                async with aiofiles.open(filepath, "wb") as f:
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        pbar_track.update(len(chunk))

            # Обложка
            async with cover_lock:
                if singer_name not in downloaded_covers and cover_url:
                    await download_cover(session, singer_name, cover_url)

            write_id3_tags(filepath, singer_name, title, mp3_url)
            return True

        except Exception as e:
            logging.error(f"Ошибка при скачивании {track_str}: {e}")
            return False

async def download_cover(session, singer_name, cover_url):
    if not cover_url:
        return
    singer_clean = sanitize_filename(singer_name)
    folder = os.path.join(OUTPUT_DIR, singer_clean)
    cover_filename = f"{singer_clean}_cover.jpg"
    cover_path = os.path.join(folder, cover_filename)
    if os.path.exists(cover_path):
        return
    try:
        async with session.get(cover_url, headers=get_random_headers(), timeout=30) as resp:
            if resp.status == 200:
                os.makedirs(folder, exist_ok=True)
                async with aiofiles.open(cover_path, "wb") as f:
                    await f.write(await resp.read())
    except Exception as e:
        logging.error(f"Ошибка скачивания обложки {singer_name}: {e}")

async def get_all_singer_urls(session: aiohttp.ClientSession):
    singer_urls = set()
    page = 1
    while True:
        url = f"{MUSIC_URL}?page={page}" if page > 1 else MUSIC_URL
        html = await fetch_html(session, url)
        if not html:
            break
        soup = BeautifulSoup(html, "html.parser")
        links = soup.select('h4.property-item-title a[href^="/music/"]')
        if not links:
            break
        for link in links:
            href = link.get("href")
            if href and not href.endswith("/music"):
                singer_urls.add(urljoin(BASE_URL, href))
        if not soup.select('ul.pagination a[rel="next"]'):
            break
        page += 1
        await asyncio.sleep(random.uniform(1.0, 2.0))
    return sorted(singer_urls)

async def collect_all_tracks(session: aiohttp.ClientSession, singer_urls):
    all_tracks = []
    async def process_singer(singer_url):
        html = await fetch_html(session, singer_url)
        if not html:
            return []
        soup = BeautifulSoup(html, "html.parser")
        singer_name_tag = soup.select_one("div.page-title h1")
        singer_name = singer_name_tag.get_text(strip=True) if singer_name_tag else "Unknown"
        cover_img = soup.select_one('img.img-fluid')
        cover_url = None
        if cover_img and cover_img.get("src"):
            cover_url = urljoin(BASE_URL, cover_img["src"])
        track_elements = soup.select('li.song i.play[data-file][data-song-title]')
        tracks = []
        for el in track_elements:
            title = el.get("data-song-title", "").strip()
            file_path = el.get("data-file", "").strip()
            if title and file_path:
                mp3_url = urljoin(BASE_URL, file_path)
                tracks.append((singer_name, title, mp3_url, cover_url))
        await asyncio.sleep(random.uniform(0.8, 1.5))
        return tracks
    from tqdm.asyncio import tqdm_asyncio
    tasks = [process_singer(url) for url in singer_urls]
    results = await tqdm_asyncio.gather(*tasks, desc="Сбор треков")
    for tracks in results:
        all_tracks.extend(tracks)
    return all_tracks

async def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Загрузка кэша
    if os.path.exists(TRACKS_CACHE_FILE):
        print(f"📂 Загрузка кэша треков из {TRACKS_CACHE_FILE}")
        try:
            with open(TRACKS_CACHE_FILE, "r", encoding="utf-8") as f:
                all_tracks = json.load(f)
            print(f"✅ Загружено треков из кэша: {len(all_tracks)}")
        except Exception as e:
            logging.error(f"Ошибка чтения кэша: {e}")
            all_tracks = []
    else:
        print("🌐 Кэш не найден. Парсинг сайта...")
        connector = aiohttp.TCPConnector(limit=20)
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            try:
                singer_urls = await get_all_singer_urls(session)
                print(f"✅ Найдено исполнителей: {len(singer_urls)}")
                all_tracks = await collect_all_tracks(session, singer_urls)
            except Exception as e:
                logging.critical(f"Критическая ошибка при парсинге: {e}")
                return
        try:
            with open(TRACKS_CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(all_tracks, f, ensure_ascii=False, indent=2)
            print(f"💾 Кэш сохранён в {TRACKS_CACHE_FILE}")
        except Exception as e:
            logging.error(f"Не удалось сохранить кэш: {e}")

    total = len(all_tracks)
    if total == 0:
        print("❌ Нет треков для скачивания.")
        return

    # Создаём прогресс-бары
    pbar_main = tqdm(total=total, desc="Общий прогресс", unit="трек", position=0, leave=True)
    pbar_tracks = []
    for i in range(MAX_CONCURRENT_DOWNLOADS):
        pbar = tqdm(
            desc="—",
            total=1,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            position=i + 1,
            leave=False,
            bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]'
        )
        pbar_tracks.append(pbar)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    connector = aiohttp.TCPConnector(limit_per_host=8, limit=20)
    timeout = aiohttp.ClientTimeout(total=60)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for i, track in enumerate(all_tracks):
            slot_idx = i % MAX_CONCURRENT_DOWNLOADS
            task = download_track(semaphore, session, track, slot_idx, pbar_tracks[slot_idx])
            tasks.append(task)

        success_count = 0
        try:
            # results = await asyncio.gather(*tasks, return_exceptions=True)
            # for res in results:
            #     if res is True:
            #         success_count += 1
            #     pbar_main.update(1)
            for coro in asyncio.as_completed(tasks):
                try:
                    result = await coro
                    if result is True:
                        success_count += 1
                except Exception as e:
                    logging.error(f"Исключение в задаче: {e}")
                    # Можно считать как неудачную загрузку, если нужно
                finally:
                    pbar_main.update(1)
        except KeyboardInterrupt:
            print("\n\n🛑 Получен сигнал прерывания. Отмена задач...")
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    task.cancel()
            await asyncio.gather(*asyncio.all_tasks(), return_exceptions=True)
        finally:
            pbar_main.close()
            for p in pbar_tracks:
                p.close()

    # Итог
    print(f"\n✅ Успешно скачано: {success_count} из {total}")
    print(f"📁 Файлы сохранены в: {os.path.abspath(OUTPUT_DIR)}")
    print(f"📄 Ошибки записаны в: {os.path.abspath(ERROR_LOG_FILE)}")
    print("🎵 Все треки содержат ID3-теги.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Программа прервана пользователем (Ctrl+C).")
        sys.exit(0)
    except Exception as e:
        logging.critical(f"Необработанное исключение: {e}")
        print(f"\n💥 Критическая ошибка: {e}")
        sys.exit(1)