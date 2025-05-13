import cProfile
import pstats
import asyncio
import httpx
from typing import Dict
from tqdm.asyncio import tqdm_asyncio
import time
import sys
import pickle
import requests
from bs4 import BeautifulSoup
from typing import List
from reverse_index import InvertedIndex
from gamma_delta_inverted_index import G_D_InvertedIndex
from work_with_bd import save_index_to_db, benchmark_search
import sqlite3


def fetch_and_clean(url: str) -> str:
    # Синхронная загрузка и очистка текста с веб-страницы
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'lxml')
        return soup.get_text(separator=' ', strip=True)
    except Exception as e:
        return ""


def load_links_from_file(path: str) -> List[str]:
    # Загрузка ссылок из текстового файла
    with open(path, 'r', encoding='windows-1251', errors='ignore') as f:
        return [line.strip() for line in f if line.strip()]


async def fetch(client: httpx.AsyncClient, url: str) -> str:
    # Асинхронная загрузка и извлечение основного текста со страницы
    try:
        response = await client.get(url, timeout=15)
        if response.status_code != 200:
            print(f"[SKIP] {url} — HTTP {response.status_code}")
            return ""
        soup = BeautifulSoup(response.text, 'html.parser')

        # Попытка извлечь содержимое из семантических блоков
        for selector in ['main', 'article', '[role="main"]', '.content', '#content']:
            content = soup.select_one(selector)
            if content:
                return content.get_text(separator=' ', strip=True)

        # Если ничего не найдено — возвращаем весь текст
        return soup.get_text(separator=' ', strip=True)

    except Exception as e:
        print(f"[ERROR] {url} — {type(e).__name__}: {e}")
        return ""


async def generate_documents_from_urls_async(urls: list[str], max_concurrent: int = 30) -> dict[int, str]:
    # Асинхронная генерация документов из списка URL
    documents = {}
    async with httpx.AsyncClient(follow_redirects=True) as client:
        sem = asyncio.Semaphore(max_concurrent)  # Ограничение параллелизма

        async def sem_fetch(idx, url):
            async with sem:
                text = await fetch(client, url)
                return (idx + 1, text) if text else None

        tasks = [sem_fetch(idx, url) for idx, url in enumerate(urls)]

        # Асинхронная обработка с прогресс-баром
        for coro in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Fetching URLs"):
            result = await coro
            if result:
                doc_id, text = result
                documents[doc_id] = text

    return documents


if __name__ == "__main__":

    db_file = "crawler.sqlite"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    # Получаем список ссылок из БД
    cursor.execute("SELECT name FROM links ")
    links = [row[0] for row in cursor.fetchall()]
    conn.close()

    # Загрузка текстов по URL (асинхронно)
    documents = asyncio.run(generate_documents_from_urls_async(links))

    # Создание двух типов индексов
    classic = InvertedIndex()
    compressed = G_D_InvertedIndex()

    # Добавление документов в оба индекса
    for doc_id, text in documents.items():
        classic.add_document(doc_id, text)
        compressed.add_document(doc_id, text)
        print()

    # Сжатие второго индекса
    compressed.compress()

    # Сохранение индексов в БД
    save_index_to_db(classic.index, db_file, "classic_index")
    save_index_to_db(compressed.index, db_file, "compressed_index")
