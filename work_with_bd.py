import sqlite3
import pickle
from typing import Dict, Set

def save_index_to_db(index: Dict[str, object], db_path: str, table_name: str):
    # Сохраняет обратный индекс в SQLite базу данных
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(f"DROP TABLE IF EXISTS {table_name}")  # Удаление таблицы, если существует
    c.execute(f"CREATE TABLE {table_name} (term TEXT PRIMARY KEY, postings BLOB)")  # Создание новой таблицы

    for term, postings in index.items():
        data = pickle.dumps(postings)  # Сериализация postings
        c.execute(f"INSERT INTO {table_name} (term, postings) VALUES (?, ?)", (term, data))

    conn.commit()
    conn.close()


def load_index_from_db(db_path: str, table_name: str) -> Dict[str, object]:
    # Загружает обратный индекс из базы данных
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(f"SELECT term, postings FROM {table_name}")
    index = {term: pickle.loads(postings) for term, postings in c.fetchall()}  # Десериализация
    conn.close()
    return index


def search_index_in_db(db_path: str, table_name: str, query: str) -> Set[int]:
    # Поиск по индексу в БД
    import re
    terms = re.findall(r'\w+', query.lower())
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    postings_list = []

    for term in terms:
        c.execute(f"SELECT postings FROM {table_name} WHERE term = ?", (term,))
        row = c.fetchone()
        if row is None:
            conn.close()
            return set()  # Если хотя бы одного терма нет — возвращаем пустое множество
        postings = pickle.loads(row[0])  # Десериализация
        postings_list.append(set(postings) if isinstance(postings, list) else postings)

    conn.close()
    return set.intersection(*postings_list)


def benchmark_search(db_path: str, table_name: str, query: str, repeat: int = 10):
    # Измерение среднего времени поиска запроса в БД
    import time
    total_time = 0
    for _ in range(repeat):
        start = time.time()
        search_index_in_db(db_path, table_name, query)
        end = time.time()
        total_time += (end - start)
    avg_time = (total_time / repeat) * 1000  # перевод в миллисекунды
    print(f"Average search time for '{query}' in {table_name}: {avg_time:.2f} ms")
