import sqlite3
import pickle
from typing import Dict, Set


def save_index_to_db(index: Dict[str, object], db_path: str, table_name: str):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(f"DROP TABLE IF EXISTS {table_name}")
    c.execute(f"CREATE TABLE {table_name} (term TEXT PRIMARY KEY, postings BLOB)")

    for term, postings in index.items():
        data = pickle.dumps(postings)
        c.execute(f"INSERT INTO {table_name} (term, postings) VALUES (?, ?)", (term, data))

    conn.commit()
    conn.close()


def load_index_from_db(db_path: str, table_name: str) -> Dict[str, object]:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(f"SELECT term, postings FROM {table_name}")
    index = {term: pickle.loads(postings) for term, postings in c.fetchall()}
    conn.close()
    return index


def search_index_in_db(db_path: str, table_name: str, query: str) -> Set[int]:
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
            return set()
        postings = pickle.loads(row[0])
        postings_list.append(set(postings) if isinstance(postings, list) else postings)

    conn.close()
    return set.intersection(*postings_list)


def benchmark_search(db_path: str, table_name: str, query: str, repeat: int = 10):
    import time
    total_time = 0
    for _ in range(repeat):
        start = time.time()
        search_index_in_db(db_path, table_name, query)
        end = time.time()
        total_time += (end - start)
    avg_time = (total_time / repeat) * 1000  # ms
    print(f"Average search time for '{query}' in {table_name}: {avg_time:.2f} ms")



