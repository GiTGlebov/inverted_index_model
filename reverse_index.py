from collections import defaultdict
from typing import List, Set
import re

class InvertedIndex:
    def __init__(self):
        # Обратный индекс: слово -> множество ID документов
        self.index = defaultdict(set)

    def _tokenize(self, text: str) -> List[str]:
        # Преобразование текста в список слов (токенизация)
        return re.findall(r'\w+', text.lower())

    def add_document(self, doc_id: int, text: str):
        # Добавление документа в индекс
        terms = self._tokenize(text)
        for term in terms:
            self.index[term].add(doc_id)

    def search(self, query: str) -> Set[int]:
        # Поиск документов, содержащих все слова из запроса
        terms = self._tokenize(query)
        if not terms:
            return set()

        # Получаем множества документов для каждого слова и находим их пересечение
        result_sets = [self.index.get(term, set()) for term in terms]
        return set.intersection(*result_sets)

# ----------- Тестирование -----------

def test_inverted_index():
    idx = InvertedIndex()
    docs = {
        1: "the quick brown fox",
        2: "jumped over the lazy dog",
        3: "the fox is quick and smart"
    }

    # Добавление документов в индекс
    for doc_id, text in docs.items():
        idx.add_document(doc_id, text)

    # Проверка результатов поиска
    assert idx.search("quick") == {1, 3}
    assert idx.search("fox") == {1, 3}
    assert idx.search("lazy") == {2}
    assert idx.search("quick fox") == {1, 3}
    assert idx.search("smart fox") == {3}
    assert idx.search("unknown") == set()
    print("All tests passed.")

if __name__ == "__main__":
    test_inverted_index()
