from collections import defaultdict
from typing import List, Set
import re

class G_D_InvertedIndex:
    def __init__(self):
        # Инициализация обратного индекса: терм -> список документов
        self.index = defaultdict(list)

    def _tokenize(self, text: str) -> List[str]:
        # Разделение текста на слова (токенизация)
        return re.findall(r'\w+', text.lower())

    def _delta_encode(self, nums: List[int]) -> List[int]:
        # Дельта-кодирование списка чисел
        if not nums:
            return []
        nums.sort()
        encoded = [nums[0]]
        for i in range(1, len(nums)):
            encoded.append(nums[i] - nums[i - 1])
        return encoded

    def _delta_decode(self, nums: List[int]) -> List[int]:
        # Декодирование дельта-кодированного списка
        result = []
        total = 0
        for num in nums:
            total += num
            result.append(total)
        return result

    def _gamma_encode(self, num: int) -> str:
        # Гамма-кодирование одного числа
        if num <= 0:
            raise ValueError("Gamma coding only supports positive integers")
        binary = bin(num)[2:]
        offset = binary[1:]
        length = '0' * (len(binary) - 1)
        return length + '1' + offset

    def _gamma_decode(self, bits: str) -> List[int]:
        # Гамма-декодирование битовой строки
        i = 0
        decoded = []
        while i < len(bits):
            zeros = 0
            while i < len(bits) and bits[i] == '0':
                zeros += 1
                i += 1
            i += 1  # пропускаем '1'
            if i + zeros - 1 >= len(bits):
                break
            binary = '1' + bits[i:i + zeros]
            decoded.append(int(binary, 2))
            i += zeros
        return decoded

    def _encode_postings(self, postings: List[int]) -> str:
        # Кодирование списка документов (postings) с помощью дельта- и гамма-кодирования
        deltas = self._delta_encode(postings)
        return ''.join(self._gamma_encode(x) for x in deltas)

    def _decode_postings(self, bits: str) -> List[int]:
        # Декодирование битовой строки обратно в postings
        deltas = self._gamma_decode(bits)
        return self._delta_decode(deltas)

    def add_document(self, doc_id: int, text: str):
        # Добавление документа в индекс
        terms = self._tokenize(text)
        for term in set(terms):
            self.index[term].append(doc_id)

    def compress(self):
        # Сжатие всего индекса
        for term in self.index:
            self.index[term] = self._encode_postings(self.index[term])

    def decompress(self):
        # Распаковка (декодирование) всего индекса
        for term in self.index:
            self.index[term] = self._decode_postings(self.index[term])

    def search(self, query: str) -> Set[int]:
        # Поиск документов, содержащих все слова из запроса
        terms = self._tokenize(query)
        if not terms:
            return set()

        postings = []
        for term in terms:
            val = self.index.get(term, '')
            if isinstance(val, str):  # если данные сжаты
                val = self._decode_postings(val)
            postings.append(set(val))

        return set.intersection(*postings)  # пересечение списков документов


# ----------- Тестирование -----------

def test_inverted_index():
    idx = G_D_InvertedIndex()
    docs = {
        1: "the quick brown fox",
        2: "jumped over the lazy dog",
        3: "the fox is quick and smart"
    }

    for doc_id, text in docs.items():
        idx.add_document(doc_id, text)

    idx.compress()
    assert isinstance(idx.index['quick'], str)  # должно быть сжато

    idx.decompress()
    assert idx.search("quick") == {1, 3}
    assert idx.search("fox") == {1, 3}
    assert idx.search("lazy") == {2}
    assert idx.search("quick fox") == {1, 3}
    assert idx.search("smart fox") == {3}
    assert idx.search("unknown") == set()
    print("All tests passed.")

if __name__ == "__main__":
    test_inverted_index()
