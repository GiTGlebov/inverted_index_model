from collections import defaultdict
from typing import List, Set
import re

class InvertedIndex:
    def __init__(self):
        self.index = defaultdict(set)

    def _tokenize(self, text: str) -> List[str]:
        return re.findall(r'\w+', text.lower())

    def add_document(self, doc_id: int, text: str):
        terms = self._tokenize(text)
        for term in terms:
            self.index[term].add(doc_id)

    def search(self, query: str) -> Set[int]:
        terms = self._tokenize(query)
        if not terms:
            return set()

        result_sets = [self.index.get(term, set()) for term in terms]
        return set.intersection(*result_sets)

def test_inverted_index():
    idx = InvertedIndex()
    docs = {
        1: "the quick brown fox",
        2: "jumped over the lazy dog",
        3: "the fox is quick and smart"
    }

    for doc_id, text in docs.items():
        idx.add_document(doc_id, text)

    assert idx.search("quick") == {1, 3}
    assert idx.search("fox") == {1, 3}
    assert idx.search("lazy") == {2}
    assert idx.search("quick fox") == {1, 3}
    assert idx.search("smart fox") == {3}
    assert idx.search("unknown") == set()
    print("All tests passed.")


if __name__ == "__main__":
    test_inverted_index()
