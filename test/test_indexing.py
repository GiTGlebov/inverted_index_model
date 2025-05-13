import pytest
from reverse_index import InvertedIndex
from gamma_delta_inverted_index import G_D_InvertedIndex
from work_with_bd import save_index_to_db, load_index_from_db, search_index_in_db

# Фикстура с тестовыми документами
@pytest.fixture
def sample_docs():
    return {
        1: "the quick brown fox",
        2: "jumped over the lazy dog",
        3: "the fox is quick and smart"
    }

# 1. Проверка: обычный индекс ищет одно слово
def test_inverted_index_single_term(sample_docs):
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    assert idx.search("fox") == {1, 3}

# 2. Проверка: обычный индекс ищет несколько слов
def test_inverted_index_multiple_terms(sample_docs):
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    assert idx.search("quick fox") == {1, 3}

# 3. Проверка: пустой запрос возвращает пустое множество
def test_inverted_index_empty_query(sample_docs):
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    assert idx.search("") == set()

# 4. Проверка: отсутствующее слово не даёт результатов
def test_inverted_index_unknown_word(sample_docs):
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    assert idx.search("dragon") == set()

# 5. Проверка: сжатый индекс корректно сжимается и ищет
def test_compressed_index_compression_and_search(sample_docs):
    idx = G_D_InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    idx.compress()
    assert isinstance(idx.index['quick'], str)
    idx.decompress()
    assert idx.search("smart fox") == {3}

# 6. Проверка: порядок слов в запросе не влияет на результат
def test_index_word_order(sample_docs):
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    assert idx.search("fox smart") == {3}
    assert idx.search("smart fox") == {3}

# 7. Проверка: токенизация преобразует в нижний регистр
def test_tokenization_case_insensitive(sample_docs):
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    assert idx.search("FOX") == {1, 3}

# 8. Проверка: сохранение и загрузка индекса из БД
def test_save_and_load_index(tmp_path, sample_docs):
    db_path = tmp_path / "test.db"
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    save_index_to_db(idx.index, str(db_path), "test_index")
    loaded = load_index_from_db(str(db_path), "test_index")
    assert "fox" in loaded
    assert set(loaded["fox"]) == {1, 3}

# 9. Проверка: поиск по индексу, сохранённому в БД
def test_search_in_db(tmp_path, sample_docs):
    db_path = tmp_path / "test_search.db"
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    save_index_to_db(idx.index, str(db_path), "search_index")
    result = search_index_in_db(str(db_path), "search_index", "lazy")
    assert result == {2}

# 10. Проверка: запрос с частично отсутствующим словом возвращает пусто
def test_partial_term_missing(tmp_path, sample_docs):
    db_path = tmp_path / "test_mixed.db"
    idx = InvertedIndex()
    for doc_id, text in sample_docs.items():
        idx.add_document(doc_id, text)
    save_index_to_db(idx.index, str(db_path), "mixed_index")
    result = search_index_in_db(str(db_path), "mixed_index", "quick unknownword")
    assert result == set()
