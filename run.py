from reverse_index import InvertedIndex
from gamma_delta_inverted_index import G_D_InvertedIndex
from work_with_bd import save_index_to_db, benchmark_search




if __name__ == "__main__":

    db_file = "crawler.sqlite"

    # Бенчмарк поиска
    benchmark_search(db_file, "classic_index", "ректор спбгу")
    benchmark_search(db_file, "compressed_index", "ректор спбгу")