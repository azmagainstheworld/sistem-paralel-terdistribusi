import threading
import queue
import os
import time

FILE_PATH = "data.txt"
READER_THREADS = 2


def get_file_ranges(file_path: str, thread_count: int):
    file_size = os.path.getsize(file_path)
    chunk_size = file_size // thread_count
    ranges = []
    start = 0

    for index in range(thread_count):
        end = start + chunk_size
        if index == thread_count - 1:
            end = file_size
        ranges.append((start, end))
        start = end

    return ranges


def read_chunk_task(chunk_index: int, start: int, end: int, data_queue: queue.Queue):
    """Thread pembaca: baca baris dari jangkauan file yang ditentukan."""
    print(f"[{threading.current_thread().name}] Mulai membaca chunk {chunk_index} dari byte {start} sampai {end}")
    try:
        with open(FILE_PATH, "r", encoding="utf-8") as file:
            file.seek(start)

            if start != 0:
                file.readline()

            while file.tell() < end:
                line = file.readline()
                if not line:
                    break
                text = line.rstrip("\n")
                if text:
                    print(f"[{threading.current_thread().name}] Sedang mengambil: {text}")
                    data_queue.put((chunk_index, text))
                    time.sleep(0.05)
    except FileNotFoundError:
        data_queue.put((chunk_index, f"File tidak ditemukan: {FILE_PATH}"))
    finally:
        data_queue.put((chunk_index, None))


def print_task(data_queue: queue.Queue, reader_count: int):
    """Thread penampil: tampilkan data yang diterima dari beberapa thread pembaca."""
    end_signals = 0

    while end_signals < reader_count:
        item = data_queue.get()
        chunk_index, text = item

        if text is None:
            end_signals += 1
            continue

        print(f"[{threading.current_thread().name}] Menampilkan data: {text}")
        print("============================================================")

    print("Selesai menampilkan data dari semua thread pembaca.")


def main():
    data_queue = queue.Queue()
    ranges = get_file_ranges(FILE_PATH, READER_THREADS)

    readers = []
    for chunk_index, (start, end) in enumerate(ranges):
        reader = threading.Thread(
            target=read_chunk_task,
            args=(chunk_index, start, end, data_queue),
            name=f"Thread-1: Baca-{chunk_index}",
        )
        readers.append(reader)

    printer = threading.Thread(target=print_task, args=(data_queue, READER_THREADS), name="Thread-2: Layar")

    start_time = time.perf_counter()

    printer.start()
    for reader in readers:
        reader.start()

    for reader in readers:
        reader.join()
    printer.join()

    elapsed = time.perf_counter() - start_time
    print(f"Program selesai.")
    print(f"Total Waktu Eksekusi (Parallel): {elapsed:.3f} detik")


if __name__ == "__main__":
    main()
