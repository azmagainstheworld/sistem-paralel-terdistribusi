import threading
import queue
import time

FILE_PATH = "data.txt"


def read_file_task(data_queue: queue.Queue, ack_queue: queue.Queue):
    """Thread pembaca: baca baris dari file, kirim ke antrean, dan tunggu konfirmasi."""
    try:
        with open(FILE_PATH, "r", encoding="utf-8") as file:
            for line in file:
                text = line.rstrip("\n")
                if text:
                    print(f"[{threading.current_thread().name}] Sedang mengambil: {text}")
                    data_queue.put(text)
                    ack_queue.get()
    except FileNotFoundError:
        data_queue.put(f"File tidak ditemukan: {FILE_PATH}")
    finally:
        data_queue.put(None)


def print_task(data_queue: queue.Queue, ack_queue: queue.Queue):
    """Thread penampil: ambil item dari antrean, tampilkan, lalu konfirmasi selesai."""
    while True:
        item = data_queue.get()
        if item is None:
            break
        print(f"[{threading.current_thread().name}] Menampilkan data: {item}")
        print("--------------------------------------------------------------")
        ack_queue.put(True)


def main():
    data_queue = queue.Queue()
    ack_queue = queue.Queue()

    reader = threading.Thread(
        target=read_file_task,
        args=(data_queue, ack_queue),
        name="Thread-1: Baca",
    )
    printer = threading.Thread(
        target=print_task,
        args=(data_queue, ack_queue),
        name="Thread-2: Layar",
    )

    start_time = time.perf_counter()

    printer.start()
    reader.start()

    reader.join()
    printer.join()

    elapsed = time.perf_counter() - start_time
    print(f"Total Waktu Eksekusi: {elapsed:.3f} detik")


if __name__ == "__main__":
    main()
