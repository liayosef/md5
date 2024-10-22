import socket
import threading
import multiprocessing
import protocol
import hashlib
import numpy as np
import logging

HOST = '127.0.0.1'
PORT = 65432


def find_matching_md5_worker(args):
    """
       Worker function for parallel MD5 hash matching within a process.

       Args:
           args (tuple): A tuple containing the start (inclusive), end (exclusive),
                         message to search for, and number of threads for this worker.

       Returns:
           int: The matching number if found, otherwise 1 (indicating no match).

       Raises:
           AssertionError: If the start value is greater than the end value.
       """
    start, end, message = args
    assert start <= end,"Invalid range: start must be less than or equal to end"
    numbers = np.arange(start, end + 1)
    hashes = np.array([hashlib.md5(str(num).encode('utf-8')).hexdigest() for num in numbers])
    indices = np.where(hashes == message)[0]
    if len(indices) > 0:
        return numbers[indices[0]]
    else:
        logging.warning("לא נמצא מספר מתאים")
        return 1


def find_matching_md5_multithreaded(start, end, message, num_threads):
    """
       Finds a number with the matching MD5 hash within a given range using multithreading.

       Args:
           start (int): The inclusive starting value of the search range.
           end (int): The exclusive ending value of the search range (not included).
           message (str): The MD5 hash to search for.
           num_threads (int): The number of threads to use for parallel processing.

       Returns:
           list: A list containing the matching numbers found by all threads,
                 or an empty list if no matches are found.

       Raises:
           AssertionError: If the number of threads is not positive.
       """
    chunk_size = (end - start) // num_threads
    threads = []
    for i in range(num_threads):
        thread_start = start + i * chunk_size
        thread_end = min(start + (i + 1) * chunk_size, end)
        thread = threading.Thread(target=find_matching_md5_worker, args=((thread_start, thread_end, message),))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    results = [thread.result() for thread in threads]  # קבלת תוצאות מה-Threads
    return results


def client():
    """
      Client-side program logic for finding a number with a matching MD5 hash.

      Establishes a connection with the server, receives communication,
      performs parallel MD5 hash matching using workers and threads,
      and sends the result back to the server.
      """
    logging.basicConfig(level=logging.DEBUG)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        logging.info("לקוח התחבר לשרת")
        message = protocol.recv_protocol(s).decode()
        print(f"לקוח קיבל: {message}")

        # שליחת מספר הליבות לשרת
        num_cores = multiprocessing.cpu_count()
        cores = str(num_cores).encode()
        msg = protocol.send_protocol(cores)
        s.sendall(msg)

        # קבלת המספרים מהשרת
        received_message = protocol.recv_protocol(s)
        start = int(received_message.decode())
        received_message2 = protocol.recv_protocol(s)
        end = int(received_message2.decode())
        print(f"לקוח קיבל טווח: {start} - {end}")
        logging.info("קיבלתי את המספרים %s ו-%s", start, end)

        assert isinstance(start, int)
        assert isinstance(end, int)

        num_processes = multiprocessing.cpu_count()
        num_threads_per_process = 4
        assert num_processes > 0
        assert num_threads_per_process > 0

        chunk_size = (end - start) // num_processes
        chunks = [(start + i * chunk_size, start + (i + 1) * chunk_size, message, num_threads_per_process) for i in
                  range(num_processes)]

        # שימוש ב-Multiprocessing עם Pool
        with multiprocessing.Pool() as pool:
            results = pool.starmap(find_matching_md5_multithreaded, chunks)

        # איסוף התוצאות מכל התהליכים
        final_results = []
        for process_results in results:
            for result in process_results:
                if result != 1:
                    final_results.append(result)

        if final_results:
            print(f"נמצא מספר מתאים: {final_results[0]}")
            final = str(final_results[0]).encode()
            answer = protocol.send_protocol(final)
            s.sendall(answer)
        else:
            print("לא נמצאה התאמה")
            num = 1
            final_not = str(num).encode()
            answer_1 = protocol.send_protocol(final_not)
            s.sendall(answer_1)
        logging.info("לקוח סיים")

if __name__ == "__main__":
    client()