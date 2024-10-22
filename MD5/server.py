import socket
import protocol
import logging

HOST = '127.0.0.1'
PORT = 65432
CONNECTIONS = []


def client_handler(conn, addr):
    """
      Handles a single client connection. This function receives data from
      a connected client, processes it, and sends responses.

      Args:
          conn (socket.socket): The socket object representing the client connection.
          addr (tuple): A tuple containing the client's IP address and port number.
      """
    print(f"לקוח חדש התחבר: {addr}")
    CONNECTIONS.append(conn)  # מוסיף את החיבור לרשימת החיבורים


def server():
    logging.basicConfig(level=logging.DEBUG)
    logging.info("שרת התחיל")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()

        # מחכה עד שכל ארבעת הלקוחות יתחברו
        while len(CONNECTIONS) < 4:
            conn, addr = s.accept()
            client_handler(conn, addr)
            logging.info(f"לקוח חדש התחבר: {addr}")

        MESSAGE = "a20c3239525629140d97b1966bee713b"
        for conn in CONNECTIONS:
            msg = protocol.send_protocol(MESSAGE.encode())
            conn.sendall(msg)

        num_cores_list = []
        for conn in CONNECTIONS:
            try:
                num_cores = int(protocol.recv_protocol(conn).decode())
                # Assertion: Ensure received value is a positive integer
                assert num_cores > 0, "Invalid number of cores: must be positive"
                print(num_cores)
                num_cores_list.append(num_cores)
            except ValueError:
                logging.error("Received invalid number of cores from client")

        # Calculate total cores
        if len(num_cores_list) != 4:
            logging.error("Expected 4 clients, but received data from %d clients", len(num_cores_list))
        else:
            total_cores = sum(num_cores_list)

        numbers = [0, 25000000, 50000000, 75000000, 100000000]
        assert total_cores > 0, "Total cores cannot be zero"
        work_per_core = (numbers[-1] - numbers[0]) // total_cores

        start = 0
        for i, conn in enumerate(CONNECTIONS):
            end = start + num_cores_list[i] * work_per_core
            start_num = protocol.send_protocol(str(start).encode())
            conn.sendall(start_num)
            end_num = protocol.send_protocol(str(end).encode())
            conn.sendall(end_num)
            start = end

        for conn in CONNECTIONS:
            try:
                final = int(protocol.recv_protocol(conn).decode())
                assert final in (1, *numbers), "Invalid final result received from client"
                if final != 1:
                    print(final)
            except ValueError:
                logging.error("Received invalid final result from client")


        for conn in CONNECTIONS:
            conn.close()
        logging.info("Server finished")


if __name__ == "__main__":
    server()
