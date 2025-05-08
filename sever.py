import socket
import threading
import sys
from collections import defaultdict
import time


# 服务器端代码
class Server:
    def __init__(self, port):
        self.port = port
        self.tuple_space = {}
        self.total_clients = 0
        self.total_operations = 0
        self.total_reads = 0
        self.total_gets = 0
        self.total_puts = 0
        self.total_errors = 0
        self.lock = threading.Lock()

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(('0.0.0.0', self.port))
            server_socket.listen(5)
            print(f"Server started on port {self.port}")

            # 启动定期输出元组空间信息的线程
            threading.Thread(target=self.print_tuple_space_summary, daemon=True).start()

            while True:
                client_socket, client_address = server_socket.accept()
                self.total_clients += 1
                threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        with client_socket:
            while True:
                try:
                    request = client_socket.recv(1024).decode('utf-8')
                    if not request:
                        break
                    command = request[3:4]
                    key = request[5:]
                    value = ""
                    if command == 'P':
                        key, value = key.split(' ', 1)

                    response = ""
                    self.total_operations += 1
                    if command == 'R':
                        self.total_reads += 1
                        response = self.handle_read(key)
                    elif command == 'G':
                        self.total_gets += 1
                        response = self.handle_get(key)
                    elif command == 'P':
                        self.total_puts += 1
                        response = self.handle_put(key, value)
                    else:
                        self.total_errors += 1
                        response = f"024 ERR invalid command"

                    client_socket.send(response.encode('utf-8'))
                except Exception as e:
                    print(f"Error handling client: {e}")
                    break

    def handle_read(self, key):
        with self.lock:
            if key in self.tuple_space:
                value = self.tuple_space[key]
                return f"{len(f'OK ({key}, {value}) read'):03d} OK ({key}, {value}) read"
            else:
                self.total_errors += 1
                return f"024 ERR {key} does not exist"

    def handle_get(self, key):
        with self.lock:
            if key in self.tuple_space:
                value = self.tuple_space.pop(key)
                return f"{len(f'OK ({key}, {value}) removed'):03d} OK ({key}, {value}) removed"
            else:
                self.total_errors += 1
                return f"024 ERR {key} does not exist"

    def handle_put(self, key, value):
        with self.lock:
            if key in self.tuple_space:
                self.total_errors += 1
                return f"024 ERR {key} already exists"
            else:
                self.tuple_space[key] = value
                return f"{len(f'OK ({key}, {value}) added'):03d} OK ({key}, {value}) added"

    def print_tuple_space_summary(self):
        while True:
            time.sleep(10)
            with self.lock:
                tuple_count = len(self.tuple_space)
                if tuple_count == 0:
                    print("Tuple space is empty.")
                    continue

                total_tuple_size = 0
                total_key_size = 0
                total_value_size = 0
                for key, value in self.tuple_space.items():
                    total_tuple_size += len(key) + len(value)
                    total_key_size += len(key)
                    total_value_size += len(value)

                average_tuple_size = total_tuple_size / tuple_count if tuple_count > 0 else 0
                average_key_size = total_key_size / tuple_count if tuple_count > 0 else 0
                average_value_size = total_value_size / tuple_count if tuple_count > 0 else 0

                print("Tuple Space Summary:")
                print(f"Number of tuples: {tuple_count}")
                print(f"Average tuple size: {average_tuple_size}")
                print(f"Average key size: {average_key_size}")
                print(f"Average value size: {average_value_size}")
                print(f"Total number of clients: {self.total_clients}")
                print(f"Total number of operations: {self.total_operations}")
                print(f"Total READs: {self.total_reads}")
                print(f"Total GETs: {self.total_gets}")
                print(f"Total PUTs: {self.total_puts}")
                print(f"Total errors: {self.total_errors}")


# 客户端代码
def client(server_host, server_port, request_file_path):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((server_host, server_port))
        with open(request_file_path, 'r') as request_file:
            for line in request_file:
                line = line.strip()
                request = build_request(line)
                if not request:
                    print(f"Invalid request: {line}")
                    continue

                client_socket.send(request.encode('utf-8'))
                response = client_socket.recv(1024).decode('utf-8')
                print(f"{line}: {response}")


def build_request(line):
    parts = line.split(' ')
    if len(parts) < 2 or len(parts) > 3:
        return None
    command = parts[0]
    key = parts[1]
    value = "" if len(parts) == 2 else parts[2]
    if len(command) != 3:
        return None
    if command == 'PUT':
        request = f"{7 + len(key) + len(value):03d} {command} {key} {value}"
    else:
        request = f"{7 + len(key):03d} {command} {key}"
    if len(request) > 999:
        return None
    return request


if __name__ == "__main__":
    if len(sys.argv) == 2:
        try:
            port = int(sys.argv[1])
            if port < 0 or port > 65535:
                print("Error: The provided port is out of valid range (0 - 65535).")
            elif port < 1024:
                print("Warning: Ports below 1024 usually require administrative privileges.")
                server = Server(port)
                server.start()
            else:
                server = Server(port)
                server.start()
        except ValueError:
            print("Error: The provided port is not a valid integer.")
    elif len(sys.argv) == 4:
        client(sys.argv[1], int(sys.argv[2]), sys.argv[3])
    else:
        print("Usage: python server.py <port> or python client.py <server_host> <server_port> <request_file>")
