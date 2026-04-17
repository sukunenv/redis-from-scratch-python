import socket
import threading

# Ini adalah "Gudang Data" kita (Dictionary)
DATA_STORE = {}


def handle_client(connection):
    try:
        while True:
            data = connection.recv(1024)
            if not data:
                break
            
            # Memecah pesan menjadi bagian-bagian
            parts = data.decode().split("\r\n")
            command = parts[2].upper()
            
            if command == "PING":
                connection.sendall(b"+PONG\r\n")
                
            elif command == "ECHO":
                # ECHO <pesan> (pesan ada di bagian ke-4)
                payload = parts[4]
                response = f"${len(payload)}\r\n{payload}\r\n"
                connection.sendall(response.encode())
                
            elif command == "SET":
                # SET <kunci> <nilai>
                # kunci ada di bagian ke-4, nilai ada di bagian ke-6
                key = parts[4]
                value = parts[6]
                DATA_STORE[key] = value
                connection.sendall(b"+OK\r\n")
                
            elif command == "GET":
                # GET <kunci> (kunci ada di bagian ke-4)
                key = parts[4]
                # Mencari kunci di gudang
                value = DATA_STORE.get(key)
                if value:
                    response = f"${len(value)}\r\n{value}\r\n"
                else:
                    # Jika tidak ketemu, balas dengan NULL (format Redis: $-1\r\n)
                    response = "$-1\r\n"
                connection.sendall(response.encode())
                
    except Exception:
        pass
    finally:
        connection.close()


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    while True:
        connection, _ = server_socket.accept()
        thread = threading.Thread(target=handle_client, args=(connection,))
        thread.start()


if __name__ == "__main__":
    main()
