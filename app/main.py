import socket
import threading


def handle_client(connection):
    try:
        while True:
            data = connection.recv(1024)
            if not data:
                break
            
            # Kita potong-potong pesannya berdasarkan tanda Enter (\r\n)
            # Pesan akan jadi daftar kata: ['*2', '$4', 'ECHO', '$3', 'hey', '']
            parts = data.decode().split("\r\n")
            
            # Bagian [2] adalah perintahnya (PING atau ECHO)
            command = parts[2].upper()
            
            if command == "PING":
                connection.sendall(b"+PONG\r\n")
            elif command == "ECHO":
                # Bagian [4] adalah kata yang mau dibalas (misal: 'hey')
                payload = parts[4]
                # Kita susun jawabannya pakai format: $panjang_kata\r\nkata\r\n
                response = f"${len(payload)}\r\n{payload}\r\n"
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
