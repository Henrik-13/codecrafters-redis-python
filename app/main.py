import socket  # noqa: F401
import threading

dictionary = {}

# def redis_protocol_encode(command):
#     print("Encoding command:", command)
#     parts = command.split()
#     result = ""
#     for part in parts:
#         result += f"${len(part)}\r\n{part}\r\n"
#     return result


def redis_protocol_decode(data):
    lines = data.split("\r\n")
    if not lines or lines[0] == "":
        return None

    if lines[0][0] != "*":
        return None

    try:
        num_elements = int(lines[0][1:])
    except ValueError:
        return None

    elements = []
    index = 1
    for _ in range(num_elements):
        if index >= len(lines) or lines[index][0] != "$":
            return None
        try:
            length = int(lines[index][1:])
        except ValueError:
            return None
        index += 1
        if index >= len(lines) or len(lines[index]) != length:
            return None
        elements.append(lines[index])
        index += 1

    return elements if len(elements) == num_elements else None


def handle_ping(connection):
    return connection.sendall(b"+PONG\r\n")


def handle_echo(connection, message):
    response = f"${len(message)}\r\n{message}\r\n"
    print("Sending response:", response)
    return connection.sendall(response.encode())


def handle_set(connection, args):
    key = args[0]
    value = args[1]
    dictionary[key] = value
    if len(args) > 2 and args[2].upper() == "PX":
        try:
            expire_time = int(args[3]) / 1000.0
            threading.Timer(expire_time, lambda: dictionary.pop(key, None)).start()
        except (ValueError, IndexError):
            return connection.sendall(b"-ERR invalid PX value\r\n")
    return connection.sendall(b"+OK\r\n")

def handle_get(connection, key):
    if key in dictionary:
        value = dictionary[key]
        response = f"${len(value)}\r\n{value}\r\n"
        return connection.sendall(response.encode())
    else:
        return connection.sendall(b"$-1\r\n")


def send_response(connection):
    while True:
        data = connection.recv(1024)
        if not data:
            break

        command = redis_protocol_decode(data.decode())
        print(f"Received command: {command}")
        if command[0].upper() == "PING":
            handle_ping(connection)
        elif command[0].upper() == "ECHO" and len(command) == 2:
            handle_echo(connection, command[1])
        elif command[0].upper() == "SET" and len(command) >= 3:
            handle_set(connection, command[1:])
        elif command[0].upper() == "GET" and len(command) == 2:
            handle_get(connection, command[1])
        else:
            connection.sendall(b"-ERR unknown command\r\n")
    connection.close()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()  # wait for client
        thread = threading.Thread(target=send_response, args=(connection,))
        thread.start()


if __name__ == "__main__":
    main()
