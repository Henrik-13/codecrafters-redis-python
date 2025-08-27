import socket  # noqa: F401
import threading
import time

dictionary = {}
list_dict = {}
streams = {}


# def redis_protocol_encode(command):
#     print("Encoding command:", command)
#     parts = command.split()
#     result = ""
#     if len(parts) > 1:
#         result += f"*{len(parts)}\r\n"
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


def handle_rpush(connection, key, values):
    if key not in list_dict:
        list_dict[key] = []
    for value in values:
        list_dict[key].append(value)
    response = f":{len(list_dict[key])}\r\n"
    return connection.sendall(response.encode())


def handle_lrange(connection, key, start, end):
    start = int(start)
    end = int(end)
    if key not in list_dict or start >= len(list_dict[key]):
        return connection.sendall("*0\r\n".encode())
    lst = list_dict[key]

    if start < 0:
        start = len(lst) + start
    if end < 0:
        end = len(lst) + end

    start = max(0, min(start, len(lst)))
    end = max(-1, min(end, len(lst) - 1))

    selected_items = lst[start:end + 1]
    response = f"*{len(selected_items)}\r\n"

    for value in selected_items:
        response += f"${len(value)}\r\n{value}\r\n"

    return connection.sendall(response.encode())


def handle_lpush(connection, key, values):
    if key not in list_dict:
        list_dict[key] = []
    for value in values:
        list_dict[key].insert(0, value)
    response = f":{len(list_dict[key])}\r\n"
    return connection.sendall(response.encode())


def handle_llen(connection, key):
    length = len(list_dict.get(key, []))
    response = f":{length}\r\n"
    return connection.sendall(response.encode())


def handle_lpop(connection, args):
    key = args[0]
    count = 1
    if key not in list_dict or not list_dict[key]:
        return connection.sendall("$-1\r\n".encode())
    if len(args) > 1:
        count = int(args[1])

    deleted_items = []
    while count > 0 and list_dict[key]:
        deleted_items.append(list_dict[key].pop(0))
        count -= 1

    if not deleted_items:
        response = "$-1\r\n"
    elif len(deleted_items) == 1:
        response = f"${len(deleted_items[0])}\r\n{deleted_items[0]}\r\n"
    else:
        response = f"*{len(deleted_items)}\r\n"
        for item in deleted_items:
            response += f"${len(item)}\r\n{item}\r\n"

    return connection.sendall(response.encode())


def handle_blpop(connection, key, timeout):
    timeout = float(timeout)
    start_time = time.time() if timeout > 0 else None

    while True:
        if key in list_dict and list_dict[key]:
            value = list_dict[key].pop(0)
            response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
            return connection.sendall(response.encode())

        if timeout == 0:
            threading.Event().wait(0.1)
        else:
            if start_time and (time.time() - start_time) >= timeout:
                return connection.sendall(b"$-1\r\n")
            threading.Event().wait(0.1)


def handle_type(connection, key):
    if key in dictionary:
        response = "+string\r\n"
    elif key in list_dict:
        response = "+list\r\n"
    elif key in streams:
        response = "+stream\r\n"
    else:
        response = "+none\r\n"
    return connection.sendall(response.encode())


def parse_stream_id(id):
    # if id == "*":
    #     return None
    parts = id.split("-")
    if len(parts) != 2:
        return None, None
    try:
        timestamp = int(parts[0])
        sequence = parts[1]
        if sequence != "*":
            sequence = int(sequence)
        return timestamp, sequence
    except ValueError:
        return None, None


def validate_stream_id(stream_key, id):
    if stream_key not in streams or not streams[stream_key]:
        return True
    last_entry = streams[stream_key][-1]
    last_id = last_entry["id"]
    # if id == "*":
    #     return True
    new_ts, new_seq = parse_stream_id(id)
    last_ts, last_seq = parse_stream_id(last_id)
    if new_ts is None or last_ts is None:
        return False
    if (new_ts > last_ts) or (new_ts == last_ts and new_seq < last_seq):
        return False
    return new_ts > last_ts or (new_ts == last_ts and new_seq > last_seq)


def generate_stream_id():
    current_time = int(time.time() * 1000)
    return f"{current_time}-0"


def handle_xadd(connection, key, id, args):
    if len(args) % 2 != 0:
        return connection.sendall(b"-ERR wrong number of arguments for 'XADD' command\r\n")
    if id == "*":
        id = generate_stream_id()
    else:
        ts, seq = parse_stream_id(id)
        if ts and seq == "*":
            if key in streams and streams[key]:
                last_entry = streams[key][-1]
                last_id = last_entry["id"]
                last_ts, last_seq = parse_stream_id(last_id)
                # if last_ts is None or last_seq is None:
                #     return connection.sendall(b"-ERR The ID specified in XADD is invalid\r\n")
                # if ts < last_ts:
                #     return connection.sendall(
                #         b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
                if ts == 0 and not last_ts:
                    seq = 1
                else:
                    seq = last_seq + 1 if ts == last_ts else 0
        else:
            if ts is None or seq is None or ts < 0 or seq < 0 or (ts == 0 and seq == 0):
                return connection.sendall(b"-ERR The ID specified in XADD must be greater than 0-0\r\n")
            if not validate_stream_id(key, id):
                    return connection.sendall(
                        b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n")
    if key not in streams:
        streams[key] = []
    entry = {"id": id, "fields": {}}
    for i in range(0, len(args), 2):
        if i + 1 < len(args):
            entry["fields"][args[i]] = args[i + 1]
    streams[key].append(entry)
    response = f"${len(id)}\r\n{id}\r\n"
    return connection.sendall(response.encode())


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
        elif command[0].upper() == "RPUSH" and len(command) >= 3:
            handle_rpush(connection, command[1], command[2:])
        elif command[0].upper() == "LRANGE" and len(command) == 4:
            handle_lrange(connection, command[1], command[2], command[3])
        elif command[0].upper() == "LPUSH" and len(command) >= 3:
            handle_lpush(connection, command[1], command[2:])
        elif command[0].upper() == "LLEN" and len(command) == 2:
            handle_llen(connection, command[1])
        elif command[0].upper() == "LPOP" and len(command) >= 2:
            handle_lpop(connection, command[1:])
        elif command[0].upper() == "BLPOP" and len(command) == 3:
            handle_blpop(connection, command[1], command[2])
        elif command[0].upper() == "TYPE" and len(command) == 2:
            handle_type(connection, command[1])
        elif command[0].upper() == "XADD":
            handle_xadd(connection, command[1], command[2], command[3:])
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
