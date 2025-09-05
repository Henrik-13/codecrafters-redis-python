import socket  # noqa: F401
import threading
import time
import argparse

parser = argparse.ArgumentParser()

dictionary = {}
list_dict = {}
streams = {}
connections = {}

replica_of = None
replicas = []
write_commands = {"SET", "DEL", "INCR", "DECR", "RPUSH", "LPUSH", "LPOP", "XADD"}


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
                return connection.sendall(b"*-1\r\n")
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


def generate_next_stream_id(key, ts):
    if key in streams and streams[key]:
        last_entry = streams[key][-1]
        last_id = last_entry["id"]
        last_ts, last_seq = parse_stream_id(last_id)
        seq = last_seq + 1 if ts == last_ts else 0
    else:
        seq = 1 if ts == 0 else 0
    return f"{ts}-{seq}"


def handle_xadd(connection, key, id, args):
    if len(args) % 2 != 0:
        return connection.sendall(b"-ERR wrong number of arguments for 'XADD' command\r\n")
    if id == "*":
        id = generate_stream_id()
    else:
        ts, seq = parse_stream_id(id)
        if ts is not None and seq == "*":
            id = generate_next_stream_id(key, ts)
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


def compare_stream_ids(id1, id2):
    ts1, seq1 = parse_stream_id(id1)
    ts2, seq2 = parse_stream_id(id2)
    if ts1 < ts2:
        return -1
    elif ts1 > ts2:
        return 1
    else:
        if seq1 < seq2:
            return -1
        elif seq1 > seq2:
            return 1
        else:
            return 0


def handle_xrange(connection, key, start, end):
    if key not in streams or not streams[key]:
        return connection.sendall(b"*0\r\n")

    if start == "-":
        start = streams[key][0]["id"]

    if end == "+":
        end = streams[key][-1]["id"]

    filtered_entries = []
    for entry in streams[key]:
        entry_id = entry["id"]
        if compare_stream_ids(start, entry_id) <= 0 and compare_stream_ids(entry_id, end) <= 0:
            filtered_entries.append(entry)

    if not filtered_entries:
        return connection.sendall(b"*0\r\n")

    response = f"*{len(filtered_entries)}\r\n"
    for entry in filtered_entries:
        response += f"*2\r\n${len(entry['id'])}\r\n{entry['id']}\r\n"
        fields = entry["fields"]
        response += f"*{len(fields) * 2}\r\n"
        for field, value in fields.items():
            response += f"${len(field)}\r\n{field}\r\n${len(value)}\r\n{value}\r\n"

    return connection.sendall(response.encode())


def handle_xread(connection, args, block=None):
    if len(args) < 2 or len(args) % 2 != 0:
        return connection.sendall(b"-ERR wrong number of arguments for 'XREAD' command\r\n")
    num_streams = len(args) // 2
    start_time = time.time() if block is not None else None

    processed_ids = []
    for i in range(num_streams):
        key = args[i]
        id = args[i + num_streams]

        if id == "$":
            if key in streams and streams[key]:
                last_entry = streams[key][-1]
                id = last_entry["id"]
            else:
                id = "0-0"
        processed_ids.append(id)

    while True:
        has_results = False
        results = []

        for i in range(num_streams):
            key = args[i]
            id = processed_ids[i]
            if key not in streams or not streams[key]:
                continue

            filtered_entries = []
            for entry in streams[key]:
                entry_id = entry["id"]
                if compare_stream_ids(entry_id, id) > 0:
                    filtered_entries.append(entry)

            if filtered_entries:
                has_results = True
                results.append((key, filtered_entries))

        if has_results:
            response = f"*{len(results)}\r\n"
            for key, entries in results:
                response += f"*2\r\n${len(key)}\r\n{key}\r\n*{len(entries)}\r\n"
                for entry in entries:
                    response += f"*2\r\n${len(entry['id'])}\r\n{entry['id']}\r\n"
                    fields = entry["fields"]
                    response += f"*{len(fields) * 2}\r\n"
                    for field, value in fields.items():
                        response += f"${len(field)}\r\n{field}\r\n${len(value)}\r\n{value}\r\n"
            return connection.sendall(response.encode())

        if block is None:
            return connection.sendall(b"*0\r\n")

        if 0 < block <= (time.time() - start_time) and start_time:
            return connection.sendall(b"*-1\r\n")

        threading.Event().wait(0.1)


def handle_incr(connection, key):
    if key not in dictionary:
        dictionary[key] = "1"
    else:
        try:
            dictionary[key] = int(dictionary[key]) + 1
        except ValueError:
            return connection.sendall(b"-ERR value is not an integer or out of range\r\n")

    dictionary[key] = str(dictionary[key])
    return connection.sendall(f":{dictionary[key]}\r\n".encode())


def handle_multi(connection):
    conn_id = id(connection)
    connections[conn_id] = {'in_transaction': True, 'commands': []}
    return connection.sendall(b"+OK\r\n")


def handle_exec(connection):
    conn_id = id(connection)
    if conn_id not in connections or not connections[conn_id].get('in_transaction'):
        return connection.sendall(b"-ERR EXEC without MULTI\r\n")

    commands = connections[conn_id]['commands']
    connection.sendall(f"*{len(commands)}\r\n".encode())
    for command in commands:
        execute_command(connection, command)
    del connections[conn_id]
    return None


def queue_command(connection, command):
    conn_id = id(connection)
    if conn_id in connections and connections[conn_id].get('in_transaction'):
        connections[conn_id]['commands'].append(command)
        connection.sendall(b"+QUEUED\r\n")
        return True
    return False


def handle_discard(connection):
    conn_id = id(connection)
    if conn_id not in connections or not connections[conn_id].get('in_transaction'):
        return connection.sendall(b"-ERR DISCARD without MULTI\r\n")
    del connections[conn_id]
    return connection.sendall(b"+OK\r\n")


def handle_info(connection, section=None):
    if section and section.upper() == "REPLICATION":
        role = "slave" if replica_of else "master"
        response = f"role:{role}\r\n"
        response += f"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
        response += f"master_repl_offset:0\r\n"
        return connection.sendall(f"${len(response)}\r\n{response}\r\n".encode())
    else:
        return connection.sendall(b"-ERR unsupported INFO section\r\n")


def handle_psync(connection, args):
    connection.sendall(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")
    empty_rdb_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    rdb_file_encoded = bytes.fromhex(empty_rdb_file)
    connection.sendall(f"${len(rdb_file_encoded)}\r\n".encode() + rdb_file_encoded)

    replicas.append(connection)


def propagate_to_replicas(command):
    for replica in replicas[:]:
        try:
            replica.sendall(command)
        except Exception:
            replicas.remove(replica)

def execute_command(connection, command):
    cmd = command[0].upper() if command else None

    if cmd == "PING":
        handle_ping(connection)
    elif cmd == "ECHO" and len(command) == 2:
        handle_echo(connection, command[1])
    elif cmd == "SET" and len(command) >= 3:
        handle_set(connection, command[1:])
    elif cmd == "GET" and len(command) == 2:
        handle_get(connection, command[1])
    elif cmd == "RPUSH" and len(command) >= 3:
        handle_rpush(connection, command[1], command[2:])
    elif cmd == "LRANGE" and len(command) == 4:
        handle_lrange(connection, command[1], command[2], command[3])
    elif cmd == "LPUSH" and len(command) >= 3:
        handle_lpush(connection, command[1], command[2:])
    elif cmd == "LLEN" and len(command) == 2:
        handle_llen(connection, command[1])
    elif cmd == "LPOP" and len(command) >= 2:
        handle_lpop(connection, command[1:])
    elif cmd == "BLPOP" and len(command) == 3:
        handle_blpop(connection, command[1], command[2])
    elif cmd == "TYPE" and len(command) == 2:
        handle_type(connection, command[1])
    elif cmd == "XADD" and len(command) >= 4:
        handle_xadd(connection, command[1], command[2], command[3:])
    elif cmd == "XRANGE" and len(command) == 4:
        handle_xrange(connection, command[1], command[2], command[3])
    elif cmd == "XREAD" and len(command) >= 4:
        if command[1].upper() == "BLOCK":
            try:
                block_time = int(command[2]) / 1000.0
                handle_xread(connection, command[4:], block=block_time)
            except ValueError:
                connection.sendall(b"-ERR invalid BLOCK value\r\n")
        else:
            handle_xread(connection, command[2:], )
    elif cmd == "INCR" and len(command) == 2:
        handle_incr(connection, command[1])
    elif cmd == "INFO" and len(command) >= 1:
        section = command[1] if len(command) == 2 else None
        handle_info(connection, section)
    elif cmd == "REPLCONF":
        connection.sendall(b"+OK\r\n")
    elif cmd == "PSYNC" and len(command) == 3:
        handle_psync(connection, command[1:])
    else:
        connection.sendall(b"-ERR unknown command\r\n")


def send_response(connection):
    try:
        while True:
            data = connection.recv(1024)
            if not data:
                break

            command = redis_protocol_decode(data.decode())
            print(f"Received command: {command}")
            cmd = command[0].upper() if command else None

            if cmd == "MULTI" and len(command) == 1:
                handle_multi(connection)
                continue
            elif cmd == "EXEC" and len(command) == 1:
                handle_exec(connection)
                continue
            elif cmd == "DISCARD" and len(command) == 1:
                handle_discard(connection)
                continue
            elif queue_command(connection, command):
                continue
            else:
                execute_command(connection, command)

                if not replica_of and cmd in write_commands:
                    propagate_to_replicas(data)
    finally:
        conn_id = id(connection)
        if conn_id in connections:
            del connections[conn_id]
        if connection in replicas:
            replicas.remove(connection)
        connection.close()


def connect_to_master(host, port, replica_port):
    try:
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((host, port))

        ping_command = b"*1\r\n$4\r\nPING\r\n"
        master_socket.sendall(ping_command)
        response = master_socket.recv(1024)
        if response != b"+PONG\r\n":
            print("Failed to receive PONG from master")
            master_socket.close()
            return None
        replconf_command = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(str(replica_port))}\r\n{replica_port}\r\n"
        master_socket.sendall(replconf_command.encode())
        response = master_socket.recv(1024)
        if response != b"+OK\r\n":
            print("Failed to receive OK from master for REPLCONF")
            master_socket.close()
            return None

        replconf_command = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        master_socket.sendall(replconf_command)
        response = master_socket.recv(1024)
        if response != b"+OK\r\n":
            print("Failed to receive OK from master for REPLCONF capa")
            master_socket.close()
            return None

        psync_command = b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
        master_socket.sendall(psync_command)
        response = master_socket.recv(1024)
        if not response.startswith(b"+FULLRESYNC"):
            print("Failed to receive FULLRESYNC from master")
            master_socket.close()
            return None

        print(f"Connected to master at {host}:{port}")
        return master_socket
    except Exception as e:
        print(f"Failed to connect to master at {host}:{port}: {e}")
        return None


def main(args):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    global replica_of
    if args.replicaof:
        replica_of = args.replicaof
        master_host, master_port = args.replicaof.split()

        master_connection = connect_to_master(master_host, int(master_port), args.port)
        if master_connection:
            print("Connected to master at {}:{}".format(master_host, master_port))
        else:
            print("Failed to connect to master at {}:{}".format(master_host, master_port))

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", int(args.port)), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()  # wait for client
        thread = threading.Thread(target=send_response, args=(connection,))
        thread.start()


if __name__ == "__main__":
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on")
    parser.add_argument("--replicaof", type=str, help="Replication source in host port format")
    args = parser.parse_args()
    main(args)
