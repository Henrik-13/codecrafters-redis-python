import socket  # noqa: F401
import threading
import time
import argparse

from app.command_parser import CommandParser
from app.rdb_parser import RDBParser
from app.stores.list_store import ListStore
from app.stores.stream_store import StreamStore
from app.stores.string_store import StringStore
from app.stores.sorted_set_store import SortedSetStore
from app.geohash import encode as encode_geohash, decode as decode_geohash, haversine

parser = argparse.ArgumentParser()
command_parser = CommandParser()

string_store = StringStore()
list_store = ListStore()
stream_store = StreamStore()
sorted_set_store = SortedSetStore()

connections = {}
connections_lock = threading.Lock()
subscriptions = {}
subscriptions_lock = threading.Lock()

master_connection_socket = None
replica_of = None
replicas = []
replicas_lock = threading.Lock()
replica_offsets = {} # Maps replica socket to its last known offset
replica_offsets_lock = threading.Lock()
master_repl_offset = 0
master_repl_offset_lock = threading.Lock()
replica_offset = 0
write_commands = {"SET", "DEL", "INCR", "DECR", "RPUSH", "LPUSH", "LPOP", "XADD", "ZADD"}

dir = None
dbfilename = None


def handle_ping(connection, command):
    if len(command) != 0:
        return connection.sendall(b"-ERR wrong number of arguments for 'PING' command\r\n")
    if connection == master_connection_socket:
        return None
    with subscriptions_lock:
        is_subscribed = connection in subscriptions and subscriptions[connection]
    if is_subscribed:
        return connection.sendall(b"*2\r\n$4\r\npong\r\n$0\r\n\r\n")
    return connection.sendall(b"+PONG\r\n")

def handle_echo(connection, command):
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'ECHO' command\r\n")
    message = command[0]
    response = f"${len(message)}\r\n{message}\r\n"
    return connection.sendall(response.encode())


def handle_set(connection, command):
    if len(command) < 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'SET' command\r\n")
    args = command
    key = args[0]
    value = args[1]
    px = None
    if len(args) > 2 and args[2].upper() == "PX":
        if len(args) < 4:
            return connection.sendall(b"-ERR syntax error\r\n")
        px = int(args[3])
    
    string_store.set(key, value, px)
    if connection != master_connection_socket:
        connection.sendall(b"+OK\r\n")


def handle_get(connection, command):
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'GET' command\r\n")
    key = command[0]
    value = string_store.get(key)
    if value is None:
        connection.sendall(b"$-1\r\n")
    else:
        connection.sendall(f"${len(value)}\r\n{value}\r\n".encode())


def handle_rpush(connection, command):
    if len(command) < 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'RPUSH' command\r\n")
    key, values = command[0], command[1:]
    count = list_store.rpush(key, values)
    connection.sendall(f":{count}\r\n".encode())


def handle_lrange(connection, command):
    if len(command) != 3:
        return connection.sendall(b"-ERR wrong number of arguments for 'LRANGE' command\r\n")
    key, start, end = command[0], command[1], command[2]
    try:
        start, end = int(start), int(end)
    except ValueError:
        return connection.sendall(b"-ERR value is not an integer or out of range\r\n")
    
    items = list_store.lrange(key, start, end)
    response = f"*{len(items)}\r\n"
    for item in items:
        response += f"${len(item)}\r\n{item}\r\n"
    connection.sendall(response.encode())


def handle_lpush(connection, command):
    if len(command) < 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'LPUSH' command\r\n")
    key, values = command[0], command[1:]
    count = list_store.lpush(key, values)
    connection.sendall(f":{count}\r\n".encode())


def handle_llen(connection, command):
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'LLEN' command\r\n")
    key = command[0]
    count = list_store.llen(key)
    connection.sendall(f":{count}\r\n".encode())


def handle_lpop(connection, command):
    if len(command) < 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'LPOP' command\r\n")
    key = command[0]
    count = 1
    if len(command) > 1:
        try:
            count = int(command[1])
        except ValueError:
            return connection.sendall(b"-ERR value is not an integer or out of range\r\n")

    items = list_store.lpop(key, count)
    if not items:
        return connection.sendall(b"$-1\r\n")
    if len(items) == 1:
        response = f"${len(items[0])}\r\n{items[0]}\r\n"
    else:
        response = f"*{len(items)}\r\n"
        for item in items:
            response += f"${len(item)}\r\n{item}\r\n"
    return connection.sendall(response.encode())



def handle_blpop(connection, command):
    if len(command) != 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'BLPOP' command\r\n")
    key, timeout = command[0], command[1]
    timeout = float(timeout)
    start_time = time.time() if timeout > 0 else None

    while True:
        popped = list_store.lpop(key, 1)
        if popped:
            value = popped[0]
            response = f"*2\r\n${len(key)}\r\n{key}\r\n${len(value)}\r\n{value}\r\n"
            return connection.sendall(response.encode())
        
        if timeout is not None and start_time and (time.time() - start_time) >= timeout:
            return connection.sendall(b"*-1\r\n")
        threading.Event().wait(0.1)


def handle_type(connection, command):
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'TYPE' command\r\n")
    key = command[0]
    if string_store.get(key) is not None:
        return connection.sendall(b"+string\r\n")
    if list_store.exists(key):
        return connection.sendall(b"+list\r\n")
    if stream_store.exists(key):
        return connection.sendall(b"+stream\r\n")
    if sorted_set_store.exists(key):
        return connection.sendall(b"+zset\r\n")
    
    return connection.sendall(b"+none\r\n")


def handle_xadd(connection, command):
    if len(command) < 3:
        return connection.sendall(b"-ERR wrong number of arguments for 'XADD' command\r\n")
    key, id, args = command[0], command[1], command[2:]
    if len(args) % 2 != 0:
        return connection.sendall(b"-ERR wrong number of arguments for 'XADD' command\r\n")

    fields_dict = {args[i]: args[i + 1] for i in range(0, len(args), 2)}
    try:
        new_id = stream_store.xadd(key, id, fields_dict)
        return connection.sendall(f"${len(new_id)}\r\n{new_id}\r\n".encode())
    except ValueError as e:
        return connection.sendall(f"-ERR {str(e)}\r\n".encode())


def handle_xrange(connection, command):
    if len(command) != 3:
        return connection.sendall(b"-ERR wrong number of arguments for 'XRANGE' command\r\n")
    key, start, end = command[0], command[1], command[2]
    entries = stream_store.xrange(key, start, end)
    if not entries:
        return connection.sendall(b"*0\r\n")

    response = f"*{len(entries)}\r\n"
    for entry in entries:
        response += f"*2\r\n${len(entry['id'])}\r\n{entry['id']}\r\n"
        fields = entry["fields"]
        response += f"*{len(fields) * 2}\r\n"
        for field, value in fields.items():
            response += f"${len(field)}\r\n{field}\r\n${len(value)}\r\n{value}\r\n"

    return connection.sendall(response.encode())


def handle_xread(connection, command):
    print(command)
    block=None
    if command[0].upper() == "BLOCK":
        try:
            block = int(command[1]) / 1000.0
            command = command[2:]
            # handle_xread(connection, command[3:], block=block)
        except ValueError:
            connection.sendall(b"-ERR invalid BLOCK value\r\n")
    command = command[1:]
    if len(command) < 2 or len(command) % 2 != 0:
        return connection.sendall(b"-ERR wrong number of arguments for 'XREAD' command\r\n")
    num_streams = len(command) // 2
    start_time = time.time() if block is not None else None

    streams_to_read = {command[i]: command[i + num_streams] for i in range(num_streams)}
    for key, stream_id in streams_to_read.items():
        if stream_id == "$":
            streams_to_read[key] = stream_store.get_last_id(key)

    while True:
        results = stream_store.xread(streams_to_read)
        if results:
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


def handle_incr(connection, command):
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'INCR' command\r\n")
    key = command[0]
    try:
        value = string_store.incr(key)
    except ValueError:
        return connection.sendall(b"-ERR value is not an integer or out of range\r\n")
    if value is not None:
        return connection.sendall(f":{value}\r\n".encode())


def handle_multi(connection):
    conn_id = id(connection)
    with connections_lock:
        connections[conn_id] = {'in_transaction': True, 'commands': []}
    return connection.sendall(b"+OK\r\n")


def handle_exec(connection):
    conn_id = id(connection)
    with connections_lock:
        if conn_id not in connections or not connections[conn_id].get('in_transaction'):
            return connection.sendall(b"-ERR EXEC without MULTI\r\n")

        commands = connections[conn_id]['commands']
        del connections[conn_id]
    
    connection.sendall(f"*{len(commands)}\r\n".encode())
    for command in commands:
        try:
            execute_command(connection, command)
        except Exception as e:
            connection.sendall(f"-ERR {str(e)}\r\n".encode())
    return None


def queue_command(connection, command):
    conn_id = id(connection)
    with connections_lock:
        if conn_id in connections and connections[conn_id].get('in_transaction'):
            connections[conn_id]['commands'].append(command)
            connection.sendall(b"+QUEUED\r\n")
            return True
    return False


def handle_discard(connection):
    conn_id = id(connection)
    with connections_lock:
        if conn_id not in connections or not connections[conn_id].get('in_transaction'):
            return connection.sendall(b"-ERR DISCARD without MULTI\r\n")
        del connections[conn_id]
    return connection.sendall(b"+OK\r\n")


def handle_info(connection, command):
    section = command[0].upper() if len(command) > 0 else None
    if section == "REPLICATION":
        role = "slave" if replica_of else "master"
        response = f"role:{role}\r\n"
        response += f"master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\r\n"
        response += f"master_repl_offset:{master_repl_offset}\r\n"
        return connection.sendall(f"${len(response)}\r\n{response}\r\n".encode())
    else:
        return connection.sendall(b"-ERR unsupported INFO section\r\n")


def handle_psync(connection, command):
    if len(command) != 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'PSYNC' command\r\n")
    connection.sendall(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")
    empty_rdb_file = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
    rdb_file_encoded = bytes.fromhex(empty_rdb_file)
    connection.sendall(f"${len(rdb_file_encoded)}\r\n".encode() + rdb_file_encoded)

    with replicas_lock:
        replicas.append(connection)


def propagate_to_replicas(command_array):
    global master_repl_offset
    encoded_command = f"*{len(command_array)}\r\n"
    for arg in command_array:
        encoded_command += f"${len(arg)}\r\n{arg}\r\n"

    encoded_bytes = encoded_command.encode()
    with master_repl_offset_lock:
        master_repl_offset += len(encoded_bytes)

    with replicas_lock:
        current_replicas = list(replicas)

    for replica in current_replicas:
        try:
            replica.sendall(encoded_bytes)
        except Exception:
            print(f"Failed to propagate to replica, removing.")
            with replicas_lock:
                if replica in replicas:
                    replicas.remove(replica)
            with replica_offsets_lock:
                if replica in replica_offsets:
                    del replica_offsets[replica]


def handle_replconf(connection, command):
    if len(command) >= 1 and command[0].upper() == "GETACK":
        offset_str = str(replica_offset)
        connection.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(offset_str)}\r\n{offset_str}\r\n".encode())
    elif len(command) >= 2 and command[0].upper() == "ACK":
        with replica_offsets_lock:
            replica_offsets[connection] = int(command[1])
    else:
        connection.sendall(b"+OK\r\n")


def handle_wait(connection, command):
    if len(command) != 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'WAIT' command\r\n")
    num_replicas, timeout = command[0], command[1]
    try:
        num_replicas = int(num_replicas)
        timeout = int(timeout)
    except ValueError:
        return connection.sendall(b"-ERR invalid WAIT arguments\r\n")
    
    with master_repl_offset_lock:
        current_master_offset = master_repl_offset
    
    if current_master_offset == 0:
        with replicas_lock:
            num_connected_replicas = len(replicas)
        return connection.sendall(f":{num_connected_replicas}\r\n".encode())

    getack_command = ["REPLCONF", "GETACK", "*"]
    propagate_to_replicas(getack_command)
    
    start_time = time.time()
    acked_replicas = 0

    while True:
        with replica_offsets_lock:
            current_acks = sum(1 for offset in replica_offsets.values() if offset >= current_master_offset)
        if current_acks >= num_replicas:
            acked_replicas = current_acks
            break

        if (time.time() - start_time) * 1000 >= timeout and timeout != 0:
            with replica_offsets_lock:
                acked_replicas = sum(1 for offset in replica_offsets.values() if offset >= current_master_offset)
            break

        threading.Event().wait(0.01)

    connection.sendall(f":{acked_replicas}\r\n".encode())


def handle_config(connection, command):
    if len(command) < 2 or command[0].upper() != "GET":
        return connection.sendall(b"-ERR syntax error\r\n")
    
    param = command[1]
    if param == "dir":
        response = f"*2\r\n$3\r\ndir\r\n${len(dir)}\r\n{dir}\r\n"
        return connection.sendall(response.encode())
    elif param == "dbfilename":
        response = f"*2\r\n$10\r\ndbfilename\r\n${len(dbfilename)}\r\n{dbfilename}\r\n"
        return connection.sendall(response.encode())
    else:
        return connection.sendall(b"-ERR unknown CONFIG GET parameter\r\n")


def handle_keys(connection, command):
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'KEYS' command\r\n")
    pattern = command[0]
    if pattern == "*":
        keys = string_store.keys()
        response = f"*{len(keys)}\r\n"
        for key in keys:
            response += f"${len(key)}\r\n{key}\r\n"
        return connection.sendall(response.encode())
    else:
        return connection.sendall(b"*0\r\n")


def handle_subscribe(connection, command):
    print(command)
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'SUBSCRIBE' command\r\n")
    channel = command[0]
    print(f"Subscribing to channel: {channel}")
    with subscriptions_lock:
        if connection not in subscriptions:
            subscriptions[connection] = set()
        if channel not in subscriptions[connection]:
            subscriptions[connection].add(channel)
    response = f"*3\r\n$9\r\nsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{len(subscriptions[connection])}\r\n"
    return connection.sendall(response.encode())


def handle_unsubscribe(connection, channel):
    if not channel:
        return connection.sendall(b"-ERR wrong number of arguments for 'UNSUBSCRIBE' command\r\n")
    with subscriptions_lock:
        if connection in subscriptions and channel in subscriptions[connection]:
            subscriptions[connection].remove(channel)
            response = f"*3\r\n$11\r\nunsubscribe\r\n${len(channel)}\r\n{channel}\r\n:{len(subscriptions[connection])}\r\n"
            connection.sendall(response.encode())
        if not subscriptions[connection]:
            del subscriptions[connection]
            return


def enter_subscription_mode(connection):
    while True:
        try:
            data = connection.recv(1024)
            if not data:
                break  # Connection closed
            # Ignore any commands while in subscription mode
            commands_with_bytes, _ = command_parser.parse_commands(data)

            # If no full commands could be parsed, we need more data
            if not commands_with_bytes:
                continue
            for command, _ in commands_with_bytes:
                cmd = command[0].upper() if command else None
                if cmd == "SUBSCRIBE":
                    handle_subscribe(connection, command[1:])
                elif cmd == "UNSUBSCRIBE" and len(command) == 2:
                    handle_unsubscribe(connection, command[1])
                elif cmd == "PING":
                    handle_ping(connection, command[1:])
                elif cmd == "PSUBSCRIBE" or cmd == "PUNSUBSCRIBE":
                    raise NotImplementedError
                elif cmd == "QUIT":
                    return
                else:
                    response = f"-ERR Can't execute '{cmd.lower()}' in subscribed mode\r\n"
                    connection.sendall(response.encode())

        except Exception:
            break
    with subscriptions_lock:
        if connection in subscriptions:
            del subscriptions[connection]
    with replicas_lock:
        if connection in replicas:
            replicas.remove(connection)
    with replica_offsets_lock:
        if connection in replica_offsets:
            del replica_offsets[connection]
    connection.close()


def handle_publish(connection, command):
    if len(command) != 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'PUBLISH' command\r\n")
    channel, message = command[0], command[1]
    subscriber_count = 0
    with subscriptions_lock:
        for conn, channels in subscriptions.items():
            if channel in channels:
                response = f"*3\r\n$7\r\nmessage\r\n${len(channel)}\r\n{channel}\r\n${len(message)}\r\n{message}\r\n"
                try:
                    conn.sendall(response.encode())
                    subscriber_count += 1
                except Exception:
                    pass  # Ignore failures to send
    return connection.sendall(f":{subscriber_count}\r\n".encode())


def handle_zadd(connection, command):
    if len(command) < 3:
        return connection.sendall(b"-ERR wrong number of arguments for 'ZADD' command\r\n")
    key, args = command[0], command[1:]
    try:
        added_count = sorted_set_store.zadd(key, args)
        return connection.sendall(f":{added_count}\r\n".encode())
    except ValueError as e:
        return connection.sendall(f"-ERR {str(e)}\r\n".encode())


def handle_zrank(connection, command):
    if len(command) != 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'ZRANK' command\r\n")
    key, member = command[0], command[1]
    rank = sorted_set_store.zrank(key, member)
    if rank is not None:
        return connection.sendall(f":{rank}\r\n".encode())
    else:
        return connection.sendall(b"$-1\r\n")
    

def handle_zrange(connection, command):
    if len(command) != 3:
        return connection.sendall(b"-ERR wrong number of arguments for 'ZRANGE' command\r\n")
    key, start, end = command[0], command[1], command[2]

    try:
        start = int(start)
        end = int(end)
    except ValueError:
        return connection.sendall(b"-ERR value is not an integer or out of range\r\n")

    members = sorted_set_store.zrange(key, start, end)
    response = f"*{len(members)}\r\n"
    for member in members:
        response += f"${len(member)}\r\n{member}\r\n"
    return connection.sendall(response.encode())


def handle_zcard(connection, command):
    if len(command) != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'ZCARD' command\r\n")
    key = command[0]
    cardinality = sorted_set_store.zcard(key)
    return connection.sendall(f":{cardinality}\r\n".encode())


def handle_zscore(connection, command):
    if len(command) != 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'ZSCORE' command\r\n")
    key, member = command[0], command[1]
    score = sorted_set_store.zscore(key, member)
    if score is not None:
        return connection.sendall(f"${len(str(score))}\r\n{score}\r\n".encode())
    else:
        return connection.sendall(b"$-1\r\n")    


def handle_zrem(connection, command):
    if len(command) != 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'ZREM' command\r\n")
    key, member = command[0], command[1]
    removed_count = sorted_set_store.zrem(key, member)
    return connection.sendall(f":{removed_count}\r\n".encode())


def handle_geoadd(connection, command):
    if len(command) < 4 or len(command) % 3 != 1:
        return connection.sendall(b"-ERR wrong number of arguments for 'GEOADD' command\r\n")
    
    key = command[0]
    locations = command[1:]
    added_count = 0
    for i in range(0, len(locations), 3):
        try:
            longitude = float(locations[i])
            latitude = float(locations[i+1])
            location = locations[i+2]
            if not (-180 <= longitude <= 180) or not (-85.05112878 <= latitude <= 85.05112878):
                raise ValueError
        except (ValueError, IndexError):
            return connection.sendall(f"-ERR invalid longitude, latitude pair for '{locations[i+2]}'\r\n".encode())

        score = encode_geohash(longitude, latitude)
        added_count += sorted_set_store.zadd(key, [str(score), location])

    connection.sendall(f":{added_count}\r\n".encode())


def handle_geopos(connection, command):
    if len(command) < 2:
        return connection.sendall(b"-ERR wrong number of arguments for 'GEOPOS' command\r\n")
    key, locations = command[0], command[1:]
    response = f"*{len(locations)}\r\n"
    for loc in locations:
        score = sorted_set_store.zscore(key, loc)
        if score is None:
            response += "*-1\r\n"
        else:
            longitude, latitude = decode_geohash(int(score))
            response += f"*2\r\n${len(str(longitude))}\r\n{longitude}\r\n${len(str(latitude))}\r\n{latitude}\r\n"

    return connection.sendall(response.encode())


def handle_geodist(connection, command):
    if len(command) != 3:
        return connection.sendall(b"-ERR wrong number of arguments for 'GEODIST' command\r\n")
    key, loc1, loc2 = command[0], command[1], command[2]
    score1 = sorted_set_store.zscore(key, loc1)
    score2 = sorted_set_store.zscore(key, loc2)

    if score1 is None or score2 is None:
        return connection.sendall(b"$-1\r\n")

    lon1, lat1 = decode_geohash(int(score1))
    lon2, lat2 = decode_geohash(int(score2))

    distance = haversine(lon1, lat1, lon2, lat2)

    return connection.sendall(f"${len(str(distance))}\r\n{distance}\r\n".encode())


def handle_geosearch(connection, command):
    if len(command) < 7 or command[1].upper() != 'FROMLONLAT' or command[4].upper() != 'BYRADIUS':
        return connection.sendall(b"-ERR syntax error\r\n")
    
    key = command[0]
    try:
        longitude = float(command[2])
        latitude = float(command[3])
        radius = float(command[5])
        unit = command[6].upper()
    except ValueError:
        return connection.sendall(b"-ERR invalid number formats\r\n")

    try:
        results = sorted_set_store.geosearch(key, longitude, latitude, radius, unit)
        response = f"*{len(results)}\r\n"
        for location in results:
            response += f"${len(location)}\r\n{location}\r\n"
        return connection.sendall(response.encode())
    except ValueError as e:
        return connection.sendall(f"-ERR {e}\r\n".encode())
    

COMMAND_HANDLERS = {
    "PING": handle_ping,
    "ECHO": handle_echo,
    "SET": handle_set,
    "GET": handle_get,
    "RPUSH": handle_rpush,
    "LRANGE": handle_lrange,
    "LPUSH": handle_lpush,
    "LLEN": handle_llen,
    "LPOP": handle_lpop,
    "BLPOP": handle_blpop,
    "TYPE": handle_type,
    "XADD": handle_xadd,
    "XRANGE": handle_xrange,
    "XREAD": handle_xread,
    "INCR": handle_incr,
    "INFO": handle_info,
    "REPLCONF": handle_replconf,
    "PSYNC": handle_psync,
    "WAIT": handle_wait,
    "CONFIG": handle_config,
    "KEYS": handle_keys,
    "SUBSCRIBE": handle_subscribe,
    "PUBLISH": handle_publish,
    "ZADD": handle_zadd,
    "ZRANK": handle_zrank,
    "ZRANGE": handle_zrange,
    "ZCARD": handle_zcard,
    "ZSCORE": handle_zscore,
    "ZREM": handle_zrem,
    "GEOADD": handle_geoadd,
    "GEOPOS": handle_geopos,
    "GEODIST": handle_geodist,
    "GEOSEARCH": handle_geosearch,
}


def execute_command(connection, command):
    cmd = command[0].upper() if command else None

    handler = COMMAND_HANDLERS.get(cmd)
    if handler:
        if cmd == "SUBSCRIBE":
            handler(connection, command[1:])
            enter_subscription_mode(connection)
        else:
            handler(connection, command[1:])
    else:
        connection.sendall(b"-ERR unknown command\r\n")


def send_response(connection, initial_buffer=b""):
    global replica_offset
    buffer = initial_buffer
    try:
        while True:
            # If we have no data, block and wait for more
            if not buffer:
                data = connection.recv(1024)
                if not data:
                    break  # Connection closed
                buffer += data

            # Use the reliable parsing functions
            commands_with_bytes, remaining_buffer = command_parser.parse_commands(buffer)

            # If no full commands could be parsed, we need more data
            if not commands_with_bytes:
                data = connection.recv(1024)
                if not data:
                    break # Connection closed
                buffer += data
                continue # Go back to the top to try parsing again

            # Process all fully parsed commands
            for command, command_bytes in commands_with_bytes:
                print(f"Received command: {command}")
                cmd = command[0].upper() if command else None

                # Handle commands from the master
                if connection == master_connection_socket:
                    # For GETACK, respond with the offset *before* this command
                    if cmd == "REPLCONF" and len(command) > 1 and command[1].upper() == "GETACK":
                        offset_str = str(replica_offset)
                        response = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(offset_str)}\r\n{offset_str}\r\n"
                        connection.sendall(response.encode())
                    else:
                        # For other commands from master, just execute them
                        execute_command(connection, command)

                    # Update the offset *after* processing the command
                    replica_offset += command_bytes

                # Handle commands from regular clients
                else:
                    if cmd == "MULTI":
                        handle_multi(connection)
                    elif cmd == "EXEC":
                        handle_exec(connection)
                    elif cmd == "DISCARD":
                        handle_discard(connection)
                    elif queue_command(connection, command):
                        # Command was queued, do nothing else
                        pass
                    else:
                        execute_command(connection, command)
                        # Propagate write commands if this is a master server
                        if not replica_of and cmd in write_commands:
                            propagate_to_replicas(command)

            # Update the buffer with any remaining, unparsed data
            buffer = remaining_buffer

    except Exception as e:
        print(f"Error in send_response: {e}")
    finally:
        conn_id = id(connection)
        with connections_lock:
            if conn_id in connections:
                del connections[conn_id]
        with replicas_lock:
            if connection in replicas:
                replicas.remove(connection)
        with replica_offsets_lock:
            if connection in replica_offsets:
                del replica_offsets[connection]
        connection.close()


def connect_to_master(host, port, replica_port):
    try:
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((host, port))

        # Handshake steps
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
        
        # Read FULLRESYNC response
        buffer = b""
        def read_line():
            nonlocal buffer
            while True:
                crlf_pos = buffer.find(b"\r\n")
                if crlf_pos != -1:
                    line = buffer[:crlf_pos]
                    buffer = buffer[crlf_pos + 2:]
                    return line
                chunk = master_socket.recv(4096)
                if not chunk:
                    return b""
                buffer += chunk

        # Read +FULLRESYNC line
        fullresync_line = read_line()
        if not fullresync_line.startswith(b"+FULLRESYNC"):
            print("Failed to receive FULLRESYNC from master")
            master_socket.close()
            return None

        # Read RDB file length header ($<length>)
        rdb_header = read_line()
        if not rdb_header.startswith(b"$"):
            print("Failed to receive RDB header")
            master_socket.close()
            return None
            
        rdb_length = int(rdb_header[1:])
        print(f"RDB file length: {rdb_length}")
        
        # Read the exact RDB file content
        while len(buffer) < rdb_length:
            chunk = master_socket.recv(min(4096, rdb_length - len(buffer)))
            if not chunk:
                break
            buffer += chunk
        
        # Remove RDB data from buffer
        buffer = buffer[rdb_length:]
        print("RDB file consumed completely")

        print(f"Connected to master at {host}:{port}")
        return master_socket, buffer  # Return remaining buffer
    except Exception as e:
        print(f"Failed to connect to master at {host}:{port}: {e}")
        return None, b""


def main(args):
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", int(args.port)), reuse_port=True)

    global replica_of, master_connection_socket, dir, dbfilename, dictionary
    if args.replicaof:
        replica_of = args.replicaof
        master_host, master_port = args.replicaof.split()

        result = connect_to_master(master_host, int(master_port), args.port)
        if result and result[0]:
            master_connection_socket, remaining_buffer = result
            print("Connected to master at {}:{}".format(master_host, master_port))
            threading.Thread(target=send_response, args=(master_connection_socket, remaining_buffer)).start()
        else:
            print("Failed to connect to master at {}:{}".format(master_host, master_port))

    if args.dir:
        dir = args.dir
    
    if args.dbfilename:
        dbfilename = args.dbfilename

    if dir and dbfilename:
        rdb_path = f"{dir}/{dbfilename}"
        rdb_parser = RDBParser(rdb_path)
        rdb_data = rdb_parser.parse()
        string_store.load_from_rdb(rdb_data)
        print(f"Loaded {len(rdb_data)} keys from RDB file")

    while True:
        connection, _ = server_socket.accept()
        thread = threading.Thread(target=send_response, args=(connection,))
        thread.start()


if __name__ == "__main__":
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on")
    parser.add_argument("--replicaof", type=str, help="Replication source in host port format")
    parser.add_argument("--dir", type=str, help="Directory for persistence files")
    parser.add_argument("--dbfilename", type=str, help="RDB filename")
    args = parser.parse_args()
    main(args)
