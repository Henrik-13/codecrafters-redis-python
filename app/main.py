import argparse
from app.server import Server

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on")
    parser.add_argument("--replicaof", type=str, help="Replication source in host port format")
    parser.add_argument("--dir", type=str, help="Directory for persistence files")
    parser.add_argument("--dbfilename", type=str, help="RDB filename")
    args = parser.parse_args()

    server = Server(args)
    server.start()

if __name__ == "__main__":
    main()