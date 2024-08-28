import argparse
import asyncio

from redis.database import Database


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', help='The directory where RDB files are stored')
    parser.add_argument('--dbfilename', help='The name of the RDB file')
    parser.add_argument('--port', help='The ustom port to start the Redis server')
    parser.add_argument('--replicaof', help='Replication settings')
    args = parser.parse_args()
    db = Database.create(args)
    await db.serve()


if __name__ == '__main__':
    asyncio.run(main())
