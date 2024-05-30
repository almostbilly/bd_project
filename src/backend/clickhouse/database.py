import contextlib
from typing import List, Tuple, Union

import asynch
from asynch.cursors import DictCursor
from asynch.pool import Pool


class Database:
    def __init__(self):
        """
        Initialize the Database class.
        """
        self.pool: Union[Pool, None] = None
        self.database: str = "default"

    @contextlib.asynccontextmanager
    async def create_pool(self, **kwargs):
        """
        Create a connection pool.

        Yields:
            The connection pool.
        """
        self.pool = await asynch.create_pool(**kwargs)
        self.database = kwargs["database"]
        yield self.pool
        self.pool.close()
        await self.pool.wait_closed()

    async def execute(self, query: str):
        """
        Execute a query.

        Args:
            query: The query to execute.
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                try:
                    await cursor.execute(query)
                except Exception as e:
                    print(f"Error executing query: {e}")

    async def create_table(self, table_name: str):
        """
        Create a table.

        Args:
            table_name: The name of the table to create.
        """
        await self.create_database()
        query = self.get_create_table_query(table_name)
        await self.execute(query)

    async def create_database(self):
        """
        Create the database.
        """
        query = f"create database if not exists {self.database}"
        await self.execute(query)

    def get_create_table_query(self, table_name: str) -> str:
        """
        Get the query to create a table.

        Args:
            table_name: The name of the table to create.

        Returns:
            The query to create the table.
        """
        match table_name:
            case "videos":
                return """
                            CREATE TABLE IF NOT EXISTS videos (
                                id UInt32,
                                user_id UInt32,
                                created_at DateTime,
                                duration String,
                                view_count UInt32
                            ) ENGINE = MergeTree
                            ORDER BY created_at
                        """
            case "clips":
                return """
                            CREATE TABLE IF NOT EXISTS clips (
                                id String,
                                video_id UInt32,
                                created_at DateTime,
                                duration UInt32,
                                view_count UInt32,
                                vod_offset UInt32
                            ) ENGINE = MergeTree
                            ORDER BY created_at
                        """
            case "messages":
                return """
                            CREATE TABLE IF NOT EXISTS messages (
                                id String,
                                video_id UInt32,
                                created_at DateTime,
                                commenter_name Nullable(String),
                                content Nullable(String),
                                text Nullable(String),
                                emotes Array(Nullable(String))
                            ) ENGINE = MergeTree
                            ORDER BY created_at
                        """
            case "emotes":
                return """
                            CREATE TABLE IF NOT EXISTS emotes (
                                id String,
                                broadcaster_id UInt32,
                                source String,
                                name String
                            ) ENGINE = MergeTree
                            ORDER BY broadcaster_id
                        """

    async def insert_in_table(self, table_name: str, values: List[Tuple]):
        """
        Insert values into a table.

        Args:
            table_name: The name of the table to insert values into.
            values: The values to insert.
        """
        columns = self._get_table_columns(table_name)
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(
                    f"""INSERT INTO {self.database}.{table_name} ({', '.join(columns)}) VALUES""",
                    values,
                )

    def _get_table_columns(self, table_name: str):
        """
        Get the columns of a table.

        Args:
            table_name: The name of the table to get the columns from.

        Returns:
            A list of the columns of the table.
        """
        match table_name:
            case "videos":
                return ["id", "user_id", "created_at", "duration", "view_count"]
            case "clips":
                return [
                    "id",
                    "video_id",
                    "created_at",
                    "duration",
                    "view_count",
                    "vod_offset",
                ]
            case "messages":
                return [
                    "id",
                    "video_id",
                    "created_at",
                    "commenter_name",
                    "content",
                    "text",
                    "emotes",
                ]
            case "emotes":
                return ["id", "broadcaster_id", "source", "name"]


db = Database()
