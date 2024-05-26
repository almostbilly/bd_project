import asyncio
import os
from typing import Any, Dict, List, Tuple, Union

import aiohttp
import more_itertools
from asynch.cursors import DictCursor
from asynch.pool import Pool
from dateutil import parser
from tqdm.asyncio import tqdm_asyncio

from src.parser.twitch_parser import TwitchParser


class ChatParser(TwitchParser):
    def __init__(self, client_id: str, secret_key: str):
        """
        Initialize the ChatParser class with client_id and secret_key.

        Args:
            client_id: The client ID for Twitch API.
            secret_key: The secret key for Twitch API.
        """
        super().__init__(client_id, secret_key)
        self.channel_emotes = None

    def get_comment_data(
        self, video_id: str | int, comment: Dict[str, Any]
    ) -> Tuple[Any, ...]:
        """
        Extracts relevant data from a comment and formats it into a tuple.

        Args:
            video_id: The ID of the video associated with the comment.
            comment: A dictionary containing the comment data.

        Returns:
            A tuple of the extracted comment data.
        """
        comment_id = comment["id"]
        video_id = int(video_id)
        created_at = parser.isoparse(comment["createdAt"]).replace(tzinfo=None)
        commenter_name = (
            comment["commenter"]["displayName"] if comment["commenter"] else None
        )

        text = []
        emotes = []
        message_fragments = []
        for fragment in comment["message"]["fragments"]:
            message_fragments.append(fragment["text"])
            for word in fragment["text"].split():
                if word in self.channel_emotes:
                    emotes.append(word)
                else:
                    text.append(word)
        content = "".join(message_fragments)
        text = " ".join(text)

        return (comment_id, video_id, created_at, commenter_name, content, text, emotes)

    async def get_page_comments(
        self,
        video_id: str | int,
        video_start: int = None,
        cursor: str = None,
        session: aiohttp.ClientSession = None,
    ) -> Dict[str, Any]:
        """
        Fetches a page of comments for a specific video.

        Args:
            video_id: The ID of the video.
            video_start: The start time of the video in seconds (optional).
            cursor: The cursor for the next page of comments (optional).
            session: An aiohttp.ClientSession instance (optional).

        Returns:
            A dictionary containing the response data.
        """
        # the base url for making api requests
        url = "https://gql.twitch.tv/gql"
        # authorization information
        headers = {
            "Client-Id": os.getenv("TWITCH_CLIENT_ID"),
            "Content-Type": "application/json",
        }
        # GraphQL query
        payload = {
            "operationName": "VideoCommentsByOffsetOrCursor",
            "variables": {"videoID": str(video_id)},
            "extensions": {
                "persistedQuery": {
                    "version": 1,
                    "sha256Hash": "b70a3591ff0f4e0313d126c6a1502d79a1c02baebb288227c582044aa76adf6a",
                }
            },
        }
        if video_start and not cursor:
            payload["variables"]["contentOffsetSeconds"] = video_start
        elif cursor and not video_start:
            payload["variables"]["cursor"] = cursor

        async with session.post(url, headers=headers, json=payload) as response:
            response = await response.json()
        return response

    async def get_batch_comments(
        self,
        video_id: int,
        video_start: int,
        video_end: int,
        session: aiohttp.ClientSession,
    ) -> List[Tuple[Any, ...]]:
        """
        Fetches comments for a specific video within a given time range.

        Args:
            video_id: The ID of the video.
            video_start: The start time of the time range in seconds.
            video_end: The end time of the time range in seconds.
            session: An aiohttp.ClientSession instance.

        Returns:
            A list of tuples containing the comment data.
        """
        comments = []
        cursor = None
        response = await self.get_page_comments(video_id, video_start, session=session)
        comments_data = response["data"]["video"]["comments"]["edges"]
        for comment in comments_data:
            if (
                comment["node"]["contentOffsetSeconds"] > video_start
                and comment["node"]["contentOffsetSeconds"] < video_end
            ):
                comment = self.get_comment_data(video_id, comment["node"])
                comments.append(comment)
        if response["data"]["video"]["comments"]["pageInfo"]["hasNextPage"]:
            cursor = response["data"]["video"]["comments"]["edges"][-1]["cursor"]

        while cursor:
            response = await self.get_page_comments(
                video_id, video_start=None, cursor=cursor, session=session
            )

            comments_data = response["data"]["video"]["comments"]["edges"]
            for comment in comments_data:
                if (
                    comment["node"]["contentOffsetSeconds"] > video_start
                    and comment["node"]["contentOffsetSeconds"] < video_end
                ):
                    comment = self.get_comment_data(video_id, comment["node"])
                    comments.append(comment)
            if comments_data[-1]["node"]["contentOffsetSeconds"] >= video_end:
                break
            if response["data"]["video"]["comments"]["pageInfo"]["hasNextPage"]:
                cursor = response["data"]["video"]["comments"]["edges"][-1]["cursor"]
            else:
                cursor = None

        return comments

    async def get_video_comments(
        self,
        video: Tuple[Any, ...],
        n_batches: int,
        session: aiohttp.ClientSession,
        pool: Pool,
    ) -> List[List[Tuple[Any, ...]]]:
        """
        Fetches comments for a specific video, divided into batches.

        Args:
            video: A tuple containing video data.
            n_batches: The number of batches to divide the video into.
            session: An aiohttp.ClientSession instance.
            pool: A Pool instance.

        Returns:
            A list of lists containing tuples of comment data.
        """
        video_id = video["id"]
        broadcaster_id = video["user_id"]
        duration_dt = self._parse_duration("PT" + video["duration"].upper())
        batch_size = duration_dt.seconds / n_batches
        batch_starts = [int(i * batch_size) for i in range(n_batches)]
        batch_ends = [batch_start + batch_size for batch_start in batch_starts]

        async with pool.acquire() as conn:
            async with conn.cursor(cursor=DictCursor) as cursor:
                await cursor.execute(
                    "SELECT name FROM highlights.emotes WHERE broadcaster_id = %(broadcaster_id)s",
                    {"broadcaster_id": int(broadcaster_id)},
                )
                records = await cursor.fetchall()
        self.channel_emotes = [record["name"] for record in records]

        tasks = []
        for i in range(n_batches):
            tasks.append(
                self.get_batch_comments(
                    video_id,
                    batch_starts[i],
                    batch_ends[i],
                    session,
                )
            )
        comments = await asyncio.gather(*tasks)
        return comments

    async def get_comments(
        self,
        video_ids: Union[str, List[str]],
        n_batches: int = 1,
        pool: Pool = None,
    ) -> List[Tuple[Any, ...]]:
        """
        Fetches all comments for the specified videos.

        Args:
            video_ids: A single video ID, or a list of video IDs.
            n_batches: The number of batches to divide each video into (optional).
            pool: A Pool instance (optional).

        Returns:
            A list of tuples containing comment data.
        """
        tasks = []
        async with aiohttp.ClientSession() as session:
            for video_id in video_ids:
                async with pool.acquire() as conn:
                    async with conn.cursor(cursor=DictCursor) as cursor:
                        await cursor.execute(
                            "SELECT * FROM highlights.videos WHERE id = %(video_id)s",
                            {"video_id": int(video_id)},
                        )
                        video = await cursor.fetchone()
                tasks.append(self.get_video_comments(video, n_batches, session, pool))
            comments = await tqdm_asyncio.gather(*tasks)

        comments = list(more_itertools.collapse(comments, base_type=tuple))

        return comments
