import asyncio
from datetime import datetime
from typing import Any, List, Tuple, Union

import aiohttp
import more_itertools
from asynch.cursors import DictCursor
from asynch.pool import Pool

from src.parser.twitch_parser import TwitchParser


class ClipParser(TwitchParser):
    def __init__(self, client_id: str, secret_key: str):
        """
        Initialize the ClipParser class with client_id and secret_key.

        Args:
            client_id: The client ID for Twitch API.
            secret_key: The secret key for Twitch API.
        """
        super().__init__(client_id, secret_key)

    async def get_batch_clips(
        self,
        broadcaster_id: int,
        started_at: str,
        ended_at: str,
        session: aiohttp.ClientSession = None,
    ) -> List[List[Tuple[Any, ...]]]:
        """
        Fetches a batch of clips for a specific broadcaster within a given time range.

        Args:
            broadcaster_id: The ID of the broadcaster.
            started_at: The start of the time range in ISO 8601 format.
            ended_at: The end of the time range in ISO 8601 format.
            session: An aiohttp.ClientSession instance (optional).

        Returns:
            A list of lists containing tuples of clip data.
        """
        has_pages = True
        pages = []
        cursor = ""
        while has_pages:
            response = await self.request_get(
                "clips",
                {
                    "broadcaster_id": broadcaster_id,
                    "started_at": started_at,
                    "ended_at": ended_at,
                    "first": 100,
                    "after": cursor,
                },
                session,
            )
            has_pages = bool(response["pagination"])
            if has_pages:
                cursor = response["pagination"]["cursor"]
            clips = [
                (
                    clip["id"],
                    int(clip["video_id"]),
                    datetime.strptime(clip["created_at"], "%Y-%m-%dT%H:%M:%SZ"),
                    int(clip["duration"]),
                    int(clip["view_count"]),
                    int(clip["vod_offset"]),
                )
                for clip in response["data"]
            ]
            pages.append(clips)
        return pages

    async def get_video_clips(
        self,
        video: Tuple[Any, ...],
        n_batches: int,
        session: aiohttp.ClientSession,
    ) -> List[List[Tuple[Any, ...]]]:
        """
        Fetches clips for a specific video, divided into batches.

        Args:
            video: A tuple containing video data.
            n_batches: The number of batches to divide the video into.
            session: An aiohttp.ClientSession instance.

        Returns:
            A list of lists containing tuples of clip data.
        """
        broadcaster_id = video["user_id"]
        started_at = video["created_at"]
        duration_dt = self._parse_duration("PT" + video["duration"].upper())
        batch_size = duration_dt / n_batches
        batch_starts = [started_at + i * batch_size for i in range(n_batches)]
        batch_ends = [batch_start + batch_size for batch_start in batch_starts]

        tasks = []
        for i in range(n_batches):
            tasks.append(
                self.get_batch_clips(
                    broadcaster_id,
                    batch_starts[i].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    batch_ends[i].strftime("%Y-%m-%dT%H:%M:%SZ"),
                    session,
                )
            )
        clips = await asyncio.gather(*tasks)
        return clips

    async def get_clips(
        self,
        video_ids: Union[str, List[str]],
        n_batches: int = 1,
        pool: Pool = None,
    ) -> List[Tuple[Any, ...]]:
        """
        Fetches all clips for the specified VODs.

        Args:
            video_ids: A single VOD ID, or a list of VOD IDs.
            n_batches: The number of batches to divide each video into (optional).
            pool: A Pool instance (optional).

        Returns:
            A list of tuples containing clip data.
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
                tasks.append(self.get_video_clips(video, n_batches, session))
            clips = await asyncio.gather(*tasks)

        clips = list(more_itertools.collapse(clips, base_type=tuple))

        return clips
