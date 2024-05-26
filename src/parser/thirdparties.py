import asyncio
import itertools
import logging
from typing import List, Union

import aiohttp
from asynch.cursors import DictCursor
from asynch.pool import Pool


class ThirdPartyEmotesParser:
    """
    A class for parsing emotes from third-party services like BetterTTV, FrankerFaceZ, and 7TV.
    """

    def __init__(self) -> None:
        pass

    async def get_bttv_emotes(
        self, broadcaster_id: str, session: aiohttp.ClientSession
    ) -> List[tuple]:
        """
        Gets the global and channel emotes for a given broadcaster ID
        from BetterTTV.

        Args:
            broadcaster_id: The ID of the broadcaster.
            session: The aiohttp ClientSession to use for making the request.

        Returns:
            A list of tuples containing the emote ID, broadcaster ID, emote
            source, and emote code.
        """
        async with session.get(
            "https://api.betterttv.net/3/cached/emotes/global"
        ) as response:
            data = await response.json()
        global_emotes = [
            (str(global_emote["id"]), broadcaster_id, "bttv", global_emote["code"])
            for global_emote in data
        ]

        channel_emotes = []
        async with session.get(
            f"https://api.betterttv.net/3/cached/users/twitch/{broadcaster_id}"
        ) as response:
            if response.status == 200:
                data = await response.json()
                channel_emotes = [
                    (
                        str(channel_emote["id"]),
                        broadcaster_id,
                        "bttv",
                        channel_emote["code"],
                    )
                    for channel_emote in data["channelEmotes"]
                ]

        return global_emotes + channel_emotes

    async def get_ffz_emotes(
        self, broadcaster_id: str, session: aiohttp.ClientSession
    ) -> List[tuple]:
        """
        Gets the global and channel emotes for a given broadcaster ID
        from FrankerFaceZ.

        Args:
            broadcaster_id: The ID of the broadcaster.
            session: The aiohttp ClientSession to use for making the request.

        Returns:
            A list of tuples containing the emote ID, broadcaster ID, emote
            source, and emote code.
        """
        async with session.get(
            "https://api.betterttv.net/3/cached/frankerfacez/emotes/global"
        ) as response:
            data = await response.json()
        global_emotes = [
            (str(global_emote["id"]), broadcaster_id, "ffz", global_emote["code"])
            for global_emote in data
        ]

        channel_emotes = []
        async with session.get(
            f"https://api.betterttv.net/3/cached/frankerfacez/users/twitch/{broadcaster_id}"
        ) as response:
            if response.status == 200:
                data = await response.json()
                channel_emotes = [
                    (
                        str(channel_emote["id"]),
                        broadcaster_id,
                        "ffz",
                        channel_emote["code"],
                    )
                    for channel_emote in data
                ]

        return global_emotes + channel_emotes

    async def get_7tv_emotes(
        self, broadcaster_id: str, session: aiohttp.ClientSession
    ) -> List[tuple]:
        """
        Gets the global and channel emotes for a given broadcaster ID
        from 7TV.

        Args:
            broadcaster_id: The ID of the broadcaster.
            session: The aiohttp ClientSession to use for making the request.

        Returns:
            A list of tuples containing the emote ID, broadcaster ID, emote
            source, and emote code.
        """
        async with session.get("https://7tv.io/v3/emote-sets/global") as response:
            data = await response.json()
        global_emotes = [
            (str(global_emote["id"]), broadcaster_id, "7tv", global_emote["name"])
            for global_emote in data["emotes"]
        ]

        channel_emotes = []
        async with session.get(
            f"https://7tv.io/v3/users/twitch/{broadcaster_id}"
        ) as response:
            if response.status == 200:
                data = await response.json()
                emote_set = data["emote_set"]
                channel_emotes = [
                    (
                        str(channel_emote["id"]),
                        broadcaster_id,
                        "7tv",
                        channel_emote["name"],
                    )
                    for channel_emote in emote_set["emotes"]
                ]

        return global_emotes + channel_emotes

    async def get_all_channel_emotes(
        self, broadcaster_id: str, session: aiohttp.ClientSession
    ) -> List[tuple]:
        """
        Gets all the emotes for a given broadcaster ID from all supported
        third-party services.

        Args:
            broadcaster_id: The ID of the broadcaster.
            session: The aiohttp ClientSession to use for making the requests.
            pool: The database pool to use for the request.

        Returns:
            A list of tuples containing the emote ID, broadcaster ID, emote
            source, and emote code.
        """
        bttv_emotes = await self.get_bttv_emotes(broadcaster_id, session)
        ffz_emotes = await self.get_ffz_emotes(broadcaster_id, session)
        stv_emotes = await self.get_7tv_emotes(broadcaster_id, session)
        all_emotes = list(set(bttv_emotes + ffz_emotes + stv_emotes))
        logging.info(
            f"Successfully found {len(all_emotes)} emotes on channel of broadcaster {broadcaster_id}"
        )
        return all_emotes

    async def get_emotes(
        self,
        video_ids: Union[int, List[int], str, List[str]],
        pool: Pool,
    ) -> List[tuple]:
        """
        Gets all the emotes for a given list of video IDs.

        Args:
            video_ids: A single video ID or a list of video IDs.
            pool: The database pool to use for the request.

        Returns:
            A list of tuples containing the emote ID, broadcaster ID, emote
            source, and emote code.
        """
        video_ids = [int(video_id) for video_id in video_ids]
        async with aiohttp.ClientSession() as session:
            async with pool.acquire() as conn:
                async with conn.cursor(cursor=DictCursor) as cursor:
                    await cursor.execute(
                        "SELECT DISTINCT user_id FROM highlights.videos WHERE id IN %(video_ids)s",
                        {"video_ids": video_ids},
                    )
                    records = await cursor.fetchall()
            tasks = []
            for record in records:
                tasks.append(self.get_all_channel_emotes(record["user_id"], session))
            emotes = await asyncio.gather(*tasks)

        emotes = list(itertools.chain.from_iterable(emotes))
        return emotes
