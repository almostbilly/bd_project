import re
from datetime import datetime, timedelta
from typing import Dict, List, Union

import aiohttp
import requests


class TwitchParser:
    def __init__(self, client_id: str, secret_key: str):
        self.client_id = client_id
        self.secret_key = secret_key
        self.access_token = self._get_token()

    def _get_token(self) -> str:
        """
        Loads an OAuth client credentials flow App token for an associated client id
        to an environment variables.

        Returns:
            The access token.
        """
        auth_url = "https://id.twitch.tv/oauth2/token"
        # parameters for token request with credentials
        auth_params = {
            "client_id": self.client_id,
            "client_secret": self.secret_key,
            "grant_type": "client_credentials",
            "scope": "chat:read",
        }
        # Request response, using the url base and params to structure the request
        auth_response = requests.post(auth_url, params=auth_params)
        token = auth_response.json().get("access_token")

        return token

    def _parse_duration(self, iso_duration):
        """
        Parses an ISO 8601 duration string into a datetime.timedelta instance.

        Args:
            iso_duration: an ISO 8601 duration string.

        Returns:
            A datetime.timedelta instance
        """
        m = re.match(
            r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:.\d+)?)S)?$",
            iso_duration,
        )
        if m is None:
            raise ValueError("invalid ISO 8601 duration string")

        days = 0
        hours = 0
        minutes = 0
        seconds = 0.0

        if m[3]:
            days = int(m[3])
        if m[4]:
            hours = int(m[4])
        if m[5]:
            minutes = int(m[5])
        if m[6]:
            seconds = float(m[6])

        return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

    async def request_get(self, query: str, fields: Dict, session) -> Dict:
        """
        Makes a GET request from the Twitch.tv helix API.

        Args:
            query: A string for the query (e.g., "users", "games/top", "search/categories...").
            fields: A dict for the field parameter (e.g., {"login": "gmhikaru"}).
            session: An aiohttp.ClientSession instance.

        Returns:
            A dict containing the response data.
        """
        base_url = "https://api.twitch.tv/helix/"
        headers = {
            "client-id": self.client_id,
            "Authorization": f"Bearer {self.access_token}",
        }
        async with session.get(
            base_url + query, headers=headers, params=fields
        ) as response:
            response = await response.json()

        return response

    async def fetch_videos_by_ids(
        self, video_ids: Union[int, List[int], str, List[str]]
    ) -> List[Dict]:
        """
        Fetches metadata of the specified VODs.

        Args:
            video_ids: A single VOD ID, a list of VOD IDs, a single VOD ID as a string, or a list of VOD IDs as strings.

        Returns:
            A list of dictionaries containing the video metadata.
        """
        if not video_ids:
            raise TypeError(f"{video_ids} provided for video_ids")
        video_ids = [str(video_id) for video_id in video_ids]

        async with aiohttp.ClientSession() as session:
            response = await self.request_get("videos", {"id": video_ids}, session)
            videos = [
                (
                    int(video["id"]),
                    int(video["user_id"]),
                    datetime.strptime(video["created_at"], "%Y-%m-%dT%H:%M:%SZ"),
                    video["duration"],
                    int(video["view_count"]),
                )
                for video in response["data"]
            ]

        return videos

    async def fetch_videos_by_category(
        self, category_id: Union[int, str], top_k: int = 10
    ) -> List[Dict]:
        """
        Fetches metadata of the videos of specified game or category.

        Args:
            category_id: A single category_id as int or string
            top_k: The maximum number of videos to return.  1 <= top_k <= 100. The default is 10.

        Returns:
            A list of tuples containing the videos metadata.
        """
        if not category_id:
            raise TypeError(f"{category_id} provided for category_id")

        params = {
            "game_id": str(category_id),
            "language": "RU",
            "period": "week",
            "sort": "views",
            "type": "archive",
            "first": top_k,
        }

        async with aiohttp.ClientSession() as session:
            response = await self.request_get("videos", params, session)
            videos = [
                (
                    int(video["id"]),
                    int(video["user_id"]),
                    datetime.strptime(video["created_at"], "%Y-%m-%dT%H:%M:%SZ"),
                    video["duration"],
                    int(video["view_count"]),
                )
                for video in response["data"]
            ]

        return videos

    async def fetch_top_categories(self, top_k: int = 10) -> List[Dict]:
        """
        Fetches metadata of top categories or games.

        Returns:
            A list of tuples containing the categories metadata.
        """
        async with aiohttp.ClientSession() as session:
            response = await self.request_get("games/top", None, session)
            videos = [
                (int(category["id"]), category["name"]) for category in response["data"]
            ]

        return videos[0:top_k]
