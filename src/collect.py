import asyncio
import logging
import os

from dotenv import load_dotenv
from hydra import compose, initialize

from src.backend.clickhouse.database import db
from src.parser.chat import ChatParser
from src.parser.clip import ClipParser
from src.parser.thirdparties import ThirdPartyEmotesParser
from src.parser.twitch_parser import TwitchParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

load_dotenv(override=True)


async def main():
    initialize(version_base=None, config_path="../configs")
    cfg = compose(config_name="parser")

    video_ids = cfg.video_ids
    n_batches = cfg.n_batches
    top_k_categories = cfg.top_k_categories
    top_k_videos = cfg.top_k_videos

    client_id = os.getenv("CLIENT_ID")
    secret_key = os.getenv("SECRET_KEY")

    async with db.create_pool(
        host=cfg.ch_host,
        port=cfg.ch_port,
        database=cfg.ch_database,
        user=cfg.ch_user,
        password=cfg.ch_password,
    ) as pool:

        await db.create_table("videos")
        await db.create_table("clips")
        await db.create_table("emotes")
        await db.create_table("messages")

        twitchParser = TwitchParser(client_id, secret_key)
        clipParser = ClipParser(client_id, secret_key)
        chatParser = ChatParser(client_id, secret_key)
        emotesParser = ThirdPartyEmotesParser()

        categories = await twitchParser.fetch_top_categories(top_k_categories)
        for category_id, category_name in categories:
            logging.info(f"Processing {category_name} category...")
            # fetch videos data
            videos = await twitchParser.fetch_videos_by_category(
                category_id, top_k=top_k_videos
            )
            video_ids = [video[0] for video in videos]
            await db.insert_in_table(table_name="videos", values=videos)
            # fetch clips data
            clips = await clipParser.get_clips(video_ids, n_batches, pool)
            await db.insert_in_table(table_name="clips", values=clips)
            # fetch emotes data
            emotes = await emotesParser.get_emotes(video_ids, pool)
            await db.insert_in_table(table_name="emotes", values=emotes)
            # fetch comments data
            comments = await chatParser.get_comments(video_ids, n_batches, pool)
            await db.insert_in_table(table_name="messages", values=comments)
            logging.info(f"{category_name} category successfully processed.")


if __name__ == "__main__":
    asyncio.run(main())
