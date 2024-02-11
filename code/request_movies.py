import os
import asyncio
from datetime import datetime, timedelta
from utils import random_user_agent, load, Block
try:
    import aiohttp
except:
    os.system("pip install aiohttp")
    import aiohttp

CURRENT_DIR_PATH = os.path.dirname(os.path.abspath(__file__))
MOVIES_PATH = os.path.join(CURRENT_DIR_PATH, "movies.json")

api_key = "<YOUR_API_KEY>"
block_size = 4000 # How many films are required before saving
requests_per_second = 20 # Requests per second (over 40 could lead to rate limiting)

block = Block(block_size, MOVIES_PATH)

async def fetch_movie(session: aiohttp.ClientSession, index: int, semaphore: asyncio.Semaphore) -> None:
    """
    Asynchronously fetches movie data from The Movie Database (TMDb) API.

    :param session: An aiohttp ClientSession object for making HTTP requests.
    :param index: The unique identifier of the movie to fetch.
    :param semaphore: An asyncio Semaphore to limit concurrent access to shared resources.
    """

    url = f"https://api.themoviedb.org/3/movie/{index}"
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + api_key,
        "User-Agent": random_user_agent(),
    }
    params = {
        'append_to_response': 'alternative_titles%2Cchanges%2Ccredits%2Cexternal_ids%2Cimages%2Ckeywords%2Clatest%2Clists%2Crecommendations%2Crelease_dates%2Creviews%2Csimilar%2Ctranslations%2Cvideos%2Cproviders',
        'language': 'en-US'
    }

    async with semaphore:
        try:
            async with session.get(url, params=params, headers=headers, timeout=3) as response:
                response.raise_for_status()

                data = await response.json()
                if data.get("success", True):
                    if not str(data.get("id", None)) == str(index):
                        data = None
                else:
                    data = None
        except:
            data = None

        is_block_complete, block_id = block.add_data(index, data)
        if is_block_complete:
            print("~", block_id, "Movies requestet")

async def main():
    "Asynchronous main function to orchestrate the movie data fetching process."

    index = 2

    os.system('cls' if os.name == 'nt' else 'clear')
    print("~~ Loading Movies ~~")

    movies = load(MOVIES_PATH, [])
    for movie in movies:
        if movie.get("id", 1) >= index:
            index = movie["id"] + 1

    semaphore = asyncio.Semaphore(requests_per_second)
    tasks = []

    print("~~ Requests started ~~")

    async with aiohttp.ClientSession() as session:
        start_time = datetime.now()
        for _ in range(2147483645):
            tasks.append(fetch_movie(session, index, semaphore))
            if len(tasks) >= requests_per_second:
                await asyncio.gather(*tasks)

                tasks = []
                elapsed_time = datetime.now() - start_time
                if elapsed_time < timedelta(seconds=1):
                    await asyncio.sleep((1 - elapsed_time.total_seconds()) / requests_per_second)

            index += 1

        if tasks:
            await asyncio.gather(*tasks)

if __name__ == "__main__":
    if api_key == "<YOUR_API_KEY>":
        api_key = input("Please enter your API token for read access from TMDB: ")

    asyncio.run(main())