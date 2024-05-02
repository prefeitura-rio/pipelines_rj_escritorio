# -*- coding: utf-8 -*-
import asyncio

from httpx import AsyncClient, AsyncHTTPTransport
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log


@task
def get_random_cat_pictures(n_times: int, batch_size: int):
    async def get_single_cat_picture():
        transport = AsyncHTTPTransport(retries=3)
        async with AsyncClient(transport=transport, timeout=None) as client:
            response = await client.get("https://cataas.com/cat")
            return response.headers["Content-Type"]

    async def main():
        log("Starting to fetch cat pictures")
        awaitables = [get_single_cat_picture() for _ in range(n_times)]
        awaitables = [
            awaitables[i : i + batch_size] for i in range(0, len(awaitables), batch_size)  # noqa
        ]
        for awaitables_batch in awaitables:
            responses = await asyncio.gather(*awaitables_batch)
            log(f"Fetched {len(responses)} cat pictures")

    asyncio.run(main())
