"""Async requests using aioHttp"""

import asyncio
import os

import aiohttp
from loguru import logger


class Requests:
    # Perform async HTTP request
    async def request(self, method, url, params=None, data=None, headers=None):
        # Configurable behavior via env vars
        # HTTP_TIMEOUT: total timeout in seconds (default 30)
        # HTTP_SSL_VERIFY: "true" to enable SSL verification, otherwise disabled
        timeout_total = float(os.getenv("HTTP_TIMEOUT", "30"))
        ssl_verify_env = os.getenv("HTTP_SSL_VERIFY", "false").lower()
        ssl_verify = ssl_verify_env in {"1", "true", "yes", "on"}

        # Use ssl=None to respect verification, ssl=False to disable
        connector = aiohttp.TCPConnector(ssl=None if ssl_verify else False)

        try:
            timeout = aiohttp.ClientTimeout(total=timeout_total)
            async with aiohttp.ClientSession(
                connector=connector, timeout=timeout
            ) as session:
                # Basic retry for transient network/server errors
                max_retries = 3
                backoff = 1.0
                last_err = None
                for attempt in range(1, max_retries + 1):
                    try:
                        async with session.request(
                            method,
                            url,
                            params=params,
                            data=data,
                            headers=headers,
                        ) as resp:
                            # Raise for 4xx/5xx
                            resp.raise_for_status()
                            try:
                                return await resp.json()
                            except aiohttp.client_exceptions.ContentTypeError:
                                return await resp.text()
                    except (
                        aiohttp.ClientConnectionError,
                        aiohttp.ServerTimeoutError,
                        aiohttp.ClientResponseError,
                    ) as e:
                        last_err = e
                        # Retry on 5xx or network issues
                        retryable_status = getattr(e, "status", None)
                        if attempt < max_retries and (
                            retryable_status is None
                            or 500 <= int(retryable_status) < 600
                        ):
                            logger.warning(
                                f"HTTP transient error on attempt {attempt}/{max_retries}: {e}. Retrying in {backoff:.1f}s"
                            )
                            await asyncio.sleep(backoff)
                            backoff *= 2
                            continue
                        raise

        except Exception as err:
            logger.error(
                "HTTP Error: {} {} PARAMS: {} DATA: {} HEADERS: {} ERR: {}".format(
                    method,
                    url,
                    params,
                    data,
                    headers,
                    err,
                ),
            )
            return {
                "success": False,
                "error": f"APIexception: {err}",
            }

    # Perform async GET request
    async def get(self, url, params=None, headers=None):
        return await self.request("GET", url, params=params, headers=headers)

    # Perform async POST request
    async def post(self, url, data=None, headers=None):
        return await self.request("POST", url, data=data, headers=headers)
