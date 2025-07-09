import asyncio
import logging
from asyncio import BoundedSemaphore
from asyncio.exceptions import TimeoutError
from typing import Union, Any

from aiohttp import (
    ClientSession,
    ClientTimeout,
    InvalidURL,
    ClientConnectorError,
    ClientResponseError,
    ServerTimeoutError,
    TCPConnector,
)
from yarl import URL

from utils.contants import (
    TIMEOUT_DEFAULT,
    SIMULTANEOUS_CONCURRENT_TASKS,
    USER_AGENT,
    LIMIT_OF_ATTEMPTS_TO_RETRY,
    CONTENT_LENGTH_DEFAULT,
    REQUESTS_RETRIES_NUM_TO_REMOVE,
    STATUS_CODE_DEFAULT,
    ASYNCIO_GATHER_TYPE,
    STREAM_READER_DEFAULT,
    BODY_LENGTH_DEFAULT,
)


class RequestManager:
    _urls: list[URL]
    _timeout: ClientTimeout
    _session: ClientSession
    _semaphore: BoundedSemaphore
    _headers: dict[str, str]
    _failed_requests_num: int

    def __init__(self, urls: list[str], timeout: int = TIMEOUT_DEFAULT):
        self._urls = [URL(url) for url in urls]
        self._timeout = ClientTimeout(total=timeout)
        self._session = ClientSession(timeout=self.timeout, connector=TCPConnector(ssl=False))
        self._semaphore = asyncio.BoundedSemaphore(SIMULTANEOUS_CONCURRENT_TASKS)
        self._headers = {'User-agent': USER_AGENT}
        self._failed_requests_num = 0

    @classmethod
    async def create_make_requests(cls, urls: list[str], timeout: int = TIMEOUT_DEFAULT) -> ASYNCIO_GATHER_TYPE:
        obj: RequestManager = cls(urls=urls, timeout=timeout)
        logging.log(logging.DEBUG, f'{obj.__class__} created')
        results = await obj.make_requests()
        logging.log(logging.DEBUG, f'Number failed results: {obj.failed_requests_num}')
        return results

    async def _fetch(self, url: URL, session: ClientSession) -> dict[str, Union[str, int]]:
        logging.log(logging.DEBUG, f'Request to url: "{url}" stated')
        async with self.semaphore:
            result: dict[str, str] = {'url': str(url)}
            left_of_attempts_to_retry: int = LIMIT_OF_ATTEMPTS_TO_RETRY
            while left_of_attempts_to_retry:
                try:
                    async with session.get(url, headers=self.headers) as response:
                        result.update({
                            'status_code': response.status,
                            'content_length': 'content-length' in response.headers
                                              and response.headers['content-length'] or CONTENT_LENGTH_DEFAULT,
                            'stream_reader': response.content.total_bytes,
                            'body_length': len(await response.read()),
                            'error': '',
                        })
                except (ClientConnectorError, ClientResponseError, ServerTimeoutError, TimeoutError, InvalidURL) as e:
                    logging.exception(
                        f'Failed attempt num: '
                        f'{LIMIT_OF_ATTEMPTS_TO_RETRY - left_of_attempts_to_retry + REQUESTS_RETRIES_NUM_TO_REMOVE}'
                        f'Error: {e}'
                    )
                    left_of_attempts_to_retry -= REQUESTS_RETRIES_NUM_TO_REMOVE
                    self.failed_requests_num = REQUESTS_RETRIES_NUM_TO_REMOVE
                    if not left_of_attempts_to_retry:
                        result.update({
                            'status_code': STATUS_CODE_DEFAULT,
                            'content_length': CONTENT_LENGTH_DEFAULT,
                            'stream_reader': STREAM_READER_DEFAULT,
                            'body_length': BODY_LENGTH_DEFAULT,
                            'error': e and str(e) or 'Something Went Wrong'
                        })
                    else:
                        continue
                else:
                    logging.log(
                        logging.DEBUG,
                        f'Request to url: "{url}" succeed with possible retries: {left_of_attempts_to_retry}')
                    break
            return result

    async def make_requests(self) -> list[Any]:
        async with self.session as session:
            return await asyncio.gather(*[
                asyncio.create_task(
                    self._fetch(url=url, session=session)
                ) for url in self.urls
            ])

    @property
    def urls(self) -> list[URL]:
        return self._urls

    @property
    def timeout(self) -> ClientTimeout:
        return self._timeout

    @property
    def session(self) -> ClientSession:
        return self._session

    @property
    def semaphore(self) -> BoundedSemaphore:
        return self._semaphore

    @property
    def headers(self) -> dict[str, str]:
        return self._headers

    @property
    def failed_requests_num(self) -> int:
        return self._failed_requests_num

    @failed_requests_num.setter
    def failed_requests_num(self, num: int) -> None:
        self._failed_requests_num += num
