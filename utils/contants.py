import pathlib
from types import TracebackType
from typing import Union, Type, Optional, Any, Final

TIMEOUT: int = 5
TIMEOUT_DEFAULT: int = 60
LIMIT_OF_ATTEMPTS_TO_RETRY: int = 5
SIMULTANEOUS_CONCURRENT_TASKS: int = 51
REQUESTS_RETRIES_NUM_TO_REMOVE = 1

DEFAULT_DEBUGGING = False

STATUS_CODE_DEFAULT, CONTENT_LENGTH_DEFAULT, STREAM_READER_DEFAULT, BODY_LENGTH_DEFAULT = 0, 0, 0, 0

USER_AGENT: str = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)' \
                  ' Chrome/85.0.4183.102 Safari/537.36'

EXC_INFO_TYPE = Union[tuple[type, BaseException, Optional[TracebackType]], tuple[None, None, None]]
ASYNCIO_GATHER_TYPE: Type = tuple[
    Union[BaseException, Any],
    Union[BaseException, Any],
    Union[BaseException, Any],
    Union[BaseException, Any],
    Union[BaseException, Any],
]

MAIN_DIR: Final[pathlib.Path] = pathlib.Path(__file__).resolve().parents[1]
