#!/usr/bin/env python

import json
import pathlib
import logging

import asyncio
import argparse

from time import time
from typing import NamedTuple

from utils.logger_formatter import OneLineExceptionFormatter
from utils.request_manager import RequestManager
from utils.contants import DEFAULT_DEBUGGING, ASYNCIO_GATHER_TYPE, TIMEOUT, MAIN_DIR


class RunConfig(NamedTuple):
    path_to_urls: pathlib.Path
    output: str
    verbose: bool = DEFAULT_DEBUGGING

def format_bytes(num_bytes: int) -> str:
    if num_bytes is None:
        return "0 B"
    if num_bytes >= 1024 * 1024:
        return f"{num_bytes / (1024 * 1024):.2f} MB"
    elif num_bytes >= 1024:
        return f"{num_bytes / 1024:.2f} KB"
    else:
        return f"{num_bytes} B"

def write_results_to_file(results: ASYNCIO_GATHER_TYPE, result_file_name) -> None:
    cleaned_results = []

    for result in results:
        if result.get("error"):
            cleaned_results.append({
                "url": result.get("url"),
                "status_code": str(result.get("status_code")),
                "error": result.get("error")
            })
        else:
            cleaned_results.append({
                "url": result.get("url"),
                "status_code": str(result.get("status_code")),
                "content_length": format_bytes(result.get('content_length')),
                "stream_reader": format_bytes(result.get('stream_reader')),
                "body_length": format_bytes(result.get('body_length')),
            })

    with open(result_file_name, 'w') as jf:
        json.dump(cleaned_results, jf, indent=2)

    logging.log(logging.DEBUG, f'Wrote results to file with name: {result_file_name}')


def define_config_from_cmd(parsed_args: argparse.Namespace) -> RunConfig:
    """
    parsing config from args
    :param parsed_args: argparse.Namespace
    :return: RunConfig
    """
    return RunConfig(
        path_to_urls=parsed_args.path_to_urls,
        verbose=parsed_args.verbose,
        output=parsed_args.output,
    )


def cli() -> argparse.Namespace:
    """
    here we define args to run the script with
    :return: argparse.Namespace
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(description='scan list of urls')
    # Add the arguments to the parser
    parser.add_argument(
        '--input', type=pathlib.Path, metavar='PATH', dest='path_to_urls',
        help='Path to file with input. Example: "/wd/input.txt"',
    )
    parser.add_argument(
        '--target', type=str, required=False,
        help='Single target URL to scan instead of file input',
    )

    parser.add_argument('--verbose', action='store_true', default=DEFAULT_DEBUGGING,
                        required=False, help='Verbose debug messages')
    parser.add_argument('--output', type=str, required=False, help='Output file name')

    return parser.parse_args()


async def main() -> None:
    args: argparse.Namespace = cli()
    config: RunConfig = define_config_from_cmd(args)

    OneLineExceptionFormatter.logger_initialisation(config.verbose)
    logging.log(logging.DEBUG, 'Main Started')

    output: str = args.output
    target: str = args.target

    if target:
        urls: list[str] = [target]
    elif config.path_to_urls and config.path_to_urls.exists():
        urls: list[str] = config.path_to_urls.read_text().splitlines()
    else:
        raise ValueError("You must specify either --target or --input with a valid file path")

    logging.log(logging.DEBUG, f'Got {len(urls)} from file with name: {args.path_to_urls}')

    results: ASYNCIO_GATHER_TYPE = await RequestManager.create_make_requests(urls=urls, timeout=TIMEOUT)

    RESULT_FILE_NAME: pathlib.Path = MAIN_DIR / output

    write_results_to_file(results, RESULT_FILE_NAME)


if __name__ == '__main__':
    start_time = time()
    asyncio.run(main(), debug=DEFAULT_DEBUGGING)
    logging.log(logging.DEBUG, f'Time consumption: {time() - start_time: 0.3f}s')
