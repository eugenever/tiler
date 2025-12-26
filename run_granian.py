import argparse

from argparse import Namespace
from granian import Granian
from granian.constants import Interfaces

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "generic": {
            "()": "logging.Formatter",
            "fmt": "%(asctime)s.%(msecs)03d [%(levelname)s] Worker: %(message)s",
            "datefmt": "%d-%m-%Y %H:%M:%S",
        },
        "access": {
            "()": "logging.Formatter",
            "fmt": "%(asctime)s.%(msecs)03d Worker: %(message)s",
            "datefmt": "%d-%m-%Y %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "formatter": "generic",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "_granian": {"handlers": ["console"], "level": "INFO", "propagate": False},
        "granian.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
    },
}


def main(args: Namespace):
    Granian(
        "app_granian:app",
        interface=Interfaces.ASGI,
        workers=args.workers,
        address=args.address,
        port=args.port,
        threads=args.threads,
        blocking_threads=args.blocking_threads,
        log_dictconfig=LOGGING_CONFIG,
    ).serve()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Worker of Tiler Server")
    parser.add_argument(
        "--interface", type=str, default="asgi", help="provide an type of Interface"
    )
    parser.add_argument(
        "--workers", type=int, default=1, help="provide an number of workers"
    )
    parser.add_argument(
        "--threads", type=int, default=1, help="provide an number of threads"
    )
    parser.add_argument(
        "--blocking_threads",
        type=int,
        default=512,
        help="provide an number of blocking-threads",
    )
    parser.add_argument(
        "--address",
        type=str,
        default="127.0.0.1",
        help="provide an address",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9000,
        help="provide an port",
    )

    args = parser.parse_args()
    main(args)
