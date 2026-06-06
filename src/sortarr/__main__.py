import logging
import sys

import uvicorn
from sortarr.config import load_settings


def _configure_logging(level: str, log_file: str) -> None:
    root = logging.getLogger("sortarr")
    if root.handlers:
        return
    root.setLevel(level.upper())

    handler: logging.Handler
    if log_file == "stream":
        handler = logging.StreamHandler(sys.stdout)
    else:
        handler = logging.FileHandler(log_file)

    handler.setLevel(level.upper())
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    root.addHandler(handler)


def main() -> None:
    settings = load_settings()
    _configure_logging(settings.log_level, settings.log_file)
    uvicorn.run(
        "sortarr.api.app:create_app",
        host="0.0.0.0",
        port=settings.api_port,
        factory=True,
        log_level=settings.log_level,
    )


if __name__ == "__main__":
    main()
