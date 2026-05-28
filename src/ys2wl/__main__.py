import uvicorn
from ys2wl.config import load_settings


def main() -> None:
    settings = load_settings()
    uvicorn.run(
        "ys2wl.api.app:create_app",
        host="0.0.0.0",
        port=settings.api_port,
        factory=True,
        log_level=settings.log_level,
    )


if __name__ == "__main__":
    main()
