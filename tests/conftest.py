import pytest
from ys2wl.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings()
