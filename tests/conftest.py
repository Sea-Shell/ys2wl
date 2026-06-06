import pytest
from sortarr.config import Settings


@pytest.fixture
def settings() -> Settings:
    return Settings()
