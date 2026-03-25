try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional in minimal environments
    def load_dotenv(*args, **kwargs):
        return False


load_dotenv()

import pytest

try:
    import activemodel
except Exception:  # pragma: no cover - optional in unit-only environments
    activemodel = None


def _db_utils_available() -> bool:
    if activemodel is None:
        return False
    try:
        from . import models  # noqa: F401
        from .utils import database_url, drop_all_tables  # noqa: F401
    except Exception:
        return False
    return True


def pytest_sessionstart(session):
    "only executes once if a test is run, at the beginning of the test suite execution"

    if not _db_utils_available():
        return

    from .utils import database_url, drop_all_tables

    activemodel.init(database_url())
    drop_all_tables()


@pytest.fixture(scope="function")
def create_and_wipe_database():
    if not _db_utils_available():
        pytest.skip("database dependencies not available in this environment")

    from .utils import temporary_tables

    with temporary_tables():
        yield


@pytest.fixture
def engine():
    if activemodel is None:
        pytest.skip("activemodel dependency not available in this environment")
    return activemodel.get_engine()
