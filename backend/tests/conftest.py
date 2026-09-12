import pytest
import sys
from pathlib import Path

# Add backend directory to sys.path
backend_dir = Path(__file__).resolve().parent.parent
if str(backend_dir) not in sys.path:
    sys.path.insert(0, str(backend_dir))

try:
    from app.main import app
except ImportError:
    from backend.app.main import app

from fastapi.testclient import TestClient

@pytest.fixture
def client():
    return TestClient(app)
