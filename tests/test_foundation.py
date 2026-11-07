from pathlib import Path
from platform import python_version


def test_python_matches_project_pin():
    project_root = Path(__file__).resolve().parents[1]
    pinned_version = (project_root / ".python-version").read_text().strip()

    assert python_version() == pinned_version
