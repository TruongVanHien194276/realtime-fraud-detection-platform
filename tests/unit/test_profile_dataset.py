from pathlib import Path

import pytest

from scripts.profile_dataset import profile_dataset


def test_profile_input_must_be_csv_file(tmp_path: Path) -> None:
    with pytest.raises(IsADirectoryError):
        profile_dataset(tmp_path)
