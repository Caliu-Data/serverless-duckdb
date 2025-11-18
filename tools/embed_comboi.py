from __future__ import annotations

import shutil
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    source = repo_root / "src" / "comboi"
    destination = repo_root / "azure_functions" / "shared_packages" / "comboi"

    if not source.exists():
        raise FileNotFoundError(f"Source package not found: {source}")

    if destination.exists():
        shutil.rmtree(destination)

    shutil.copytree(source, destination)
    print(f"Copied {source} -> {destination}")


if __name__ == "__main__":
    main()

