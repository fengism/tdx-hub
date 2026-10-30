"""Check project-owned TDX sources, licenses and release dependency metadata."""

import argparse
import ast
import re
import tarfile
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[1]
TDX = "tdxhub/tdx/"


def check_contents(read, names, metadata_path, version):
    metadata = BytesParser().parsebytes(read(metadata_path))
    assert metadata["Name"] == "tdxhub-sdk"
    assert metadata["Version"] == version
    for requirement in metadata.get_all("Requires-Dist", []):
        name = re.split(r"[\s\[<>=!~;(]", requirement, maxsplit=1)[0].lower()
        assert name not in {"tdxpy", "pytdx", "cython"}, requirement

    assert not any(
        name.startswith(("tdxhub/_vendor/", "tdxhub/_tdx/", "tdxhub/licenses/", "tdxpy/", "pytdx/", "tdx/"))
        or name in {"tdxhub/tdxpy_compat.py", TDX + "README.md", TDX + "UPSTREAM.json"}
        for name in names
    ), "obsolete package"
    assert not any(name.startswith("tdxhub/") and name.endswith((".so", ".pyd", ".pyc", ".dll")) for name in names), (
        "compiled package residue"
    )

    # Reject stale/missing Python modules, including leftover compiled extensions.
    expected_sources = {path.relative_to(ROOT).as_posix() for path in (ROOT / "tdxhub").rglob("*.py")}
    packaged_sources = {name for name in names if name.startswith("tdxhub/") and name.endswith(".py")}
    assert packaged_sources == expected_sources, packaged_sources ^ expected_sources
    for name in expected_sources | {"tdxhub/utils/holiday.js"}:
        assert name in names, name
        assert read(name) == (ROOT / name).read_bytes(), name

    # Root license documents are authoritative; do not hard-code a historical author list.
    assert metadata["License-Expression"] == "MIT"
    assert set(metadata.get_all("License-File", [])) == {"LICENSE", "AUTHORS.rst"}
    license_dir = (
        PurePosixPath(metadata_path).parent / "licenses" if metadata_path.endswith("/METADATA") else PurePosixPath(".")
    )
    for filename in ("LICENSE", "AUTHORS.rst"):
        name = (license_dir / filename).as_posix()
        assert name in names, name
        assert read(name) == (ROOT / filename).read_bytes(), name
    license_text = read((license_dir / "LICENSE").as_posix()).decode("utf-8")
    for notice in (
        "MIT License",
        "Copyright (c)",
        "Permission is hereby granted",
        "The above copyright notice",
        'THE SOFTWARE IS PROVIDED "AS IS"',
    ):
        assert notice in license_text, f"LICENSE: missing {notice}"

    # The root README is shipped as sdist source and wheel/sdist long description.
    assert metadata["Description-Content-Type"] == "text/markdown"
    payload = metadata.get_payload(decode=True).decode("utf-8").replace("\r\n", "\n").rstrip("\n")
    readme = (ROOT / "README.md").read_text(encoding="utf-8").replace("\r\n", "\n").rstrip("\n")
    assert payload == readme
    if metadata_path == "PKG-INFO":
        assert "README.md" in names
        assert read("README.md") == (ROOT / "README.md").read_bytes()


def read_source_version():
    """不执行包导入, 静态提取 tdxhub/__init__.py 中的 __version__ 赋值。"""

    source = (ROOT / "tdxhub" / "__init__.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError("tdxhub/__init__.py 缺少 __version__ 字面量赋值")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("sdist", type=Path)
    args = parser.parse_args()
    version = read_source_version()
    with zipfile.ZipFile(args.wheel) as archive:
        names = set(archive.namelist())
        metadata = next(name for name in names if name.endswith(".dist-info/METADATA"))
        check_contents(archive.read, names, metadata, version)
    with tarfile.open(args.sdist) as archive:
        members = {member.name.split("/", 1)[1]: member for member in archive.getmembers() if member.isfile()}

        def read(name):
            with archive.extractfile(members[name]) as source:
                return source.read()

        check_contents(read, set(members), "PKG-INFO", version)
    print("wheel/sdist: metadata, current Python source bytes, README and licenses verified")


if __name__ == "__main__":
    main()
