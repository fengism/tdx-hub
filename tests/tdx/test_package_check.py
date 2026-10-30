"""Release checks must detect stale modules and mismatched source/resource bytes."""

import pytest

from scripts import check_package
from scripts.check_package import ROOT, TDX, check_contents
from tdxhub import __version__ as VERSION


@pytest.fixture(params=["wheel", "sdist"])
def package_contents(request):
    files = {path.relative_to(ROOT).as_posix(): path.read_bytes() for path in (ROOT / "tdxhub").rglob("*.py")}
    files["tdxhub/utils/holiday.js"] = (ROOT / "tdxhub/utils/holiday.js").read_bytes()
    if request.param == "wheel":
        metadata_path = f"tdxhub_sdk-{VERSION}.dist-info/METADATA"
        license_dir = f"tdxhub_sdk-{VERSION}.dist-info/licenses/"
    else:
        metadata_path = "PKG-INFO"
        license_dir = ""
        files["README.md"] = (ROOT / "README.md").read_bytes()
    for filename in ("LICENSE", "AUTHORS.rst"):
        files[license_dir + filename] = (ROOT / filename).read_bytes()
    files[metadata_path] = (
        f"Name: tdxhub-sdk\nVersion: {VERSION}\nLicense-Expression: MIT\n"
        "License-File: LICENSE\nLicense-File: AUTHORS.rst\n"
        "Description-Content-Type: text/markdown\n\n"
    ).encode() + (ROOT / "README.md").read_bytes()
    return files


def metadata_path(files):
    return next(name for name in files if name == "PKG-INFO" or name.endswith("/METADATA"))


def check(files):
    check_contents(files.__getitem__, set(files), metadata_path(files), VERSION)


def test_current_sources_pass(package_contents):
    check(package_contents)


@pytest.mark.parametrize(
    "name",
    [
        "tdxhub/_vendor/README.md",
        "tdxhub/_tdx/__init__.py",
        "tdxhub/tdxpy_compat.py",
        "tdxhub/tdx/version.py",
        "tdxhub/tdx/parser.so",
        "tdxhub/tdx/README.md",
        "tdxhub/tdx/UPSTREAM.json",
        "tdxhub/licenses/__init__.py",
        "tdxhub/licenses/tdxpy-LICENSE",
        "tdxhub/licenses/tdxpy-UPSTREAM.json",
        "tdx/__init__.py",
    ],
)
def test_obsolete_or_compiled_artifacts_fail(package_contents, name):
    package_contents[name] = b"stale"
    with pytest.raises(AssertionError):
        check(package_contents)


@pytest.mark.parametrize("dependency", ["tdxpy", "pytdx", "Cython"])
def test_external_dependency_fails(package_contents, dependency):
    name = metadata_path(package_contents)
    package_contents[name] = f"Requires-Dist: {dependency}>=1\n".encode() + package_contents[name]
    with pytest.raises(AssertionError):
        check(package_contents)


@pytest.mark.parametrize("mutation", ["missing", "changed"])
@pytest.mark.parametrize("resource", [TDX + "client.py", "tdxhub/utils/holiday.js", "LICENSE", "AUTHORS.rst"])
def test_source_or_resource_drift_fails(package_contents, mutation, resource):
    name = next(name for name in package_contents if name == resource or name.endswith("/" + resource))
    if mutation == "missing":
        del package_contents[name]
    else:
        package_contents[name] += b"\n# drift\n"
    with pytest.raises(AssertionError):
        check(package_contents)


@pytest.mark.parametrize("mutation", ["missing", "changed"])
def test_readme_drift_fails(package_contents, mutation):
    name = metadata_path(package_contents)
    headers, _, _ = package_contents[name].partition(b"\n\n")
    package_contents[name] = headers + b"\n\n" + (b"" if mutation == "missing" else b"old README\n")
    with pytest.raises(AssertionError):
        check(package_contents)


@pytest.mark.parametrize("filename", ["LICENSE", "AUTHORS.rst"])
def test_license_metadata_required(package_contents, filename):
    name = metadata_path(package_contents)
    package_contents[name] = package_contents[name].replace(f"License-File: {filename}\n".encode(), b"")
    with pytest.raises(AssertionError):
        check(package_contents)


def test_sdist_root_readme_required(package_contents):
    if "PKG-INFO" not in package_contents:
        name = metadata_path(package_contents)
        package_contents["PKG-INFO"] = package_contents.pop(name)
        prefix = name.removesuffix("METADATA") + "licenses/"
        for filename in ("LICENSE", "AUTHORS.rst"):
            package_contents[filename] = package_contents.pop(prefix + filename)
    else:
        del package_contents["README.md"]
    with pytest.raises(AssertionError):
        check(package_contents)


@pytest.fixture
def source_tree(package_contents, tmp_path, monkeypatch):
    """Isolate root-document edits without changing the real working tree."""
    for name, content in package_contents.items():
        if name.startswith("tdxhub/"):
            path = tmp_path / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
    for filename in ("README.md", "LICENSE", "AUTHORS.rst"):
        (tmp_path / filename).write_bytes((ROOT / filename).read_bytes())
    monkeypatch.setattr(check_package, "ROOT", tmp_path)
    return tmp_path


@pytest.mark.parametrize("filename", ["LICENSE", "AUTHORS.rst"])
def test_latest_root_documents_are_authoritative(package_contents, source_tree, filename):
    path = source_tree / filename
    if filename == "LICENSE":
        current = path.read_bytes().replace(b"Copyright (c) 2017, tdxhub", b"Copyright (c) 2026, Example contributors")
    else:
        current = "开发团队\n==============\n\n开发者列表\n----------------\n\nExample contributors\n".encode()
    assert current != path.read_bytes()
    path.write_bytes(current)

    # An otherwise valid release with yesterday's documents must fail.
    with pytest.raises(AssertionError, match=filename):
        check(package_contents)
    name = next(name for name in package_contents if name == filename or name.endswith("/" + filename))
    package_contents[name] = current
    check(package_contents)


def test_matching_license_without_mit_terms_fails(package_contents, source_tree):
    current = b"MIT License\nCopyright (c) 2017, tdxhub\n"
    (source_tree / "LICENSE").write_bytes(current)
    name = next(name for name in package_contents if name == "LICENSE" or name.endswith("/LICENSE"))
    package_contents[name] = current
    with pytest.raises(AssertionError, match="LICENSE: missing Permission is hereby granted"):
        check(package_contents)
