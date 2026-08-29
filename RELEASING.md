# Releasing BeeTea

1. Select a license, add its full text as `LICENSE`, and update the license
   fields in `pyproject.toml` and `package.xml`.
2. Set the release version in `beetea/__init__.py` and `package.xml`.
3. Run the test suite in both a plain Python environment and the supported ROS
   2 environment.
4. Build fresh artifacts with `python -m build`.
5. Validate them with `python -m twine check dist/*` and inspect their contents.
6. Upload to TestPyPI and verify installation in a clean environment.
7. Tag the release, then upload the unchanged artifacts to PyPI.

PyPI does not allow an uploaded version to be replaced. Always increment the
version before rebuilding a release that was already uploaded.
