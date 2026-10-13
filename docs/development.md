# Development

## Tests

The core test suite has no dependencies:

```console
python -m unittest beetea.tests.test_bt
```

or, with pytest installed (`pip install -e '.[test]'`):

```console
pytest
```

The ROS adapter tests (`beetea/tests/test_bt_ros.py`) also need `svea_core`
to be importable.

## Documentation

This site is built with [Zensical](https://zensical.org). API pages are
generated from the docstrings with mkdocstrings.

```console
pip install -e '.[docs]'
zensical serve      # live preview at http://localhost:8000
zensical build      # static site in ./site
```

Docstrings use the Google style (`Args:`, `Returns:`, `Raises:`).

## Releasing

See `RELEASING.md` in the repository root. In short: set the version in
`beetea/__init__.py` and `package.xml`, run the tests in plain Python and in
ROS 2, then run `python -m build` and `twine check`. Upload to TestPyPI first,
then to PyPI.
