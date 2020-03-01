# PyPI

## How to build and submit distributions to PyPI

1. Modify `pypi.py` with a new version name and any other necessary changes
2. Install necessary build packages: `python3 -m pip install --upgrade setuptools wheel twine`
3. Run `python3 pypi.py sdist bdist_wheel`
4. Run `python3 -m twine --skip-existing dist/*`