# PyPI

## How to build and submit locally

**Ensure you do not have Goliath installed from PyPI!** (`pip uninstall goliath` until it skips)

Just run `./make-package.sh`.

## How to build and submit to PyPI

1. Modify `pypi.py` with a new version name and any other necessary changes
2. Install necessary build packages: `python3 -m pip install --upgrade setuptools wheel twine`
3. Run `./make-package.sh <VERSION>` where `<VERSION>` is like `1.2.3` (see [semantic versioning](https://semver.org/)) to build
4. Run `python3 -m twine --skip-existing dist/goliath-<VERSION>*` (inserting your version number) to upload