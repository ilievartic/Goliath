import setuptools
from sys import stderr
import re
import os

if __name__ == '__main__':
    version = os.getenv('VERSION', '')
    if not re.match(r'^[0-9]+\.[0-9]+\.[0-9]+$', version):
        print('Please provide a version number in the format #.#.#', file=stderr)
        exit(1)
        
    with open("README.md", "r") as fh:
        long_description = fh.read()

    setuptools.setup(
        name='goliath',  
        version=version,
        author="Ilie Vartic, Logan Pulley, Zach Oldham, Deepan Venkatesh, Manikandan Swaminathan",
        author_email="ilie.vartic@gmail.com",
        description="This package enables Python to offload sets of function calls to pools of remote worker processes.",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/ilievartic/Goliath/",
        packages=setuptools.find_packages(),
        classifiers=[
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
            "Operating System :: OS Independent",
        ],
    )
