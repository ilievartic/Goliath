import setuptools

with open("README.md", "r") as fh:
        long_description = fh.read()

setuptools.setup(
    name='goliath',  
    version='0.1.1',
    scripts=['goliath'] ,
    author="Ilie Vartic",
    author_email="ilie.vartic@gmail.com",
    description="This package enables python coders to build \"multi-threaded\" programs and optimize their data processing.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ilievartic/Goliath/archive/0.1.tar.gz",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
