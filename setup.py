import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

description = "Utilities for building and testing AWS applications in Python"

setuptools.setup(
    name="awstin",
    version="0.0.1",
    author="Kevin Duff",
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://https://github.com/k2bd/awstin",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "boto3",
    ],
)
