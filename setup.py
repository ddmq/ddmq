# -*- coding: utf-8 -*-
import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ddmq",
    version="0.8.1",
    author="Martin Dahlö",
    author_email="m.dahlo@gmail.com",
    description="A file based serverless messaging queue",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ddmq/ddmq",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pyyaml',
    ],
)
