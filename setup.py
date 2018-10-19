# -*- coding: utf-8 -*-
import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ddmq",
    version="0.9.10",
    author="Martin Dahlö",
    author_email="m.dahlo@gmail.com",
    description="A file based serverless messaging queue",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ddmq/ddmq",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'pyyaml',
    ],

    entry_points={
        'console_scripts': ['ddmq = ddmq.cli:main',
        ],
    }
)
