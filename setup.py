#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-google-analytics",
    version="0.0.1",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_analytics"],
    install_requires=[
        "singer-python==5.9.0",
        "requests==2.22.0"
    ],
    extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint==2.1.1',
            'astroid==2.1.0',
            'nose'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-google-analytics=tap_google_analytics:main
    """,
    packages=["tap_google_analytics"],
    package_data = {
        # Add any hardcoded schemas here
    },
    include_package_data=True,
)
