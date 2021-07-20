#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-google-analytics",
    version="1.0.4",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_analytics"],
    install_requires=[
        "singer-python==5.9.0",
        "requests==2.22.0",
        "backoff==1.8.0",
        "jwt==0.6.1"
    ],
    extras_require={
        'dev': [
            'ipdb==0.11',
            'pylint',
            'nose'
        ]
    },
    entry_points="""
    [console_scripts]
    tap-google-analytics=tap_google_analytics:main
    """,
    packages=["tap_google_analytics"],
    package_data = {
        "tap_google_analytics": ["tap_google_analytics/ga_cubes.json"]
    },
    include_package_data=True,
)
