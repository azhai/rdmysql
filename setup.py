import sys
from os.path import join, dirname
from setuptools import setup, find_packages
sys.path.insert(0, join(dirname(__file__), 'src'))
from rdmysql import __version__


setup(
    name="rdmysql",
    version=__version__,
    description="a simple db layer based on ultra-mysql",
    author="Ryan Liu",
    author_email="azhai (at) 126 (dot) com",
    url="https://github.com/azhai/rdmysql",
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
        "License :: OSI Approved :: MIT License",
    ],
    keywords=["mysql", "database", "model"],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=[
        "umysql>2.61",
    ],
    dependency_links=[
        "git+ssh://git@github.com/azhai/ultramysql.git@2.62.dev0#egg=mayo-2.62.dev0",
    ]
)
