#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

setup(
    author="Alessio Bogon",
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="File System Backend for Dogpile Cache",
    install_requires=[
        'dogpile.cache',
        'six',
    ],
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='dogpile_cache_fs_backend',
    name='dogpile_cache_fs_backend',
    packages=find_packages(include=['dogpile_cache_fs_backend']),
    test_suite='tests',
    url='https://github.com/youtux/dogpile_cache_fs_backend',
    version='0.1.0',
    zip_safe=False,
    entry_points={
        'dogpile.cache': [
            'paylogic.fs_backend = dogpile_cache_fs_backend.backend:FSBackend',
        ]
    },
)
