#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('CHANGELOG.md') as history_file:
    history = history_file.read()

setup(
    author="Alessio Bogon",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="File System Backends for Dogpile Cache",
    install_requires=[
        'dogpile.cache',
    ],
    license="MIT license",
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/markdown',
    include_package_data=True,
    keywords='dogpile_filesystem',
    name='dogpile_filesystem',
    packages=find_packages(include=['dogpile_filesystem']),
    test_suite='tests',
    url='https://github.com/paylogic/dogpile_filesystem',
    version='0.2.0',
    zip_safe=False,
    entry_points={
        'dogpile.cache': [
            'paylogic.filesystem = dogpile_filesystem.backend:GenericFSBackend',
            'paylogic.raw_filesystem = dogpile_filesystem.backend:RawFSBackend',
        ]
    },
)
