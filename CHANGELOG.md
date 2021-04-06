# Changelog
All notable changes to this project will be documented in this file.

This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased
- Avoid `ResourceWarning: unclosed file <...>` warning by keeping a strong reference to the lock files. Lock files were weakly referenced on purpose, so that they could stay alive until the interpreter decides to remove them.
- Drop support for python 2.7 and 3.5. Only python >= 3.6 is supported now.

## 0.2.0
- Removed logic that falls back to copying a file when `file_movable=True` but the file does not appear to be movable. Do not try to be smart.

## 0.1.1
- Let PyPI know that we use markdown

## 0.1.0
- Initial release
