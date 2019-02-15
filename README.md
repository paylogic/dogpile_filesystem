# File System Backends for Dogpile Cache


[![Build Status](https://travis-ci.org/paylogic/dogpile_filesystem.svg?branch=master)](https://travis-ci.org/paylogic/dogpile_filesystem)
[![Coverage Status](https://coveralls.io/repos/github/paylogic/dogpile_filesystem/badge.svg?branch=master)](https://coveralls.io/github/paylogic/dogpile_filesystem?branch=master)



Filesystem-based backends for dogpile cache.

The generic variant of the backend, `paylogic.filesystem`, will accept any picklable value and it will store it in the file system.

The raw variant `paylogic.raw_filesystem` will only work with file-like values and it will avoid the pickling phase. This is useful when you are generating a big file and you don't want to keep in memory the contents of this file.
 
Both variants use [fcntl.lockf](https://docs.python.org/3.7/library/fcntl.html#fcntl.lockf) operations, therefore it is compatible with  UNIX-like systems only.
The lockf system call allows to allocate an arbitrary number of locks using the same file, avoiding problems that arise when deleting lock files.


## Installation
Install with pip:

`$ pip install dogpile_filesystem`

## Usage
### Generic variant
Configure a region to use `paylogic.filesystem`:
```python
from dogpile.cache import make_region
import datetime

region = make_region().configure(
    'paylogic.filesystem',
    arguments = {
        "base_dir": "/path/to/cachedir",  # Make sure this directory is only for this region
        # Optional parameters
        "cache_size": 1024**3,  # Defaults to 1 Gb
        "expiration_time": datetime.timedelta(seconds=30),  # Defaults to no expiration
        "distributed_lock": True,  # Defaults to true
    }
)

@region.cache_on_arguments()
def my_function(args):
    return 42
```

### Raw variant
Configure a region to use dogpile_filesystem:
```python
from dogpile.cache import make_region
import datetime
import tempfile

region = make_region().configure(
    'paylogic.raw_filesystem',
    arguments = {
        "base_dir": "/path/to/cachedir",  # Make sure this directory is only for this region
        # Optional parameters
        "cache_size": 1024**3,  # Defaults to 1 Gb
        "file_movable": True,  # Whether the backend can freely move the file.
                               # When True, the backend will move the file to the cache
                               # directory directly using os.rename(file.name).
                               # When False (default), the content of the file will be copied to 
                               # the cache directory.
        "expiration_time": datetime.timedelta(seconds=30),  # Defaults to no expiration
        "distributed_lock": True,  # Defaults to true
    }
)

@region.cache_on_arguments()
def big_file_operation(args):
    # When using `file_movable=True`, we must make sure that NamedTemporaryFile does not delete the file on close,
    # otherwise it will complain that it cannot find the file.
    f = tempfile.NamedTemporaryFile(delete=False)
    # fill the file
    f.flush()
    f.seek(0)
    return f
```

## Development
Install the dev requirements and the project in development mode:

`$ pip install -r requirements_dev.txt -e .`

Run tests:

`$ pytest tests`

Optionally run tests for all supported configurations:

`$ tox`
