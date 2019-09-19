# resticserver
A simple restic REST backend implemented in Python 3.

## Overview

This is a simple script intended to be run on a local machine for a short period of time to do offsite backups to an external hard drive.
Because of this there are no security measures like password restrictions or SSL.

## Dependencies

waitress - (https://github.com/Pylons/waitress) required to be either pip installed in the environment you are using or a copy of the files could be directly included.

## Usage

1. Copy the `resticserver.py` file somewhere accessible. Possibly the parent path to where you want the repositories to be.
1. Create a `config.py` file and specify one variable: `ROOT_PATH = '/path/to/repositories'`
1. Install `waitress` with pip (`pip install waitress`) or copy waitress package into the same directory.
1. Run `python resticserver.py`

NOTE: `resticserver.py` contains a WSGI Application so technically `waitress` is not required and this could be run as a WSGI app in a different WSGI server.