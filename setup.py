import os
from setuptools import setup

setup(
    name = "zfs",
    version = "0.0.2",
    author = "Geoff Davis",
    author_email = "geoff@ucsd.edu",
    license = "BSD",
    packages=['zfs',],
    scripts=['zfsautosnap','zfspurgesnapshots'],
)
