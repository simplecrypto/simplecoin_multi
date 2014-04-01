#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='simplecoin',
      version='0.4.0',
      description='Dogecoin mining with no registration required.',
      author='Eric Cook',
      author_email='eric@simpload.com',
      url='http://www.simpledoge.com',
      entry_points={
          'console_scripts': [
              'sc_rpc = simplecoin.rpc:entry'
          ]
      },
      packages=find_packages()
      )
