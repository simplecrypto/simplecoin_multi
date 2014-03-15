#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='simpledoge',
      version='0.3.0',
      description='Dogecoin mining with no registration required.',
      author='Eric Cook',
      author_email='eric@simpload.com',
      url='http://www.simpledoge.com',
      entry_points={
          'console_scripts': [
              'sd_rpc = simpledoge.rpc:entry'
          ]
      },
      packages=find_packages()
      )
