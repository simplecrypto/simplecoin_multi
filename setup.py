#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='simplecoin',
      version='0.8.1-dev',
      description='Dogecoin mining with no registration required.',
      author='Eric Cook',
      author_email='eric@simpload.com',
      url='http://www.simpledoge.com',
      entry_points={
          'console_scripts': [
              'simplecoin_scheduler = simplecoin.scheduler:main'
          ]
      },
      packages=find_packages()
      )
