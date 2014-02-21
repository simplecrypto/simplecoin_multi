#!/usr/bin/env python

from setuptools import setup, find_packages
from pip.req import parse_requirements

install_reqs = parse_requirements("requirements.txt")
reqs = [str(ir.req) for ir in install_reqs]

setup(name='simpledoge',
      version='0.1.0',
      description='Dogecoin mining with no registration required.',
      author='Eric Cook',
      author_email='eric@simpload.com',
      url='http://www.simpledoge.com',
      install_requires=reqs,
      entry_points={
          'console_scripts': [
              'sd_rpc = simpledoge.rpc:entry'
          ]
      },
      packages=find_packages()
      )
