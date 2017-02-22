#!/usr/bin/env python

"""pydistcp: python WebHDFS inter/intra-cluster data copy tool."""

import os
import sys
import re
from setuptools import find_packages, setup

sys.path.insert(0, os.path.abspath('src'))

def _get_version():
  """Extract version from package."""
  with open('pydistcp/__init__.py') as reader:
    match = re.search(
      r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
      reader.read(),
      re.MULTILINE
    )
    if match:
      return match.group(1)
    else:
      raise RuntimeError('Unable to extract version.')

def _get_long_description():
  """Get README contents."""
  with open('README.md') as reader:
    return reader.read()

setup(
  name='pydistcp',
  version=_get_version(),
  description=__doc__,
  long_description=_get_long_description(),
  author='Yassine Azzouz',
  author_email='yassine.azzouz@agmail.com',
  url='https://github.com/yassineazzouz/pydistcp',
  license='MIT',
  packages=find_packages(),
  classifiers=[
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2.6',
    'Programming Language :: Python :: 2.7',
    'Programming Language :: Python :: 3.3',
    'Programming Language :: Python :: 3.4',
  ],
  install_requires=[
    'docopt',
    'pywhdfs>=1.0.0',
  ],
  entry_points={'console_scripts': 
     [ 'pydistcp = pydistcp.__main__:main' ]
  },
)
