#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os
import sys
from shutil import rmtree

from setuptools import find_packages, setup, Command

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.rst' is present in your MANIFEST.in file!
with io.open(os.path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = '\n' + f.read()

version = os.path.join(here, 'falcon_dbapi', '__init__.py')
about = {}
with open(version, 'r') as f:
    exec(f.read(), about)


class UploadCommand(Command):
    """Support setup.py upload."""

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(here, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPi via Twine…')
        os.system('twine upload dist/*')

        sys.exit()


setup(
    name=about['__title__'],
    version=about['__version__'],
    description='Falcon API resources for databases',
    long_description=long_description,
    author='Jan Waś',
    author_email='jan.was@opentopic.com',
    url='https://github.com/opentopic/falcon-api',
    platforms=['OS Independent'],
    packages=find_packages(exclude=('tests',)),
    install_requires=[
        'falcon>=1.3.0',
    ],
    include_package_data=True,
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    setup_requires=[
        'pytest-runner',
        'recommonmark',
    ],
    tests_require=[
        'pytest',
        'mongoengine==0.10.6',
        'SQLAlchemy>=1.0.12',
        'alchemyjsonschema>=0.4.2',
        'elasticsearch-dsl==2.1.0',
        'elasticsearch==2.3.0'
    ],
    # $ setup.py publish support.
    cmdclass={
        'upload': UploadCommand,
    },
)
