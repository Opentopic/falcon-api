#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='opentopic-falcon-api',
    version='1.1.29',
    author='Jan Waś',
    author_email='jan.was@opentopic.com',
    description='Falcon API resources for databases',
    url='http://git.opentopic.com/backend/falcon-api',
    zip_safe=True,
    license='MIT',
    platforms=['OS Independent'],
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.x',
    ],
    install_requires=[
        'falcon>=1.0.0',
    ],
    tests_require=[
        'mongoengine==0.10.6',
        'SQLAlchemy>=1.0.12',
        'alchemyjsonschema>=0.4.2',
        'elasticsearch-dsl==2.1.0',
        'elasticsearch==2.3.0'
    ],
)
