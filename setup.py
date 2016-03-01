from setuptools import find_packages, setup

setup(
    name='opentopic-falcon-api',
    version='0.1',
    author='Tomasz Roszko',
    author_email='tom@opentopic',
    description='Base Library for services api endpoints',
    url='http://git.opentopic.com/backend/falcon-api',
    license='GNU GENERAL PUBLIC LICENSE',
    platforms=['OS Independent'],
    packages=find_packages(),
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.x',
    ],
    install_requires=[
        'cython==0.23.4',
        'falcon==0.3.0',
        'recommonmark==0.4.0',
        'Sphinx==1.3.5',
        'sphinx-autobuild==0.6.0',
        'sphinx-rtd-theme==0.1.9',
    ],
)