from setuptools import find_packages, setup


setup(
    name='opentopic-falcon-api',
    version='0.4.10',
    author='Tomasz Roszko',
    author_email='tom@opentopic.com',
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
        'falcon>=1.0.0',
        'python-rapidjson>=0.0.6',
    ],
    tests_require=[
        'mongoengine==0.10.6',
        'SQLAlchemy>=1.0.12',
        'elasticsearch-dsl==2.1.0',
        'elasticsearch==2.3.0'
    ],
)
