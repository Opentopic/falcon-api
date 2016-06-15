from setuptools import find_packages, setup


with open('requirements.txt') as f:
    REQUIREMENTS = f.read().splitlines()

setup(
    name='opentopic-falcon-api',
    version='0.1.1',
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
    install_requires=REQUIREMENTS,
)
