#!/usr/bin/env python
from setuptools import setup, find_packages

REQUIRES = []

setup(
    name='MultiProcessPipeline',
    version='0.1',
    url='https://github.com/bshakur8/multiprocess_pipeline',
    author='Bhaa Shakur',
    author_email='bhaa.shakur@gmail.com',
    license='MIT',
    packages=find_packages('.'),
    entry_points={'console_scripts': ['multiprocess_pipeline=main:main']},
    zip_safe=False,
    install_requires=REQUIRES
)
