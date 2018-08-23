# -*- coding: utf-8 -*-

# python std lib
import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# if you are using vagrant, just delete os.link directly,
# The hard link only saves a little disk space, so you should not care
if os.getenv('USER', '').lower() == 'vagrant':
    del os.link

with open('README.md') as f:
    readme = f.read()
with open(os.path.join('docs', 'release-notes.rst')) as f:
    history = f.read()

setup(
    name="redis-py-cloud",
    version="1.3.8",
    description="Cluster library for redis 5.0.0 built on top of redis-py lib",
    long_description=readme + '\n\n' + history,
    author="Max Hua",
    author_email="zhhua110@163.com",
    maintainer='Max Hua',
    maintainer_email='zhhua110@163.com',
    packages=["rediscluster"],
    url='https://github.com/ChinaGoldBear/redis-py-cloud',
    license='MIT',
    install_requires=[
        'redis>=2.10.6'
    ],
    keywords=[
        'redis',
        'redis cluster',
    ],
    classifiers=[
        # As from https://pypi.python.org/pypi?%3Aaction=list_classifiers
        # 'Development Status :: 1 - Planning',
        # 'Development Status :: 2 - Pre-Alpha',
        # 'Development Status :: 3 - Alpha',
        # 'Development Status :: 4 - Beta',
        'Development Status :: 5 - Production/Stable',
        # 'Development Status :: 6 - Mature',
        # 'Development Status :: 7 - Inactive',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Environment :: Web Environment',
        'Operating System :: POSIX',
        'License :: OSI Approved :: MIT License',
    ]
)
