#from distutils.core import setup
from setuptools import setup

setup(
    name='ElasticsearchIndex',
    version='0.1.0',
    author='Ashish Hunnargikar',
    author_email='ahunnargikar@ebay.com',
    packages=['elasticsearchindex'],
    #scripts=['bin/elasticsearchindex.py'],
    #url='http://pypi.python.org/pypi/ElasticsearchIndex/',
    license='LICENSE.txt',
    description='Elasticsearch backend search module for the Docker registry',
    long_description=open('README.txt').read(),
    install_requires=[
        "elasticsearch>=1.0.0,<2.0.0",
        "kazoo>=2.0"
        ]
)