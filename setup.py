import ndio

VERSION = ndio.version
"""
roll with
git tag VERSION
git push --tags
python setup.py sdist upload -r pypi
"""

from distutils.core import setup
from setuptools import setup
setup(
    name = 'ndio',
    packages = [
        'ndio',
        'ndio.convert',
        'ndio.ramon',
        'ndio.remote',
        'ndio.utils'
    ],
    version = VERSION,
    description = 'A Python library for open neuroscience data access and manipulation.',
    author = 'Jordan Matelsky',
    author_email = 'jordan@neurodata.io',
    url = 'https://github.com/openconnectome/ndio',
    download_url = 'https://github.com/openconnectome/ndio/tarball/' + VERSION,
    keywords = [
        'brain',
        'medicine',
        'microscopy',
        'neuro',
        'neurodata',
        'neurodata.io',
        'neuroscience',
        'ndio',
        'ocpy',
        'ocp',
        'ocp.me',
        'connectome',
        'connectomics',
        'spatial',
        'EM',
        'MRI',
        'fMRI',
        'calcium'
    ],
    install_requires = [
        'pillow',
        'numpy',
        'h5py',
        'requests',
        'networkx',
        'scipy',
        'pymcubes',
        'json-spec'
    ],
    classifiers = [],
)
