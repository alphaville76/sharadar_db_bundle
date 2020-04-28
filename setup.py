from setuptools import setup

setup(
    name='sharadar-zipline',
    version='1.0',
    packages=['sharadar', 'sharadar.data', 'sharadar.loaders', 'sharadar.pipeline', 'sharadar.util'],
    url='',
    license='',
    author='Costantino',
    author_email='',
    description='', install_requires=['numpy', 'pandas', 'quandl', 'zipline', 'toolz', 'memoization', 'logbook',
                                      'click', 'requests', 'six'],
    entry_points = {
                   'console_scripts': [
                       'sharadar-zipline = sharadar.__main__:main',
                   ],
               }
)
