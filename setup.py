from setuptools import setup, find_packages


setup(
    name='python-rabid.mongoose',
    version='0.2',
    url='https://github.com/lmco/rabid.mongoose',
    description='A REST interface for MongoR',
    author='Daniel Bauman',
    author_email='Daniel.Bauman@lmco.com',
    license='MIT',
    keywords='mongo http rest json proxy'.split(),
    platforms='any',
    entry_points = {
        'console_scripts': [
            'httpf = rabidmongoose.httpf:main',
        ],
    },
    packages=find_packages(exclude=['t']),
    include_package_data=True,
)

