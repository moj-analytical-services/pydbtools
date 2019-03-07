from setuptools import setup, find_packages

setup(
    name='pydbtools',
    version='0.0.1',
    packages=find_packages(exclude=['tests*']),
    license='MIT',
    description='A python package to query data via amazon athena and bring it into a pandas df',
    long_description=open('README.md').read(),
    install_requires=[
        "boto3 >= 1.7.4",
        "pandas >= 0.23.4",
        "numpy >= 1.16.1",
        "s3fs >= 0.1.6",
        "gluejobutils >= v1.0.0"
    ],
    include_package_data=True,
    url='https://github.com/moj-analytical-services/pydbtools',
    author='Karik Isichei',
    author_email='karik.isichei@digital.justice.gov.uk'
)