from setuptools import setup 

setup(
    name='nyseconverter',
    version='1.0',
    description='NYSE Converter',
    url='https://github.com/ttteety/nyse-converter',
    author='Atit',
    author_email='ttteety@yahoo.com',
    license='MIT',
    packages=['nyseconverter'],
    zip_safe=False,
    install_requires=[
        'dask[complete]<=2023.3.0',
    ],
    entry_points={
        'console_scripts':['nysec=nyseconverter:main'],
    }
)