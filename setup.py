from setuptools import setup, find_packages
setup(
    name='clearwing',
    version='1.0',
    packages=find_packages(),
    install_requires=[
        'numpy==1.6.2',
        'pandas==0.8.0',
        'matplotlib',
    ]
)

