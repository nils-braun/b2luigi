from setuptools import setup, find_packages

setup(
    name="b2luigi",
    version="0.1",
    packages=find_packages(),
    description='Luigi for Basf2',
    author='Nils Braun',
    author_email='nils.braun@kit.edu',
    install_requires=["luigi"],
)
