"""
The setup for installation of the BikesData package.
Helpful resource:
https://docs.python.org/3.6/distutils/setupscript.html
"""
from setuptools import setup

setup(
    name="BikesData",
    version=0.1,
    description="Fetches and stores data about Dublin Bikes from the jcdecaux API",
    author="Team 12",
    keywords="Dublin bikes data getter",
    packages=["BikesData"],
    install_requires=open("requirements.txt", "rt").readlines(),
    entry_points={'console_scripts': ['bikes-data = BikesData.main:main']}
)
