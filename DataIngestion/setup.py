from distutils.util import convert_path

from setuptools import setup

path_to_version_file = "version.py"
main_ns = {}
version_path = convert_path(path_to_version_file)
with open(version_path) as ver_file:
    exec(ver_file.read(), main_ns)
setup(
    name="dataingestion",

    version=main_ns['__version__'],

    author="Yannick Radji",

    packages=["data_ingestion", "entities", "utils"],
)
