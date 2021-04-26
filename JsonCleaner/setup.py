from distutils.util import convert_path

from setuptools import setup

path_to_version_file = "version.py"
main_ns = {}
version_path = convert_path(path_to_version_file)
with open(version_path) as ver_file:
    exec(ver_file.read(), main_ns)
setup(
    name="json_cleaner",

    version=main_ns['__version__'],

    author="Yannick Radji",

    packages=["json_cleaner", "entities", "utils"],
)
