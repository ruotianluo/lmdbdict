import setuptools
from os import path

# from detectron2
def get_version():
    init_py_path = path.join(path.abspath(path.dirname(__file__)), "lmdbdict", "__init__.py")
    init_py = open(init_py_path, "r").readlines()
    version_line = [l.strip() for l in init_py if l.startswith("__version__")][0]
    version = version_line.split("=")[-1].strip().strip("'\"")
    return version

setuptools.setup(
    name="lmdbdict",
    version=get_version(),
    author="Ruotian Luo",
    author_email="rluo@ttic.edu",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    install_requires=[
        "lmdb",
    ],
)
