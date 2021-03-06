# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
from sphinx.domains import Domain


# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
import sphinx_rtd_theme


# to support markdown
from recommonmark.parser import CommonMarkParser

sys.path.insert(0, os.path.abspath("../../"))
os.environ["DOC_BUILDING"] = "True"
DEPLOY = os.environ.get("READTHEDOCS") == "True"


# -- Project information -----------------------------------------------------
import lmdbdict

project = 'lmdbdict'
copyright = '2020, Ruotian Luo'
author = 'Ruotian Luo'

# The full version, including alpha/beta/rc tags
release = lmdbdict.__version__ 


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "recommonmark",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "sphinx.ext.githubpages",
    "nbsphinx",
]


# -- Configurations for plugins ------------
napoleon_google_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_special_with_doc = True
napoleon_numpy_docstring = False
napoleon_use_rtype = False
autodoc_inherit_docstrings = False
autodoc_member_order = "bysource"

if DEPLOY:
    intersphinx_timeout = 10
else:
    # skip this when building locally
    intersphinx_timeout = 0.1

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

source_suffix = [".rst", ".md", ".ipynb"]

# The master toctree document.
master_doc = "index"

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
#
# This is also used if you do content translation via gettext catalogs.
# Usually you set "language" from the command line for these cases.
language = None

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["build", "READEME.md"]

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = "sphinx"


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme_options = {}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
