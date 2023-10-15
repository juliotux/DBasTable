import sys
import os

ap_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ap_dir)
sys.tracebacklimit = 0

# Minimum version, enforced by sphinx
needs_sphinx = '4.3.0'

extensions = [
    'sphinx.ext.intersphinx',
    'sphinx.ext.extlinks',
    'sphinx.ext.todo',
    'sphinx.ext.mathjax',
    'sphinx_automodapi.automodapi',
    'numpydoc'
]

todo_include_todos = True

numpydoc_show_class_members = False
numpydoc_attributes_as_param_list = False
# numpydoc_xref_aliases = {}

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']
# The suffix of source filenames.
source_suffix = '.rst'
# Doc entrypoint
master_doc = 'index'

# Project info
project = 'DBasTable'
copyright = '2023-, Julio Campagnolo and contributors'
import dbastable
version = dbastable.__version__.split('-', 1)[0]
# The full version, including alpha/beta/rc tags.
release = dbastable.__version__
today_fmt = '%B %d, %Y'

# General theme infos
exclude_patterns = ['_templates', '_build', 'Thumbs.db', '.DS_Store']
pygments_style = 'sphinx'
html_theme = 'pydata_sphinx_theme'
html_static_path = ['_static']
htmlhelp_basename = 'dbastable'
html_theme_options = {
  "show_prev_next": False,
  "footer_items": ["copyright", "sphinx-version", "theme-version"]
}

autosummary_generate = True

default_role = 'py:obj'
intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "numpy": ("https://docs.scipy.org/doc/numpy/", None),
    "matplotlib": ("https://matplotlib.org/", None),
    "astropy": ('http://docs.astropy.org/en/latest/', None),
    "pandas": ('https://pandas.pydata.org/pandas-docs/stable/', None)
}
