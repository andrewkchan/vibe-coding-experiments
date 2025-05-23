import sys
import os

# Add the project root directory (crawler/) to sys.path
# This allows tests to import modules from crawler_module
# Assumes pytest is run from the 'crawler' directory itself.
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, PROJECT_ROOT)

# If crawler_module is one level deeper than where conftest.py is (e.g. src/crawler_module)
# and conftest.py is in tests/, you might need:
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Or more robustly, find the project root based on a known file/dir if structure is complex. 