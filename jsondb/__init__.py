# -*- coding: utf-8 -*-

import pkg_resources  # part of setuptools
version = pkg_resources.require("jsondb")[0].version

from _jsondb import *
