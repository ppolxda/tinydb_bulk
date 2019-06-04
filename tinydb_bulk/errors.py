# -*- coding: utf-8 -*-
"""
@create: 2019-05-30 22:19:12.

@author: ppolxda

@desc:
"""


class Error(Exception):
    pass


class InputError(Error):
    pass


class DuplicateError(Error):
    pass


class IndexExpiredError(Error):
    pass
