#!/usr/bin/env python
# -*- coding: utf-8 -*-


class PimPamQueuesError(Exception):

    MESSAGE = 'Unexpected error'

    def __init__(self, message=''):
        self.message = message if message else self.MESSAGE

    def __str__(self):
        return '<%s %s>' % (self.__class__.__name__, self.message)


class PimPamQueuesElementWithoutValueError(PimPamQueuesError):

    MESSAGE = 'Element do not has a value'


class PimPamQueuesDisambiguatorInvalidError(PimPamQueuesError):

    MESSAGE = 'Disambiguator has to contain a disambiguate() static method ' \
              'which returns a string'
