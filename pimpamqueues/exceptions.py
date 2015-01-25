#!/usr/bin/env python
# -*- encoding: utf-8 -*-


class PimPamQueuesError(Exception):

    MESSAGE = 'General error'

    def __init__(self, message=''):
        self.message = message if message else self.MESSAGE

    def __str__(self):
        return '<%s %s>' % (self.__class__.__name__, self.message)


class PimPamQueuesElementWithoutValueError(PimPamQueuesError):

    MESSAGE = 'Element do not have a value'


class PimPamQueuesDisambiguatorInvalidError(PimPamQueuesError):

    MESSAGE = 'Disambiguator must contain a disambiguate() static method ' \
              'that returns a string'