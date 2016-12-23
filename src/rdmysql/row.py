# -*- coding: utf-8 -*-

from datetime import date, datetime
from decimal import Decimal


class Row(object):
    """ 单行结果 """
    _fields = []
    _data = {}
        
    def __init__(self, data):
        self.set_data(data)
        
    def __getattr__(self, field):
        if field in self._data:
            return self._data[field]
    
    def set_fields(self, fields):
        self._fields = list(fields)
    
    def set_data(self, data):
        if self._fields and isinstance(data, (list, tuple)):
            data = dict(zip(self._fields, list(data)))
        self._data = data
            
    def to_dict(self):
        data = {}
        for k, v in self._data.iteritems():
            data[k] = self.coerce_string(v)
        return data
    
    @staticmethod
    def coerce_string(value):
        if isinstance(value, datetime):
            value = value.strftime('%F %T')
        elif isinstance(value, date):
            value = value.strftime('%F')
        elif isinstance(value, Decimal):
            value = float(value)
        return value
