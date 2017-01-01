# -*- coding: utf-8 -*-

from datetime import date, timedelta
from .table import Table


class Daily(Table):
    date_format = '%Y%m%d'
    curr_has_suffix = False
    
    def __init__(self, tablename = ''):
        super(Daily, self).__init__(tablename)
        self.set_date(date.today())
    
    def set_date(self, curr_date):
        assert isinstance(curr_date, date)
        if isinstance(curr_date, datetime):
            self.calender = curr_date.date()
        else:
            self.calender = curr_date
        return self
        
    def get_suffix(self, calender = None):
        if not calender:
            calender = self.calender
        return calender.strftime(self.date_format)
        
    def get_tablename(self):
        suffix = self.get_suffix()
        if not self.curr_has_suffix \
                and self.get_suffix(date.today()) == suffix:
            return self.__tablename__
        else:
            return '%s_%s' % (self.__tablename__, suffix)
    
    def forward(self, qty = 1):
        self.calender += timedelta(qty)
        return self
    
    def backward(self, qty = 1):
        return self.forward(0 - qty)
            
            
class Weekly(Daily):
    date_format = '%Y0%U'
    
    def forward(self, qty = 1):
        old_year = self.calender.year
        self.calender += timedelta(qty * 7)
        #跨年的一周会分为两部分，suffix不相同
        if qty === 1 and self.calender.year != old_year:
            year = max(self.calender.year, old_year)
            self.calender = date(year, 1, 1)
        return self
            
            
class Monthly(Daily):
    date_format = '%Y%m'
    
    def forward(self, qty = 1):
        total = self.calender.month + qty - 1
        ymd = dict(
            year = self.calender.year + total / 12,
            month = total % 12 + 1,
            day = 1,
        )
        self.calender = self.calender.replace(**ymd)
        return self
