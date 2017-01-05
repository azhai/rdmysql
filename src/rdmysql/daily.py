# -*- coding: utf-8 -*-

from datetime import date, datetime, timedelta
from .table import Table


class Daily(Table):
    date_format = '%Y%m%d'
    curr_has_suffix = False
    
    def __init__(self, tablename = ''):
        super(Daily, self).__init__(tablename)
        self.set_date(date.today())
    
    def set_date(self, curr_date):
        assert isinstance(curr_date, date)
        self.calender = self.adjust_date(curr_date)
        return self
        
    def adjust_date(self, calender):
        if isinstance(calender, datetime):
            calender = calender.date()
        return calender
        
    def get_suffix(self, calender = None):
        if not calender:
            calender = self.calender
        calender = self.adjust_date(calender)
        return calender.strftime(self.date_format)
        
    def get_delta_days(self):
        delta = self.calender - self.adjust_date(date.today())
        return delta.days
        
    def get_diff_units(self):
        return self.get_delta_days()
        
    def is_current(self):
        return self.get_delta_days() == 0
        
    def get_tablename(self):
        if not self.curr_has_suffix and self.is_current():
            return self.__tablename__
        else:
            return '%s_%s' % (self.__tablename__, self.get_suffix())
    
    def forward(self, qty = 1):
        self.calender += timedelta(qty)
        return self
    
    def backward(self, qty = 1):
        return self.forward(0 - qty)
        
    def migrate(self, prev_date, **where):
        assert self.curr_has_suffix is False
        table = self.__tablename__
        suffix = self.get_suffix(prev_date)
        csql = "CREATE TABLE IF NOT EXISTS `%s_%s` LIKE `%s`" % (
                table, suffix, table)
        self.db.execute(csql)
        isql = "INSERT DELAYED `%s_%s` SELECT * FROM `%s`" % (
                table, suffix, table)
        rs = self.db.write_sql(isql, **where)
        dsql = "DELETE FROM `%s`" % table
        self.db.write_sql(dsql, **where)
        return rs[0] if rs else -1 #影响的行数
            
            
class Weekly(Daily):
    """ 以周日为一周开始，跨年的一周算作前一年最后一周 """
    
    date_format = '%Y0%U'
        
    def adjust_date(self, calender):
        calender = super(Weekly, self).adjust_date(calender)
        weekday = calender.weekday()
        if weekday > 0:
            calender -= timedelta(weekday)
        return calender
    
    def forward(self, qty = 1):
        weekday = self.calender.weekday()
        self.calender += timedelta(qty * 7 - weekday)
        return self
        
    def get_diff_units(self):
        return self.get_delta_days() / 7
            
            
class Monthly(Daily):
    date_format = '%Y%m'
        
    def adjust_date(self, calender):
        calender = super(Monthly, self).adjust_date(calender)
        if calender.day > 1:
            calender = calender.replace(day = 1)
        return calender
    
    def forward(self, qty = 1):
        total = self.calender.month + qty - 1
        ymd = dict(
            year = self.calender.year + total / 12,
            month = total % 12 + 1,
            day = 1,
        )
        self.calender = date(**ymd)
        return self
        
    def get_diff_units(self):
        today = date.today()
        return (self.calender.year - today.year) * 12 \
                + (self.calender.month - today.month)
        
        
class Yearly(Daily):
    date_format = '%Y'
        
    def adjust_date(self, calender):
        calender = super(Monthly, self).adjust_date(calender)
        if calender.month > 1 or calender.day > 1:
            calender = calender.replace(month = 1, day = 1)
        return calender
    
    def forward(self, qty = 1):
        year = self.calender.year + qty
        self.calender = date(year, 1, 1)
        return self
        
    def get_diff_units(self):
        today = date.today()
        return self.calender.year - today.year
