# -*- coding: utf-8 -*-

from .database import Database
from .expr import Expr, And, Or


class Table(object):
    """ 数据表 """
    __dbkey__ = 'default'
    __tablename__ = ''
    __indexes__ = ['id']
    
    def __init__(self, tablename = ''):
        if tablename:
            self.__tablename__ = tablename
        self.reset()
    
    @property
    def db(self):
        if not hasattr(self, '_db') or not self._db:
            db = Database(self.__dbkey__)
            self.set_db(db)
        return self._db
        
    def set_db(self, db):
        if isinstance(db, Database):
            self._db = db
        return self
        
    def get_tablename(self):
        return self.__tablename__
    
    def reset(self, or_cond = False):
        self.condition = Or() if or_cond else And()
        self.additions = {}
        return self
    
    def filter(self, expr, *args):
        if isinstance(expr, str):
            expr = Expr(expr).op(*args)
        self.condition.append(expr)
        return self
        
    def filter_by(self, **where):
        for field, value in where.items():
            self.condition.append(Expr(field) == value)
        return self
    
    def build_where(self):
        return self.condition.build()
        
    def order_by(self, field, direction = 'ASC'):
        if 'ORDER BY' not in self.additions:
            self.additions['ORDER BY'] = []
        order = '%s %s' % (field, direction)
        self.additions['ORDER BY'].append(order)
        return self
        
    def group_by(self, field):
        if 'GROUP BY' not in self.additions:
            self.additions['GROUP BY'] = []
        self.additions['GROUP BY'].append(field)
        return self
    
    def build_group_order(self):
        group_order = ''
        for key, vals in self.additions.items():
            item = ' %s %s' % (key, ', '.join(vals))
            group_order += item
        return group_order
        
    @staticmethod
    def unzip_pair(row, keys = []):
        if isinstance(row, dict):
            keys = row.keys()
        if len(keys) > 0:
            fields = "(`%s`)" % "`,`".join(keys)
            values = [row[key] for key in keys]
        else:
            fields = ''
            values = list(row)
        return keys, values, fields
        
    def insert(self, *rows, **kwargs):
        action = kwargs.get('action', 'INSERT INTO')
        if len(rows) == 0:
            return 0
        elif len(rows) > 10:
            action = action.replace('INTO', 'DELAYED')
        rows = list(rows)
        row = rows.pop(0)
        keys, params, fields = self.unzip_pair(row)
        holders = ",".join(["%s"] * len(params))
        sql = "%s %s%s VALUES(%s)" % (action,
                self.get_tablename(), fields, holders)
        if len(rows) > 0: #插入更多行
            sql += (", (%s)" % holders) * len(rows)
            for row in rows:
                keys, values, _fields = self.unzip_pair(row, keys)
                params.extend(values)
        rs = self.db.execute(sql, *params)
        return rs[1] if rs else 0 #最后的自增ID
        
    def delete(self, where):
        sql = "DELETE FROM `%s`" % self.get_tablename()
        if where:
            self.filter_by(**where)
        where, params = self.build_where()
        if where:
            sql += " WHERE " + where
        rs = self.db.execute(sql, *params)
        return rs[0] if rs else -1 #影响的行数
        
    def update(self, changes, where = {}):
        assert isinstance(changes, dict)
        keys, values, _fields = self.unzip_pair(changes)
        fields = ",".join(["`%s`=%%s" % key for key in keys])
        sql = "UPDATE `%s` SET %s" % (self.get_tablename(), fields)
        if where:
            self.filter_by(**where)
        where, params = self.build_where()
        if where:
            sql += " WHERE " + where
        params = values + params
        rs = self.db.execute(sql, *params)
        return rs[0] if rs else -1 #影响的行数
        
    def save(self, row, indexes = []):
        if len(indexes) == 0:
            indexes = self.__indexes__
        if hasattr(row, 'to_dict'):
            changes = row.to_dict()
        else:
            changes = dict(row)
        where = {}
        for index in indexes:
            value = changes.pop(index, None)
            if value is not None:
                where[index] = value
        if len(where) > 0:
            return self.update(changes, where)
        else:
            return self.insert(changes, action = 'REPLACE INTO')
        
    def prepare(self, coulmns = '*', addition = ''):
        if isinstance(coulmns, (list,tuple,set)):
            coulmns = ",".join(coulmns)
        sql = "SELECT %s FROM `%s`" % (coulmns, self.get_tablename())
        where, params = self.build_where()
        if where:
            sql += " WHERE " + where
        if addition:
            sql += " " + addition.strip()
        return sql, params
        
    def all(self, coulmns = '*', limit = 0, offset = 0, model = dict):
        addition = self.build_group_order()
        if limit > 0:
            if offset > 0:
                addition += " LIMIT %d, %d" % (offset, limit)
            else:
                addition += " LIMIT %d" % limit
        sql, params = self.prepare(coulmns, addition)
        rs = self.db.execute(sql, *params)
        return [row for row in self.db.fetch(rs, model = model)]
        
    def one(self, coulmns = '*', model = dict):
        rows = self.all(coulmns, limit = 1, model = model)
        if rows and len(rows) > 0:
            return rows[0]
        elif model is dict:
            return {}
            
    def apply(self, name, *args, **kwargs):
        name = name.strip().upper()
        if name == 'COUNT' and len(args) == 0:
            column = 'COUNT(*)'
        else:
            column = '%s(%s)' % (name, ', '.join(args))
        row = self.one(column, dict)
        if row and row.has_key(column):
            result = row[column]
        else:
            result = kwargs.pop('default', None)
        if kwargs.has_key('coerce'):
            result = kwargs['coerce'](result)
        return result
        
    def count(self, *args, **kwargs):
        kwargs['coerce'] = int
        if not kwargs.has_key('default'):
            kwargs['default'] = 0
        return self.apply('count', *args, **kwargs)
        
    def sum(self, *args, **kwargs):
        if not kwargs.has_key('default'):
            kwargs['default'] = 0
        return self.apply('sum', *args, **kwargs)
        
    def max(self, *args, **kwargs):
        return self.apply('max', *args, **kwargs)
        
    def min(self, *args, **kwargs):
        return self.apply('min', *args, **kwargs)
        
    def avg(self, *args, **kwargs):
        return self.apply('avg', *args, **kwargs)
