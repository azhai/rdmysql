# -*- coding: utf-8 -*-

import umysql
from datetime import date, timedelta
from .expr import Expr, And, Or
from .row import Row


class Database(object):
    """ MySQL数据库 """
    
    configures = {}
    connections = {}
    
    def __init__(self, current = 'default'):
        self.current = current
        self.conn = None
        self.sqls = []
        
    @classmethod
    def add_configure(cls, name, **configure):
        cls.configures[name] = configure
        
    def close(self):
        if isinstance(self.conn, umysql.Connection):
            self.conn.close()
        self.__class__.connections.pop(self.current)
        
    def reconnect(self, force = False):
        if not self.conn: #重用连接
            self.conn = self.__class__.connections.get(self.current)
        if force and self.conn: #强制重连
            self.conn.close()
        if force or not self.conn or not self.conn.is_connected(): #需要时连接
            self.conn = self.connect(self.current)
            self.__class__.connections[self.current] = self.conn
        return self.conn
        
    def connect(self, current):
        conf = self.__class__.configures.get(current, {})
        conn = umysql.Connection()
        host = conf.get('host', '127.0.0.1')
        port = int(conf.get('port', 3306))
        username = conf.get('username', 'root')
        password = conf.get('password', '')
        dbname = conf.get('database', '')
        conn.connect(host, port, username, password, dbname)
        self.set_charset(conf.get('charset', 'utf8'), conn)
        return conn

    def is_connection_lost(self, err):
        if isinstance(err, umysql.Error):
            errmsg = err.message
            if errmsg.startswith('Connection reset by peer'):
                return True
            elif errmsg.startswith('Not connected'):
                return True
            """
            elif errmsg.startswith('Too many connections'):
                return True
            elif errmsg.startswith('Access denied for user'):
                return True
            """
        return False
        
    def set_charset(self, charset, conn = None):
        sql = "SET NAMES '%s'" % charset
        if not conn:
            conn = self.reconnect()
        return conn.query(sql)
        
    def add_sql(self, sql, params):
        if len(self.sqls) > 50:
            del self.sqls[:-49]
        to_str = lambda p: u"NULL" if p is None else u"'%s'" % p
        full_sql = sql % tuple([to_str(p) for p in params])
        self.sqls.append(full_sql)
        return full_sql
        
    def query(self, sql, *params, **env):
        if env.has_key('charset'): #使用字符集
            self.set_charset(env.get('charset', 'utf8'))
        try:
            rs = self.reconnect().query(sql, params)
        except umysql.Error as err:
            if self.is_connection_lost(err):
                rs = self.reconnect(True).query(sql, params)
            else:
                raise err
        finally:
            self.add_sql(sql, params)
        if isinstance(rs, umysql.ResultSet):
            fs = [f[0] for f in rs.fields]
            return [dict(zip(fs, r)) for r in rs.rows]
        elif isinstance(rs, tuple):
            return rs[0] #影响行数
        
        
class Table(object):
    """ 数据表 """
    __dbkey__ = 'default'
    __tablename__ = ''
    __indexes__ = ['id']
    
    def __init__(self, tablename = ''):
        if tablename:
            self.__tablename__ = tablename
        self.condition = And()
        self.additions = {}
    
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
        
    def insert(self, data, action = 'INSERT INTO'):
        if isinstance(data, dict):
            keys = data.keys()
            fields = "(`%s`)" % "`,`".join(keys)
            params = [data[key] for key in keys]
        else:
            assert isinstance(data, (list, tuple))
            fields, params = "", list(data)
        holders = ",".join(["%s"] * len(params))
        sql = "%s %s%s VALUES(%s)" % (action,
                self.get_tablename(), fields, holders)
        return self.db.query(sql, *params)
        
    def update(self, changes, where = {}):
        if where:
            self.filter_by(**where)
        assert changes and isinstance(changes, dict)
        sets, params = [], []
        for key, value in changes.items():
            sets.append("`%s`=%%s" % key)
            params.append(value)
        sql = "UPDATE `%s` SET %s" % (self.get_tablename(), ",".join(sets))
        where, wh_params = self.build_where()
        if where:
            sql += " WHERE " + where
        params.extend(wh_params)
        return self.db.query(sql, *params)
        
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
            return self.insert(changes, 'REPLACE INTO')
        
    def all(self, coulmns = '*', limit = 0, offset = 0):
        if isinstance(coulmns, (list,tuple,set)):
            coulmns = ",".join(coulmns)
        sql = "SELECT %s FROM `%s`" % (coulmns, self.get_tablename())
        where, wh_params = self.build_where()
        if where:
            sql += " WHERE " + where
        sql += self.build_group_order()
        if limit > 0:
            if offset > 0:
                sql += " LIMIT %d, %d" % (offset, limit)
            else:
                sql += " LIMIT %d" % limit
        params = wh_params
        return self.db.query(sql, *params)
        
    def one(self, coulmns = '*', klass = Row):
        rows = self.all(coulmns, 1)
        if rows and len(rows) > 0:
            return klass(rows[0])
        elif klass is dict:
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


class Monthly(Table):
    calender = date.today()
    
    def set_date(self, curr_date):
        self.calender = curr_date
        return self
    
    def prevous(self, monthes = 1):
        for i in range(monthes): #小等于0时为空list
            self.calender = self.calender.replace(day = 1)
            self.calender -= timedelta(1)
        return self
        
    def get_tablename(self):
        ym = self.calender.strftime('%Y%m')
        return '%s_%s' % (self.__tablename__, ym)
