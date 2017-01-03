# -*- coding: utf-8 -*-

import umysql
from .expr import And


class Database(object):
    """ MySQL数据库 """
    
    configures = {}
    connections = {}
    
    def __init__(self, current = 'default'):
        self.current = current
        self.conn = None
        self.sqls = []
        
    @staticmethod
    def set_charset(conn, charset = 'utf8'):
        sql = "SET NAMES '%s'" % charset
        return conn.query(sql)
        
    @classmethod
    def add_configure(cls, name, **configure):
        cls.configures[name] = configure
        
    def close(self):
        if isinstance(self.conn, umysql.Connection):
            self.conn.close()
        self.__class__.connections.pop(self.current)
        
    def reconnect(self, force = False, **env):
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
        self.set_charset(conn, conf.get('charset', 'utf8'))
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
        
    def execute(self, sql, *params):
        try:
            rs = self.reconnect().query(sql, params)
        except umysql.Error as err:
            if self.is_connection_lost(err):
                rs = self.reconnect(True).query(sql, params)
            else:
                rs = None
                raise err
        finally:
            self.add_sql(sql, *params)
            return rs
        
    @staticmethod
    def fetch(rs, model = dict):
        if isinstance(rs, umysql.ResultSet):
            fs = [f[0] for f in rs.fields]
            for r in rs.rows:
                row = dict(zip(fs, r))
                if model is dict:
                    yield row
                else:
                    yield model(row)
        
    def add_sql(self, sql, *params):
        if len(self.sqls) > 50:
            del self.sqls[:-49]
        to_str = lambda p: u"NULL" if p is None else u"'%s'" % p
        full_sql = sql % tuple([to_str(p) for p in params])
        self.sqls.append(full_sql)
        return full_sql
        
    def read_sql(self, sql, condition, addition = ''):
        assert isinstance(condition, And)
        where, params = condition.build()
        if where:
            sql += " WHERE " + where
        if addition:
            sql += " " + addition.strip()
        return self.execute(sql, *params)
        
    def write_sql(self, sql, condition, *values):
        assert isinstance(condition, And)
        where, params = condition.build()
        if where:
            sql += " WHERE " + where
        if len(values) > 0:
            params = list(values) + params
        return self.execute(sql, *params)
    
    def find_tables(self, name, is_wild = False):
        wildcard = '%' if is_wild else ''
        sql = "SHOW TABLES LIKE `%s`" % (name + wildcard)
        rs = self.execute(sql)
        return [row[0] for row in rs.rows]
    
    def is_exists(self, name):
        tables = self.find_tables(name)
        return len(tables) > 0
