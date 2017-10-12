# -*- coding: utf-8 -*-

import umysql
from .expr import And


class Database(object):
    """ MySQL数据库 """

    configures = {}
    connections = {}

    def __init__(self, current='default'):
        self.current = current
        self.readonly = False
        self.sqls = []
        self.conn = None
        self.logger = None

    @staticmethod
    def set_charset(conn, charset='utf8'):
        sql = "SET NAMES '%s'" % charset
        return conn.query(sql)

    @classmethod
    def add_configure(cls, name, **configure):
        cls.configures[name] = configure

    @classmethod
    def set_logger(cls, logger):
        cls.logger = logger

    def close(self):
        if isinstance(self.conn, umysql.Connection):
            self.conn.close()
        self.__class__.connections.pop(self.current)

    def reconnect(self, force=False, **env):
        if not self.conn:  # 重用连接
            self.conn = self.__class__.connections.get(self.current)
        if force and self.conn:  # 强制重连
            self.conn.close()
        if force or not self.conn or not self.conn.is_connected():  # 需要时连接
            self.conn = self.connect(self.current)
            self.__class__.connections[self.current] = self.conn
        return self.conn

    def connect(self, current):
        conf = self.__class__.configures.get(current, {})
        host = conf.get('host', '127.0.0.1')
        port = int(conf.get('port', 3306))
        username = conf.get('username', 'root')
        password = conf.get('password', '')
        dbname = conf.get('database', '')
        conn = umysql.Connection()
        conn.connect(host, port, username, password, dbname)
        self.set_charset(conn, conf.get('charset', 'utf8'))
        self.readonly = conf.get('readonly', False)
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

    def add_sql(self, sql, *params, is_write = False):
        if len(self.sqls) > 50:
            del self.sqls[:-49]
        to_str = lambda p: u"NULL" if p is None else u"'%s'" % p
        full_sql = sql % tuple([to_str(p) for p in params])
        self.sqls.append(full_sql)
        if self.logger:
            if is_write:
                self.logger.info(full_sql)
            else:
                self.logger.debug(full_sql)
        return full_sql

    def execute(self, sql, *params, **kwargs):
        type = kwargs.get('type', '')
        if type and type.lower() == 'write':
            is_write = True
        else:
            is_write = False
        self.add_sql(sql, *params, is_write = is_write)
        if is_write and self.readonly: #只读，不执行
            return
        try:
            return self.reconnect().query(sql, params)
        except umysql.Error as err:
            if self.logger:
                self.logger.error(err.message or str(err))
            if self.is_connection_lost(err):
                return self.reconnect(True).query(sql, params)
            else:
                raise err

    @staticmethod
    def fetch(rs, model=dict):
        if isinstance(rs, umysql.ResultSet):
            fs = [f[0] for f in rs.fields]
            for r in rs.rows:
                row = dict(zip(fs, r))
                if model is dict:
                    yield row
                else:
                    yield model(row)

    def execute_read(self, sql, condition, addition=''):
        assert isinstance(condition, And)
        where, params = condition.build()
        if where:
            sql += " WHERE " + where
        if addition:
            sql += " " + addition.strip()
        kwargs = {'type': 'read'}
        return self.execute(sql, *params, **kwargs)

    def execute_write(self, sql, condition, *values):
        assert isinstance(condition, And)
        where, params = condition.build()
        if where:
            sql += " WHERE " + where
        if len(values) > 0:
            params = list(values) + params
        kwargs = {'type': 'write'}
        return self.execute(sql, *params, **kwargs)

    def get_dbname(self):
        sql = "SELECT DATABASE()"
        rs = self.execute(sql, type='read')
        if rs.rows:
            return rs.rows[0]['DATABASE()']

    def get_exist_tables(self, name = '', is_wild=False):
        sql = "SHOW TABLES LIKE %s"
        if is_wild:
            name += '%'
        rs = self.execute(sql, name, type = 'read')
        return [row[0] for row in rs.rows]
