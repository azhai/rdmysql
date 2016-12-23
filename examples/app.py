from rdmysql import Database, Table, Expr, Row
import settings

Database.configures.update(settings.MYSQL_CONFS)


class UserProfile(Table):
    __dbkey__ = 'user'
    __tablename__ = 'user_profiles'
    
    
if __name__ == '__main__':
    uname = 'ryan'
    query = UserProfile()
    query.filter(Expr('username') == uname)
    ryan = query.one('*', Row)
    assert ryan.username == uname
    print ryan.to_dict()
    print query.db.sqls[-1]