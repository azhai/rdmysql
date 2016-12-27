# rdmysql: a simple db layer based on ultra-mysql

## Installation

    pip install rdmysql

It required umysql. If you use pypy, read https://github.com/esnme/ultramysql/pull/58

## Usage:

``` python
# For memcache
from datetime import datetime
from rdmysql import Database, Table, Row, Expr, And, Or
import settings

Database.configures.update(settings.MYSQL_CONFS)

class UserProfile(Table):
    __dbkey__ = 'user'
    __tablename__ = 't_user_profiles'

query = UserProfile().filter_by(username = 'ryan')
ryan = query.one()
if ryan:
    print ryan.to_dict()
    now = datetime.now()
    today = now.strftime('%Y%m%d')
    changed_at = now.strftime('%Y-%m-%d %H:%M:%S')
    ryan.change('nickname', 'Ryan-%s' % today)
    ryan.change('changed_at', changed_at)
    query.save(ryan, 'id')
    print query.db.sqls
```

## Methods of Table

There are some methods for class named 'Table':
    
    insert      param data : dict
    
    update      param changes : dict
                param where   : dict (optional default={})
    
    save        param changes : dict / object
                param pkey    : str (optional default='')
    
    filter      param expr : Expr / str
                param *args
    
    filter_by   param **where
    
    order_by    param field     : str
                param direction : 'ASC' / 'DESC' (optional default='ASC')
    
    group_by    param field : str
    
    all         param coulmns : str (optional default='*')
                param limit   : int (optional default=0)
                param offset  : int (optional default=0)
    
    one         param coulmns : str   (optional default='*')
                param klass   : class (optional default=Row)
    
    apply       param name : str
                param *args
                param **kwargs
    
    count,sum,max,min,avg       param *args
                                param **kwargs

## Methods of Monthly

Monthly is a subclass of Table, There are other two methods for Monthly:
    
    prevous    param monthes : int default=1
    
    set_date   param curr_date : date
