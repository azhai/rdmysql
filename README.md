# rdmysql: a simple db layer based on ultra-mysql

## Installation

    pip install rdmysql

## Usage:

``` python
# For memcache
from rdmysql import Database, Table, Expr, Row
import settings

Database.configures.update(settings.MYSQL_CONFS)

class UserProfile(Table):
    __dbkey__ = 'user'
    __tablename__ = 'user_profiles'

ryan = UserProfile().filter_by(username = 'ryan').one()
print ryan.to_dict()
```

## Methods of Table

There are some methods for class named 'Table':
    
    insert      param data : dict
    
    update      param changes : dict
                param where   : dict (optional)
    
    replace     param changes : dict
                param where   : dict (optional)
    
    filter      param expr : Expr / str
                param *args
    
    filter_by   param **where
    
    order_by    param field     : str
                param direction : 'ASC' / 'DESC'
    
    group_by    param field : str
    
    all         param coulmns : str default='*'
                param limit   : int default=0
                param offset  : int default=0
    
    one         param coulmns : str default='*'
                param klass   : class default=Row
    
    apply       param name : str
                param *args
                param **kwargs
    
    count,sum,max,min,avg       param *args
                                param **kwargs

## Methods of Monthly

Monthly is a subclass of Table, There are other two methods for Monthly:
    
    prevous    param monthes : int default=1
    
    set_date   param curr_date : date
