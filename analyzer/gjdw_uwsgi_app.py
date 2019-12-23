

from analyzer import config
config.cfg_init()

from analyzer import log
log.log_init("api", '/home/log/api.log')

from analyzer import es_db
from analyzer.bn_api import gjdw_api

es_db.es_db_init()
app = gjdw_api.app