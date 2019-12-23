
from analyzer import config
config.cfg_init()

from analyzer import log
log.log_init("data_main", '/home/log/data_main.log')

from analyzer import es_db, analyze_data, process_data, data_source


if __name__ == '__main__':
    es_db.es_db_init()
    analyze_data.analyze_data_init()
    process_data.process_data_init()
    data_source.start()
