
from analyzer import config
config.cfg_init()

from analyzer import log
log.log_init("model", '/home/log/model.log')

from analyzer import data_modeling

if __name__ == '__main__':
    data_modeling.start('GJDW')

