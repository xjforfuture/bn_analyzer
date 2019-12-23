
from .bn_data_process import gjdw_process
from .log import logger


def unknown_process(data):
    logger.error(f"process unknown system data: {data['bn_data']['system_name']}")
    return None


DATA_PROCESS_FUNC = {
    'GJDW': gjdw_process.gjdw_data_process,
    'unknown': unknown_process
}


def process_data(data):
    if data['bn_data']:
        return DATA_PROCESS_FUNC.get(data['bn_data']['system_name'], unknown_process)(data)


def process_data_init():
    gjdw_process.gjdw_process_init()
