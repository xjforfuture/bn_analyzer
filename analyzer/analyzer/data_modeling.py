

from .bn_data_model import gjdw_data_model
from .log import logger

def unknown_process():
    logger.error(f"unknown system")
    return None


DATA_MODELING_FUNC = {
    'GJDW': gjdw_data_model.start,
    'unknown': unknown_process
}


def start(system_name):
    return DATA_MODELING_FUNC.get(system_name, unknown_process)()
