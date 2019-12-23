
from .bn_analyze import gjdw_analyze
from .log import logger

def unknown_process(params):
    logger.error(f"analyzer module: unknown system")
    return None


DATA_ANALYZE_FUNC = {
    'GJDW': gjdw_analyze.analyzer,
    'unknown': unknown_process
}


def analyzer(system_name, params):
    return DATA_ANALYZE_FUNC.get(system_name, unknown_process)(params)


def analyze_data_init():
    gjdw_analyze.gjdw_analyze_init()

