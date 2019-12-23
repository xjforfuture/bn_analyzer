import json
from functional import seq

from . import pkt_parse as pp
from .log import logger
from .bn_pkt_parse import gjdw_parse

PKT_PARSE_FUNC = [gjdw_parse.gjdw_parse]
debug_login_count = 0


def bn_parse(data):
    """
    返回从报文中分析得到的业务信息
    :param project:
    :param http_data:
    :return:
    {
        "system_name": "",  # 业务系统名称，例如国家电网交易平台
        "operation": "",    # 操作步骤的名称 例如login、logout
        "url": "",          # 该操作的url，不包含参数，有用的参数在info中
        "info": {},         # 获取到的业务信息
    }
    """
    return seq(PKT_PARSE_FUNC).map(lambda o: o(data)).find(lambda o: o)


def parse_data(data):
    global debug_login_count
    raw_data = pp.parse_raw_data(data)

    data = {
        'timestamp': raw_data['header'].get('timestamp', None),
        'raw_data': raw_data,
        'ip_data': pp.parse_ip_data(raw_data),
        'http_data': pp.parse_http_data(raw_data),
    }

    if not data['raw_data']['header']['http'].get('url'):
        logger.info(f"error pkt: {json.dumps(data['raw_data']['header']['http'])}")
        return None

    if 'loginServlet' in data['http_data']['url']:
        debug_login_count += 1
        logger.info(f"login pkt url: {debug_login_count}, {data['http_data']['url']}")

    data['bn_data'] = bn_parse(data)

    return data
