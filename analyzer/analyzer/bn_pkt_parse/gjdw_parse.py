
"""
国家电网业务解析
"""

from functional import seq
import json
from analyzer.log import logger


def login_parse(http_data):
    if http_data['request_body_urlencoded']:
        try:
            return {
                "user_id_encryption": seq(http_data['request_body_urlencoded']).find(lambda o: 'userName' in o).split('=')[1]
            }
        except Exception as e:
            pass
    return {}


def logout_parse(http_data):
    return {}


def user_info_parse(http_data):
    if http_data['response_bs']:
        try:
            data = json.loads(http_data['response_bs'].text)
            return {
                'user_id': data.get('userId'),
                'user_name': data.get('userName'),
            }

        except Exception as e:
            pass
    return {}


def unknown_parse(http_data):
    return {}


OPERATION_PARSE = [
    {
        'operation': 'login',
        'url': '/pmos/loginServlet',
        'parse_fun': login_parse,
    },
    {
        'operation': 'logout',
        'url': '/pmos/logOutServlet',
        'parse_fun': logout_parse,
    },
    {
        'operation': 'user_info',
        'url': '/pmos/UserInfoServlet',
        'parse_fun': user_info_parse,
    },
    {
        'operation': 'unknown',
        'url': '',
        'parse_fun': unknown_parse,
    },
]


def gjdw_parse(data):
    """
    :param http_data:
    :return:
    {
        "system_name": "",  # 业务系统名称，例如国家电网交易平台
        "url": "",          # 该操作的url，不包含参数，有用的参数在info中
        "operation": "",    # 操作步骤的名称 例如login、logout
        "info": {},         # 获取到的业务信息
    }
    """
    if data['raw_data']['header']['http'].get('hostname') != "pmos.sc.sgcc.com.cn":
        return None

    http_data = data['http_data']

    filter = ('.js', '.css', '.jpg', '.gif', '.png', '.ico', '.swf','unknown')
    http_tail = http_data.get('url').split('?', 1)[0].split('/')[-1] if http_data.get('url') else 'unknown'
    if seq(filter).find(lambda o: o in http_tail):
        return None

    bn_info = {
        'system_name': "GJDW",
        'url': http_data.get('url').split('?', 1)[0],
    }

    parse = seq(OPERATION_PARSE).find(lambda o: o['url'] in bn_info['url'])

    if parse:
        bn_info['operation'] = parse['operation']
        bn_info['info'] = parse['parse_fun'](http_data)

    return bn_info
