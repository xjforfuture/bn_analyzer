
import json
from functional import seq
from bs4 import BeautifulSoup
import urllib.parse as url_parse
from .log import logger


def try_decode(data):
    if data:
        try:
            return data.decode('utf-8')
        except Exception as e:
            logger.error("decode url error for utf-8")

        try:
            return data.decode('gbk')
        except Exception as e:
            logger.error("decode url error gbk")

    return None


def parse_raw_data(data):
    raw_data = {
        'sig_id': data.sig_id(),
        'sig_name': data.sig_name().decode('utf-8'),
        'header': json.loads(data.http_header().decode('utf-8')),
        'request_body_len': data.request_body_len(),
        'request_body': data.request_body() if data.request_body_len() else None,
        'response_body_len': data.response_body_len(),
        'response_body': data.response_body() if data.response_body_len() else None,
        'url_len': data.url_len(),
        'url': data.url() if data.url_len() else None
    }
    raw_data['header']['timestamp'] = raw_data['header'].get('timestamp').split('.')[0].replace('T', ' ', 1)

    return raw_data


def parse_ip_data(raw_data: dict):
    return {
        'dst_ip': raw_data['header'].get('dest_ip', None),
        'src_ip': raw_data['header'].get('src_ip', None),
        'dst_port': raw_data['header'].get('dest_port', None),
        'src_port': raw_data['header'].get('src_port', None),
    }


def parse_http_data(raw_data: dict):

    def parse_method(raw_data):
        try:
            return raw_data['header']['http']['http_method']
        except Exception as e:
            return None

    def parse_server_content_type(raw_data):
        try:
            return raw_data['header']['http']['response_header']['content_type']
        except Exception as e:
            return None

    def parse_client_content_type(raw_data):
        try:
            return raw_data['header']['http']['request_header']['content_type']
        except Exception as e:
            return None

    def parse_cookie(raw_data):
        try:
            return raw_data['header']['http']['request_header'].get('cookie', None)
        except Exception as e:
            return None

    def parse_jsessionid(cookie):
        try:
            return seq(cookie.split(';'))\
                .filter(lambda o: 'JSESSIONID' in o)\
                .map(lambda o: o.split('=')[1].strip())\
                .list()
        except Exception as e:
            return None

    def parse_body(body_type, raw_data, content_type):
        decode = ['utf-8', 'gbk', 'gb2312']
        try:
            return BeautifulSoup(raw_data[body_type].decode(seq(decode).find(lambda o: o in content_type.lower()),
                                                                 errors='ignore'),
                                 "lxml")

        except Exception as e:
            try:
                return BeautifulSoup(raw_data[body_type], "lxml")
            except Exception as e:
                return None

    def parse_body_urlencoded(raw_data, content_type):
        try:
            return url_parse.unquote(raw_data['request_body'].decode('utf-8'), 'utf-8').split('&') \
                if 'application/x-www-form-urlencoded' in content_type else None
        except Exception as e:
            return None

    def parse_response_status(raw_data):
        try:
            return raw_data['header']['http']['response_status']
        except Exception as e:
            return None

    def parse_user_agent(raw_data):
        try:
            return raw_data['header']['http']['http_user_agent']
        except Exception as e:
            return None

    http_info = {
        'url': try_decode(raw_data['url']),
        'method': parse_method(raw_data),
        'server_content_type': parse_server_content_type(raw_data),
        'client_content_type': parse_client_content_type(raw_data),
        'cookie': parse_cookie(raw_data),
        'response_status': parse_response_status(raw_data),
        'user_agent': parse_user_agent(raw_data),
    }

    http_info['jsessionids'] = parse_jsessionid(http_info['cookie'])
    http_info['request_bs'] = parse_body('request_body', raw_data, http_info['client_content_type'])
    http_info['response_bs'] = parse_body('response_body', raw_data, http_info['server_content_type'])

    http_info['request_body_urlencoded'] = parse_body_urlencoded(raw_data, http_info['client_content_type'])

    return http_info

