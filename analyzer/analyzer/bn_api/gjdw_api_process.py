
import requests
import datetime
from functional import seq
from pymonad.Maybe import *

from analyzer import config
from analyzer.log import logger
from analyzer import models
from analyzer import es_db
from analyzer.bn_data_model import gjdw_data_model as gdm

def query_condition(item):
    def date_range_condition(key, value):
        if value.get('start') and value.get('end'):
            return {
                    "range": {
                        key: {
                            "gte": value['start'],
                            "lte": value['end']
                        }
                    }
                }
        return None

    def str_condition(key, value):
        if value:
            return {"term": {key: value}}
        else:
            return None

    def none_func(key, value):
        return None

    # 条件查询，包括：
    condition_func = {
        'start_time': date_range_condition,
        'time': date_range_condition,
        'user_ip': str_condition,
        'label': str_condition,
        'none_func': none_func,
    }

    return condition_func.get(item[0], none_func)(item[0], item[1])


def geolocation(ip):
    try:
        res = requests.get(f"http://{config.CFG['host_ip']}:29010/geolocation", params={'ip': ip})
        if res.status_code == 200:
            geo = res.json()
            return geo.get('city') if geo.get('city') else geo.get('country')
        else:
            return None
    except Exception as e:
        return None


def get_bn_overview(start_time, end_time):
    param = {
        'start_time': {
            'start': start_time,
            'end': end_time
        }
    }
    condition = seq(param.items()).map(query_condition).filter(lambda o: o).list()

    def get_user_ip_count():
        body = {
            "size": 0,
            "query": {
                "bool": {
                    "must": condition
                }
            },
            "aggs": {
                "user_ip": {
                    "terms": {
                        "field": "user_ip"
                    }
                }
            }
        }

        logger.info(f"body: {body}")
        res = es_db.search(models.ES_INDEX_GJDW_SESSION_FEATURE, body)

        try:
            return len(res.get('aggregations').get('user_ip').get('buckets'))
        except Exception as e:
            return 0

    def get_session_count():
        body = {
            "query": {
                "bool": {
                    "must": condition
                }
            }
        }
        total, datas = es_db.scroll_search(models.ES_INDEX_GJDW_SESSION_FEATURE, body)
        return total

    def get_access_count():
        body = {
            "size": 0,
            "query": {
                "bool": {
                    "must": condition
                }
            },
            "aggs": {
                "access_count": {
                    "sum": {
                        "field": "access_count"
                    }
                }
            }
        }
        res = es_db.search(models.ES_INDEX_GJDW_SESSION_FEATURE, body)
        try:
            return res.get('aggregations').get('access_count').get('value')
        except Exception as e:
            return 0

    return {
        'user_ip_count': get_user_ip_count(),
        'session_count': get_session_count(),
        'access_count': get_access_count()
    }


def get_access_stats():
    param = {
        'time': {
            'start': (datetime.datetime.now() - datetime.timedelta(seconds=60)).strftime(models.TIME_FORMAT),
            'end': datetime.datetime.now().strftime(models.TIME_FORMAT)
        }
    }

    body = {
        "query": {
            "bool": {
                "must": seq(param.items()).map(query_condition).filter(lambda o: o).list()
            }
        },
        "sort": {"time": {"order": "asc"}}
    }

    total, datas = es_db.scroll_search(models.ES_INDEX_GJDW_ACCESS_STATS, body, page=1, size=60)

    return datas


def get_overview_session(start_time, end_time):
    result ={
        'low': 0,
        'median': 0,
        'high': 0
    }
    param = {
        'start_time': {
            'start': start_time,
            'end': end_time
        }
    }

    body = {
        "size": 0,
        "query": {
            "bool": {
                "must": seq(param.items()).map(query_condition).filter(lambda o: o).list()
            }
        },
        "aggs": {
            "label": {
                "terms": {
                    "field": "label"
                },
            }
        }
    }

    logger.info(f"body: {body}")
    res = es_db.search(models.ES_INDEX_GJDW_SESSION_FEATURE, body)

    try:
        return {
            **result,
            **seq(res.get('aggregations').get('label').get('buckets')).map(lambda o: (o['key'], o['doc_count'])).dict()
        }
    except Exception as e:
        return None


def get_overview_risk_entries(start_time, end_time, page=1, page_size=5):
    param = {
        'start_time': {
            'start': start_time,
            'end': end_time
        },
        'label': 'high'
    }
    body = {
        "query": {
            "bool": {
                "must": seq(param.items()).map(query_condition).filter(lambda o: o).list()
            }
        }
    }
    total, datas = es_db.scroll_search(models.ES_INDEX_GJDW_SESSION_FEATURE, body, page=page, size=page_size)
    datas = seq(datas)\
        .group_by(lambda o: o['user_ip'])\
        .smap(lambda x, y: {
            'user_ip': x,
            'user_name': seq(y).find(lambda o: o['user_name'])['user_name'] if seq(y).find(lambda o: o['user_name']) else None,
            'user_id':  seq(y).find(lambda o: o['user_id'])['user_id'] if seq(y).find(lambda o: o['user_id']) else None,
            'geolocation': geolocation(x)
            })\
        .list()

    return {
        'total': total,
        'datas': datas
    }


def get_session_overview(start_time, end_time, user_ip, user_id, user_name, geolocation):
    result = {
        'exception_session': 0,
        'user_ip': user_ip,
        'user_id': user_id,
        'user_name': user_name,
        'geolocation': geolocation,
        'session': 0,
        'low': 0,
        'median': 0,
        'high': 0,
    }

    param = {
        'start_time': {
            'start': start_time,
            'end': end_time
        },
        'user_ip': user_ip,
    }

    body = {
        "size": 0,
        "query": {
            "bool": {
                "must": seq(param.items()).map(query_condition).filter(lambda o: o).list()
            }
        },
        "aggs": {
            "label_buckets": {
                "terms": {
                    "field": "label"
                }
            }
        }
    }

    res = es_db.search(models.ES_INDEX_GJDW_SESSION_FEATURE, body)

    try:
        result = {**result,
                  **seq(res.get('aggregations').get('label_buckets').get('buckets'))
                      .map(lambda o: (o['key'], o['doc_count']))
                      .dict()
                  }
    except Exception as e:
        logger.error(f"in function get_session_overview error, reason: {e}")

    result['exception_session'] = result['high']
    result['session'] = result['low'] + result['median'] + result['high']

    return result


def get_session_data_model():
    return gdm.decision_tree_dot('session')


def get_session_feature(start_time, end_time, user_ip, label , page=1, page_size=5):
    param = {
        'start_time': {
            'start': start_time,
            'end': end_time
        },
        'user_ip': user_ip,
        'label': label,
    }

    body = {
        "query": {
            "bool": {
                "must": seq(param.items()).map(query_condition).filter(lambda o: o).list()
            }
        }
    }
    logger.info(f"body: {body}")

    total, datas = es_db.scroll_search(models.ES_INDEX_GJDW_SESSION_FEATURE, body, page=page, size=page_size)
    return {
        'total': total,
        'datas': datas
    }


def get_session_info(session_id):
    return es_db.get(models.ES_INDEX_GJDW_SESSION_INFO, session_id)


def get_session_plot_scatter(session_id):
    data = (Just('/home/data/session_info.txt') >> gdm.get_data >> gdm.extract_session_feature(3000)).getValue()

    currt = es_db.get(models.ES_INDEX_GJDW_SESSION_FEATURE, session_id)
    if currt:
        currt['y'] = len(data) + 1
        data.append(currt)

    return data

