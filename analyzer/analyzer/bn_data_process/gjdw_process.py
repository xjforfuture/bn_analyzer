
import os
import json
import threading
import time
from functional import seq
import datetime
import csv
from pymonad.Reader import curry
from pymonad.Maybe import *
from sklearn.externals import joblib

# from analyzer.save_data import save
from analyzer import models
from analyzer.log import logger
from analyzer import analyze_data
from analyzer import config
from analyzer import es_db

GJDW_DATA_ENTRY = {}


ENTRYS_LOCK = threading.Lock()

PKT_NEW_TIME = datetime.datetime.strptime("1970-01-01 00:00:00", models.TIME_FORMAT)
GJDW_ACCESS_STATS = {
    'count': 0,
    'last_time': datetime.datetime.strptime("1970-01-01 00:00:00", models.TIME_FORMAT),
}


def write_raw_session(data):
    jsessionid = seq(data['http_data']['jsessionids']).find(lambda o: os.path.exists(f"/home/data/{o}.txt"))

    if not jsessionid:
        jsessionid = data['http_data']['jsessionids'][0]

    path = f"/home/data/{jsessionid}.txt"
    with open(path, "a+") as f:
        json.dump({'raw_header': data['raw_data']['header'], 'business_data': data['bn_data']}, f)
        f.write("\n")


def write_session_info(entry):
    path = f"/home/data/session_info.txt"
    with open(path, "a+") as f:
        json.dump(entry, f)
        f.write("\n")


def write_access_stats(data):
    # csv 写入
    # 打开文件，追加a
    out = open('/home/data/access_stats.csv', 'a', newline='')
    # 设定写入模式
    csv_write = csv.writer(out, dialect='excel')
    # 写入具体内容
    csv_write.writerow([data['time'], data['count']])


def save(type, data):
    """
    在数据建模前收集数据时将数据写入文件
    在正式数据处理时，用于存储数据
    :param type:
    raw_session/session_info/access_stats
    :param data:
    :return:
    """
    if config.CFG.get('collect_data'):
        write_func = {
            'raw_session': write_raw_session,
            'session_info': write_session_info,
            'access_stats': write_access_stats
        }
        write_func.get(type)(data)

    else:
        if type == 'raw_session':
            jsessionid = seq(data['http_data']['jsessionids'])\
                .find(lambda o: es_db.exists(models.ES_INDEX_GJDW_RAW_SESSION, o))

            if not jsessionid:
                jsessionid = data['http_data']['jsessionids'][0]

            es_db.save(models.ES_INDEX_GJDW_RAW_SESSION,
                       data={
                           "time": data['timestamp'],
                           "jsessionid": jsessionid,
                           'http': data['raw_data']['header']['http'],
                           'ip_data': data['ip_data'],
                           'business_data': data['bn_data']
                       })
        elif type == 'session_info':
            es_db.save(models.ES_INDEX_GJDW_SESSION_INFO, data['jsessionid'], data)
        elif type == 'access_stats':
            es_db.save(models.ES_INDEX_GJDW_ACCESS_STATS, data=data)
        else:
            pass


def get_access_step(info):
    if info['operation'] == 'logout':
        if seq(list(GJDW_DATA_ENTRY[info['jsessionid']]['url_statistics'].values()))\
                .find(lambda o: o['operation'] == 'login'):
            with open(f"/home/data/{info['jsessionid']}.txt", "r") as f:
                access_list = seq(list(f))\
                    .map(lambda o: json.loads(o.strip()))\
                    .map(lambda o: {
                        'operation': o['business_data']['operation'],
                        'url': o['business_data']['url'],
                        'method': o['raw_header']['http'].get('http_method')})\
                    .list()

                flag = False
                steps = []
                for o in access_list:
                    if o['operation'] == 'login':
                        flag = True

                    if flag:
                        steps.append(o)
                    if o['operation'] == 'logout':
                        break

                path = f"/home/data/steps.txt"
                with open(path, "a+") as f:
                    json.dump({info['jsessionid']: steps}, f)
                    f.write("\n")


def gjdw_extract_info(data):
    """
    提取数据中的特征
    :param data:
    {
        "ip_data": dict,
        "http_data": dict,
        "bn_data": dict,
    }
    :return:
    {
        'timestamp': "",
        'jsessionids': "",
        'dst_ip': "",
        'src_ip': "",
        'dst_port': "",
        'src_port': "",
        'operation': "",
        'url': "",
        'user_name': ""
    }
    """
    jsessionid = seq(data['http_data']['jsessionids']).find(lambda o: GJDW_DATA_ENTRY.get(o))
    if not jsessionid:
        jsessionid = data['http_data']['jsessionids'][0]

    info = {
        'timestamp': data['timestamp'],
        'jsessionid': jsessionid,
        'dst_ip': data['ip_data']['dst_ip'],
        'src_ip': data['ip_data']['src_ip'],
        'dst_port': data['ip_data']['dst_port'],
        'src_port': data['ip_data']['src_port'],
        'operation': data['bn_data']['operation'],
        'url': data['bn_data']['url'],
        'user_agent': data['http_data']['user_agent'],
        'method': data['http_data']['method'],
        'response_status': data['http_data']['response_status'],
    }

    if not info['url']:
        return None

    info['user_id_encryption'] = data['bn_data']['info'].get('user_id_encryption')
    info['user_id'] = data['bn_data']['info'].get('user_id')
    info['user_name'] = data['bn_data']['info'].get('user_name')

    return info


def gjdw_data_collect(info):
    ENTRYS_LOCK.acquire()
    if not GJDW_DATA_ENTRY.get(info['jsessionid']):
        GJDW_DATA_ENTRY[info['jsessionid']] = {
            'start_time': info['timestamp'],
            'end_time': info['timestamp'],
            'jsessionid': info.get('jsessionid'),
            'user_id_encryption': info.get('user_id_encryption'),
            'user_id': info['user_id'],
            'user_name': info['user_name'],
            'user_ip': info.get('src_ip'),
            'user_agent': info.get('user_agent'),
            'max_per_10s': 1,  # per 10 sec
            'frequency': {
                'count': 1,
                'last_time': info['timestamp'],
            },
            'url_statistics': {
                info.get('url'): {
                    'url': info.get('url'),
                    'method': info['method'],
                    'operation': info['operation'],
                    'access_count': 1,
                    'success_count': 1 if isinstance(info['response_status'], int) and info['response_status'] < 400 else 0,
                    'fail_count': 1 if isinstance(info['response_status'], int) and info['response_status'] >= 400 else 0,
                    'max_per_10s': 1,  # per 10 sec
                    'frequency': {
                        'count': 1,
                        'last_time': info['timestamp'],
                    }
                }
            }
        }
    else:

        url = GJDW_DATA_ENTRY[info['jsessionid']]['url_statistics'].get(info['url'])
        if url:
            url['url'] = info['url']
            url['method'] = info['method']
            url['operation'] = info['operation']
            url['access_count'] += 1
            url['success_count'] += 1 if isinstance(info['response_status'], int) and info['response_status'] < 400 else 0

            url['fail_count'] += 1 if isinstance(info['response_status'], int) and info['response_status'] >= 400 else 0

            url['frequency']['count'] += 1
            elapse = (datetime.datetime.strptime(info['timestamp'], models.TIME_FORMAT)
                      - datetime.datetime.strptime(url['frequency']['last_time'], models.TIME_FORMAT)).seconds

            if elapse >= 10:
                frequency = int((url['frequency']['count'] / elapse) * 10)
                if frequency > url['max_per_10s']:
                    url['max_per_10s'] = frequency
                url['frequency']['count'] = 0
                url['frequency']['last_time'] = info['timestamp']

        else:
            GJDW_DATA_ENTRY[info['jsessionid']]['url_statistics'][info['url']] = {
                'url': info.get('url'),
                'method': info['method'],
                'operation': info['operation'],
                'access_count': 1,
                'success_count': 1 if isinstance(info['response_status'], int) and info['response_status'] < 400 else 0,
                'fail_count': 1 if isinstance(info['response_status'], int) and info['response_status'] >= 400 else 0,
                'max_per_10s': 1,  # per 10 sec
                'frequency': {
                    'count': 1,
                    'last_time': info['timestamp'],
                }
            }

        GJDW_DATA_ENTRY[info['jsessionid']]['frequency']['count'] += 1
        elapse = (datetime.datetime.strptime(info['timestamp'], models.TIME_FORMAT)
                  - datetime.datetime.strptime(GJDW_DATA_ENTRY[info['jsessionid']]['frequency']['last_time'], models.TIME_FORMAT)).seconds

        if elapse >= 10:
            frequency = int((GJDW_DATA_ENTRY[info['jsessionid']]['frequency']['count'] / elapse) * 10)
            if frequency > GJDW_DATA_ENTRY[info['jsessionid']]['max_per_10s']:
                GJDW_DATA_ENTRY[info['jsessionid']]['max_per_10s'] = frequency
            GJDW_DATA_ENTRY[info['jsessionid']]['frequency']['count'] = 0
            GJDW_DATA_ENTRY[info['jsessionid']]['frequency']['last_time'] = info['timestamp']

        if info.get('user_id_encryption'):
            GJDW_DATA_ENTRY[info['jsessionid']]['user_id_encryption'] = info['user_id_encryption']
        if info.get('user_id'):
            GJDW_DATA_ENTRY[info['jsessionid']]['user_id'] = info['user_id']
        if info.get('user_name'):
            GJDW_DATA_ENTRY[info['jsessionid']]['user_name'] = info['user_name']
        if info.get('src_ip'):
            GJDW_DATA_ENTRY[info['jsessionid']]['user_ip'] = info['src_ip']
        if info.get('timestamp'):
            if datetime.datetime.strptime(info['timestamp'], models.TIME_FORMAT) \
                >= datetime.datetime.strptime(GJDW_DATA_ENTRY[info['jsessionid']]['end_time'], models.TIME_FORMAT):
                GJDW_DATA_ENTRY[info['jsessionid']]['end_time'] = info['timestamp']
    ENTRYS_LOCK.release()


def save_all():
    seq(GJDW_DATA_ENTRY.values()).for_each(save)


def access_statistics(info):
    timestamp = info['timestamp']
    GJDW_ACCESS_STATS['count'] += 1
    if GJDW_ACCESS_STATS['last_time'] == datetime.datetime.strptime("1970-01-01 00:00:00", models.TIME_FORMAT):
        # first pkt
        GJDW_ACCESS_STATS['last_time'] = datetime.datetime.strptime(timestamp, models.TIME_FORMAT)
        return None

    new_time = datetime.datetime.strptime(timestamp, models.TIME_FORMAT)
    if new_time > GJDW_ACCESS_STATS['last_time']:
        elapse = (new_time - GJDW_ACCESS_STATS['last_time']).seconds
        if elapse == 1:
            save('access_stats', {'time': timestamp, 'count': GJDW_ACCESS_STATS['count']})
            GJDW_ACCESS_STATS['count'] = 0
            GJDW_ACCESS_STATS['last_time'] = new_time
        elif elapse > 1:
            seq(range(1, elapse))\
                .map(lambda o: {
                    'time': (GJDW_ACCESS_STATS['last_time'] + datetime.timedelta(seconds=o)).strftime(models.TIME_FORMAT),
                    'count': 0
                    })\
                .for_each(lambda o: save('access_stats', o))

            save("access_stats", {'time': timestamp, 'count': GJDW_ACCESS_STATS['count']})
            GJDW_ACCESS_STATS['count'] = 0
            GJDW_ACCESS_STATS['last_time'] = new_time
        else:
            pass


def gjdw_data_process(data):
    global PKT_NEW_TIME
    if not data['bn_data'] or not data['http_data']['jsessionids']:
        return

    save('raw_session', data)
    info = gjdw_extract_info(data)
    if info:
        PKT_NEW_TIME = datetime.datetime.strptime(info['timestamp'], models.TIME_FORMAT)
        access_statistics(info)
        gjdw_data_collect(info)


def session_timeout_process():
    global GJDW_DATA_ENTRY
    while True:
        ENTRYS_LOCK.acquire()
        whole_sessions = seq(GJDW_DATA_ENTRY.values()) \
            .filter(lambda o: PKT_NEW_TIME - datetime.datetime.strptime(o['end_time'], models.TIME_FORMAT)
                              > datetime.timedelta(hours=1)) \
            .list()

        entries = seq(GJDW_DATA_ENTRY.items()) \
            .filter(lambda o: PKT_NEW_TIME - datetime.datetime.strptime(o[1]['end_time'], models.TIME_FORMAT)
                              <= datetime.timedelta(hours=1)) \
            .dict()

        GJDW_DATA_ENTRY = entries
        ENTRYS_LOCK.release()

        whole_sessions = seq(whole_sessions)\
            .map(lambda o: {**o, 'url_statistics': list(o['url_statistics'].values())})\
            .list()

        seq(whole_sessions).for_each(lambda o: save('session_info', o))
        analyze_data.analyzer('GJDW', {"obj_type": 'session', "datas": whole_sessions})

        time.sleep(60)


class SessionTimeOutThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        while True:
            try:
                session_timeout_process()
            except Exception as e:
                logger.critical(f"Got exception: reason {e}", exc_info=True)
                time.sleep(10)


def create_template():
    template = {
        "template": models.ES_INDEX_GJDW_RAW_SESSION,
        "order": 1,
        "settings": {
            "number_of_replicas": 1,
            "number_of_shards": 1
        },
        "mappings": {
            "_default_": {
                "_all": {"enabled": False}
            },
            models.ES_DOC_TYPE: {
                "properties": {
                    "time":{
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss"
                    },
                    "jsessionid": {"type": "keyword"},
                    "ip_data": {
                        "properties": {
                            'dst_ip': {"type": "keyword"},
                            'src_ip': {"type": "keyword"},
                            'dst_port': {"type": "integer"},
                            'src_port': {"type": "integer"}
                        }
                    },
                    "http": {
                        "properties": {
                            "hostname": {"type": "keyword"},
                            "url": {"type": "keyword"},
                            "http_user_agent": {"type": "keyword"},
                            "http_refer": {"type": "keyword"},
                            "http_method": {"type": "keyword"},
                            "protocol": {"type": "keyword"},
                            "response_status": {"type": "integer"},
                            "response_length": {"type": "integer"},
                            "request_header": {
                                "properties": {
                                    "accept": {"type": "keyword"},
                                    "accept_encoding": {"type": "keyword"},
                                    "accept_language": {"type": "keyword"},
                                    "cookie": {"type": "keyword"},
                                    "x_requested_with": {"type": "keyword"},
                                    "connection": {"type": "keyword"}
                                }
                            },
                            "response_header": {
                                "properties": {
                                    "connection": {"type": "keyword"},
                                    "content_encoding": {"type": "keyword"},
                                    "content_type": {"type": "keyword"},
                                    "date": {"type": "keyword"},
                                    "server": {"type": "keyword"},
                                    "set_cookie": {"type": "keyword"},
                                    "transfer_encoding": {"type": "keyword"}
                                }
                            },
                        }
                    },
                    "business_data": {
                        "properties": {
                            "system_name": {"type": "keyword"},
                            "url": {"type": "keyword"},
                            "operation": {"type": "keyword"},
                            "info": {"type": "nested"}
                        }
                    }
                }
            }
        }
    }
    es_db.create_template(models.ES_INDEX_GJDW_RAW_SESSION, template)

    template = {
        "template": models.ES_INDEX_GJDW_SESSION_INFO,
        "order": 1,
        "settings": {
            "number_of_replicas": 1,
            "number_of_shards": 1
        },
        "mappings": {
            "_default_": {
                "_all": {"enabled": False}
            },
            models.ES_DOC_TYPE: {
                "properties": {
                    "start_time": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss"
                    },
                    "end_time": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss"
                    },
                    "jsessionid": {"type": "keyword"},
                    "user_id_encryption": {"type": "keyword"},
                    "user_id": {"type": "keyword"},
                    "user_name": {"type": "keyword"},
                    "user_ip": {"type": "keyword"},
                    "user_agent": {"type": "keyword"},
                    "max_per_10s": {"type": "integer"},
                    "frequency": {
                        "properties": {
                            "count": {"type": "integer"},
                            "last_time": {
                                "type": "date",
                                "format": "yyyy-MM-dd HH:mm:ss"
                            }
                        }
                    },
                    "url_statistics": {"type": "nested"}
                }
            }
        }
    }
    es_db.create_template(models.ES_INDEX_GJDW_SESSION_INFO, template)

    template = {
        "template": models.ES_INDEX_GJDW_ACCESS_STATS,
        "order": 1,
        "settings": {
            "number_of_replicas": 1,
            "number_of_shards": 1
        },
        "mappings": {
            "_default_": {
                "_all": {"enabled": False}
            },
            models.ES_DOC_TYPE: {
                "properties": {
                    "time": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss"
                    },
                    "count": {"type": "integer"}
                }
            }
        }
    }
    es_db.create_template(models.ES_INDEX_GJDW_ACCESS_STATS, template)


def gjdw_process_init():
    create_template()
    SessionTimeOutThread('SessionTimeOutThread').start()

