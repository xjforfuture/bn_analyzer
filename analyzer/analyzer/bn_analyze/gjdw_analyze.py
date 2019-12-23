
import datetime

from functional import seq
from pymonad.Maybe import *
import joblib

from analyzer import models
from analyzer.log import logger
from analyzer import es_db

SESSION_DATA_MODEL = None


def session_analyze(datas):
    if not SESSION_DATA_MODEL:
        return []

    def extract_feature(datas):

        def extract(item):
            access_count = seq(item['url_statistics']).map(lambda o: o['access_count']).sum()
            fail_count = seq(item['url_statistics']).map(lambda o: o['fail_count']).sum()
            return {
                "start_time": item['start_time'],
                "end_time": item['end_time'],
                "jsessionid": item['jsessionid'],
                "user_id_encryption": item['user_id_encryption'],
                "user_id": item['user_id'],
                "user_name": item['user_name'],
                "user_ip": item['user_ip'],
                "elapse": (datetime.datetime.strptime(item['end_time'], models.TIME_FORMAT)
                           - datetime.datetime.strptime(item['start_time'], models.TIME_FORMAT)).seconds,
                "frequency": int(item['max_per_10s'] / 10) + 1,
                "access_count": access_count,
                "fail_count": fail_count,
                "fail_score": (fail_count / access_count) * fail_count,
                "access_kind": seq(item['url_statistics']).len(),
            }

        return Just(seq(datas).map(extract).list())

    def predict(datas):
        features = ['elapse', 'frequency', 'access_count', 'fail_score', 'access_kind']
        return Just(
            seq(datas)
            .zip(SESSION_DATA_MODEL.predict(seq(datas).map(lambda o: [o[k] for k in features]).list()))
            .smap(lambda x, y: {**x, 'label': y})
            .list()
        )

    return (Just(datas) >> extract_feature >> predict).getValue()


OBJ_ANALYZE = {
    'session': session_analyze
}


def create_template():
    template = {
        "template": models.ES_INDEX_GJDW_SESSION_FEATURE,
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
                    "elapse": {"type": "integer"},
                    "access_count": {"type": "integer"},
                    "fail_count": {"type": "integer"},
                    "fail_score": {"type": "float"},
                    "access_kind": {"type": "integer"},
                    'label': {"type": "keyword"},
                }
            }
        }
    }
    es_db.create_template(models.ES_INDEX_GJDW_SESSION_FEATURE, template)


def analyzer(params):
    if params.get('datas'):
        result = OBJ_ANALYZE.get(params.get('obj_type'))(params.get('datas'))
        seq(result).filter(lambda o: o['label'] == "high").for_each(logger.warn)

        if result:
            body = seq(result).map(lambda o: [
                {"create": {"_id": o['jsessionid'], "_type": models.ES_DOC_TYPE, "_index": models.ES_INDEX_GJDW_SESSION_FEATURE}},
                o
            ]).flatten().list()

            es_db.bulk(body=body, index=models.ES_INDEX_GJDW_SESSION_FEATURE, doc_type=models.ES_DOC_TYPE, ignore=[404, 400])


def gjdw_analyze_init():
    global SESSION_DATA_MODEL

    create_template()

    try:
        SESSION_DATA_MODEL = joblib.load(f"{models.DATA_PATH}/gjdw_session_data_model.m")
    except FileNotFoundError as e:
        SESSION_DATA_MODEL = None
        logger.error(f"session data model [{models.DATA_PATH}/gjdw_session_data_model.m] not find")
        return False

    return True
