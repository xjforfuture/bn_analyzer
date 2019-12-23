
from functional import seq
import json
import datetime
import time

import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, plot


from sklearn.cluster import DBSCAN, AgglomerativeClustering, KMeans
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.decomposition import PCA
from sklearn.manifold import TSNE
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import export_graphviz
import joblib

from pymonad.Reader import curry
from pymonad.Maybe import *

from analyzer.log import logger
from analyzer import models


def get_data(paths):
    # 从文件中获取数据
    def read_file(paths):
        with open(paths, "r") as f:
            info = seq(f).map(lambda o: json.loads(o)).list()
            return info

    return Just(seq(paths).map(read_file).flatten().list())


@curry
def extract_session_feature(scale, data):
    logger.info(f"datas: {len(data)}")
    count = 0
    def extract(item):
        nonlocal count
        count += 1
        access_count = seq(item['url_statistics'].values()).map(lambda o: o['access_count']).sum()
        fail_count = seq(item['url_statistics'].values()).map(lambda o: o['fail_count']).sum()
        return {
            "start_time":item['start_time'],
            "end_time":item['end_time'],
            "jsessionid":item['jsessionid'],
            "user_id_encryption":item['user_id_encryption'],
            "user_id":item['user_id'],
            "user_name":item['user_name'],
            "user_ip":item['user_ip'],
            "elapse": (datetime.datetime.strptime(item['end_time'].split('.')[0], models.TIME_FORMAT)
                      - datetime.datetime.strptime(item['start_time'].split('.')[0], models.TIME_FORMAT)).seconds,
            "frequency":int(item['max_per_10s']/10)+1,
            "access_count": access_count,
            "fail_count": fail_count,
            "fail_score": (fail_count / access_count) * fail_count,
            "access_kind": seq(item['url_statistics'].values()).len(),
            "y": count
        }

    return Just(seq(data).map(extract).slice(0, scale).list())


@curry
def clustering(features: list, n_clusters, datas):
    print(len(datas))
    train_datas = seq(datas) \
        .map(lambda o: [o[k] for k in features]) \
        .list()
#     tsne = TSNE(random_state=42)
#     train_datas = tsne.fit_transform(train_datas)
#     train_datas = PCA(n_components=2).fit_transform(train_datas)
#     train_datas = MinMaxScaler().fit_transform(train_datas)
#     train_datas = StandardScaler().fit_transform(train_datas)


#     kmeans = KMeans(n_clusters=n_clusters)
#     clusters = kmeans.fit_predict(train_datas)

    agg = AgglomerativeClustering(n_clusters=n_clusters, linkage='average')
    clusters = agg.fit_predict(train_datas)

    # dbscan = DBSCAN(eps=2, min_samples=3)
    # clusters = dbscan.fit_predict(train_datas)

    # print(clusters)
    return Just(seq(datas).zip(clusters).smap(lambda x, y: {**x, 'cluster': y}).list())


@curry
def plot_data(feature, datas):
    def color_func(data):

        if data['cluster'] == 0:
            return '#00FF00'  # g
        elif data['cluster'] == 1:
            return '#FFFF00'  # y
        elif data['cluster'] == 2:
            return '#FF0000'  # r
        elif data['cluster'] == 3:
            return '#0000FF'  # b
        else:
            return '#FFFFFF'  # blank

    # Create a trace
    trace = go.Scatter(
        x=seq(datas).map(lambda o: o[feature]).list(),
        y=seq(datas).map(lambda o: o['y']).list(),
        mode='markers',
        text=seq(datas) \
            .map(lambda o: f"ip:{o['user_ip']} \
                 <br>access_count:{o['access_count']} \
                 <br>elapse:{o['elapse']} \
                 <br>frequency:{o['frequency']}\
                 <br>fail_count:{o['fail_count']}\
                 <br>access_kind:{o['access_kind']}") \
            .list(),
        marker=dict(
            #         size = 10,
            color=seq(datas).map(color_func).list(),
            #         line = dict(
            #             width = 2,
            #         )
        )
    )

    layout = go.Layout(
        title='cluster',
        hovermode='closest',
        xaxis=dict(
            title=feature,
            #         ticklen= 5,
            #         zeroline= False,
            #         gridwidth= 2,
        ),
        yaxis=dict(
            title='probability',
            #         ticklen= 5,
            #         gridwidth= 2,
        ),
        showlegend=False
    )
    fig = go.Figure(data=[trace], layout=layout)
    plot(fig, filename=f"{models.DATA_PATH}/{feature}-plot.html")
    return Just(datas)


@curry
def set_label(labels: list, datas):
    # label, 0:low, 1:median, 2:high
    label_map = {
        0: labels[0],
        1: labels[1],
        2: labels[2],
    }

    return Just(seq(datas)
                .map(lambda o: {
                                    **o,
                                    'label': label_map.get(o['cluster'])
                                                    if o.get('label', 0) < label_map.get(o['cluster'])
                                                    else o.get('label', 0)
                                }
                     )
                .list()
                )


def replace_label(datas):
    # label, 0:low, 1:median, 2:high
    label_map = {
        0: 'low',
        1: 'median',
        2: 'high',
    }

    return Just(seq(datas).map(lambda o: {**o, 'label': label_map.get(o['label'])}).list())

@curry
def train_model(model, datas):
    x = seq(datas) \
        .map(lambda o: [o['elapse'], o['frequency'], o['access_count'], o['fail_score'], o['access_kind']]) \
        .list()
    y = seq(datas) \
        .map(lambda o: o['label']) \
        .list()

    skf = StratifiedKFold(n_splits=5)
    score = cross_val_score(model, x, y, cv=skf)
    logger.info(f"model score {score}")

    model.fit(x, y)

    return Just(model)


def session_data_model():
    """
        1 提取session特征数据
        2 单独从5个维度聚类（凝聚聚类），分成3类，为数据点最多的类打上low标签、为数据点较少的类打上median、为数据点最少的类打上high标签
          如果从不同的维度，该数据点标签不一样，取最高标签
        3 训练机器学习模型，尝试各种监督学习算法（目前用随机森林），用分层k折交叉验证模型。
        :return:
        """
    str = input("请输入原始数据的路径，多个路径用空格隔开:\n")
    scale = int(input("请输入训练集数据规模:\n"))

    label_data = Just(str.split(' ')) \
                 >> get_data \
                 >> extract_session_feature(scale) \
                 >> clustering(['elapse'], 3) \
                 >> plot_data('elapse')

    str = input("feature: elapse, 请根据图形颜色输入标签0:low, 1:median, 2:high: ")
    label_data = label_data >> set_label(seq(str.split(' ')).map(lambda o: int(o)).list())
    seq(label_data.getValue()).group_by(lambda o: o['label']).for_each(lambda o: logger.info(f"{o[0]}: {len(o[1])}"))

    label_data = label_data >> clustering(['frequency'], 3) >> plot_data('frequency')
    str = input("feature: frequency, 请根据图形颜色输入标签0:low, 1:median, 2:high: ")
    label_data = label_data >> set_label(seq(str.split(' ')).map(lambda o: int(o)).list())
    seq(label_data.getValue()).group_by(lambda o: o['label']).for_each(lambda o: logger.info(f"{o[0]}: {len(o[1])}"))

    label_data = label_data >> clustering(['access_count'], 3) >> plot_data('access_count')
    str = input("feature: access_count, 请根据图形颜色输入标签0:low, 1:median, 2:high: ")
    label_data = label_data >> set_label(seq(str.split(' ')).map(lambda o: int(o)).list())
    seq(label_data.getValue()).group_by(lambda o: o['label']).for_each(lambda o: logger.info(f"{o[0]}: {len(o[1])}"))

    label_data = label_data >> clustering(['fail_score'], 3) >> plot_data('fail_score')
    str = input("feature: fail_score, 请根据图形颜色输入标签0:low, 1:median, 2:high: ")
    label_data = label_data >> set_label(seq(str.split(' ')).map(lambda o: int(o)).list())
    seq(label_data.getValue()).group_by(lambda o: o['label']).for_each(lambda o: logger.info(f"{o[0]}: {len(o[1])}"))

    label_data = label_data >> clustering(['access_kind'], 3) >> plot_data('access_kind')
    str = input("feature: access_kind, 请根据图形颜色输入标签0:low, 1:median, 2:high: ")
    label_data = label_data >> set_label(seq(str.split(' ')).map(lambda o: int(o)).list())
    seq(label_data.getValue()).group_by(lambda o: o['label']).for_each(lambda o: logger.info(f"{o[0]}: {len(o[1])}"))

    forest = RandomForestClassifier(n_estimators=10, random_state=2)
    model = label_data >> replace_label >> train_model(forest)
    joblib.dump(model.getValue(), f"{models.DATA_PATH}/gjdw_session_data_model.m")


def ip_action_model():
    # todo
    str = input("请输入原始数据的路径，多个路径用空格隔开:\n")
    scale = int(input("请输入训练集数据规模:\n"))
    label_data = Just(str.split(' ')) \
                 >> get_data \
                 >> extract_session_feature(scale) \


def start():
    str = input("请选择需要训练的模型:\n1 session data model \n2 user action model\n")
    if str == 1:
        session_data_model()
    elif str == 2:
        ip_action_model()


def decision_tree_dot(type):
    try:
        model = joblib.load(f"{models.DATA_PATH}/gjdw_{type}_data_model.m")
    except FileNotFoundError as e:
        logger.error(f"session data model [{models.DATA_PATH}/gjdw_session_data_model.m] not find")
        return []

    def get_dot_data(item):
        return export_graphviz(item, out_file=None,
                               feature_names=['持续时间', '频率', '访问次数', '失败分数', 'url数量'],
                               class_names=['low', 'median', 'high'],
                               filled=True, rounded=True,
                               special_characters=True)

    return seq(list(model.estimators_)).map(get_dot_data).list()

