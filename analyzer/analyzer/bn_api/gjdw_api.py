
# -*- coding: utf-8 -*-

from flask import Flask, jsonify, request

from . import gjdw_api_process as gap

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Hello'


################################
# 业务概览 api
################################
@app.route('/api/v1.0/bn_overview', methods=['GET'])
def get_bn_overview_api():
    return jsonify(gap.get_bn_overview(request.args.get('start_time'), request.args.get('end_time')))


@app.route('/api/v1.0/bn_overview/access_stats', methods=['GET'])
def get_overview_access_stats_api():
    return jsonify(gap.get_access_stats())


@app.route('/api/v1.0/bn_overview/session', methods=['GET'])
def get_overview_session_api():
    return jsonify(gap.get_overview_session(request.args.get('start_time'), request.args.get('end_time')))


@app.route('/api/v1.0/bn_overview/risk_entries', methods=['GET'])
def get_overview_risk_entries_api():
    return jsonify(gap.get_overview_risk_entries(
        request.args.get('start_time'),
        request.args.get('end_time'),
        int(request.args.get('page')),
        int(request.args.get('page_size'))
    ))


################################
# 会话分析api
################################
@app.route('/api/v1.0/session/overview', methods=['GET'])
def get_session_overview_api():
    return jsonify(gap.get_session_overview(
        request.args.get('start_time'),
        request.args.get('end_time'),
        request.args.get('user_ip'),
        request.args.get('user_id'),
        request.args.get('user_name'),
        request.args.get('geolocation'),
    ))


@app.route('/api/v1.0/session/model', methods=['GET'])
def get_session_model_api():
    return jsonify(gap.get_session_data_model())


@app.route('/api/v1.0/session/feature', methods=['GET'])
def get_session_feature_api():
    return jsonify(gap.get_session_feature(
        request.args.get('start_time'),
        request.args.get('end_time'),
        request.args.get('user_ip'),
        request.args.get('label'),
        int(request.args.get('page')),
        int(request.args.get('page_size')),
    ))


@app.route('/api/v1.0/session/info', methods=['GET'])
def get_session_info_api():
    return jsonify(gap.get_session_info(request.args.get('session_id')))


@app.route('/api/v1.0/session/plot_scatter', methods=['GET'])
def get_session_plot_scatter_api():
    return jsonify(gap.get_session_plot_scatter(request.args.get('session_id')))

if __name__ == '__main__':

    app.run(debug=True, use_reloader=False, port=5000)
