from pprint import pprint
from flask import render_template, Blueprint, abort, jsonify
from elasticsearch import Elasticsearch


main = Blueprint('main', __name__)


@main.route("/")
def home():
    return render_template('home.html')

@main.route("/nav_stats")
def nav_stats():
    es = Elasticsearch()
    res = es.search(index="p_stats", size="5", body={})

    nav_stats = [(r['_source']) for r in res['hits']['hits']]
    return jsonify(nav_stats=nav_stats)

@main.route("/pool_stats")
def pool_stats():

    es = Elasticsearch()
    res = es.search(index="p_hashrate", size="288", body={
        "query": {
            "match_all": {}
        },
        "sort": {
            "time": "desc"
        }

    })

    p_stats = [(list(r['_source'].values())) for r in res['hits']['hits']]
    return jsonify(p_stats=p_stats, length=len(p_stats))


@main.route("/<address>")

def view_resume(address=None):
    return render_template('user_stats.html', username=address)

@main.route("/<address>/stats")
def address_stats(address=None):
    es = Elasticsearch()
    res = es.search(index="minute_shares", size="1440", fields="time,shares", body={
        "query": {
            "term": {
                'username':address
            }

        },
        "sort": {
            "time": "desc"
        }
    })
    min_shares = [(r['fields']['time'], r['fields']['shares']) for r in res['hits']['hits']]
    return jsonify(points=min_shares, length=len(min_shares))
