from flask import render_template, Blueprint, abort, jsonify
from elasticsearch import Elasticsearch


main = Blueprint('main', __name__)


@main.route("/")
def home():
    return render_template('home.html')


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
