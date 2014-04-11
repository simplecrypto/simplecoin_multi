from pprint import pformat
import sys

from flask import Blueprint, current_app, jsonify
from lever import (API, LeverException)
import six

from .models import (Block, Share, Transaction, Payout, OneMinuteShare, Status,
                     Blob, BonusPayout)
from . import db


api = Blueprint('api_bp', __name__)


@api.errorhandler(Exception)
def api_error_handler(exc):
    # set some defaults
    log = 'debug'
    msg = "Exception occurred in error handling"
    code = 500
    extra = {}
    end_user = {}

    try:
        six.reraise(type(exc), exc, tb=sys.exc_info()[2])
    except LeverException as e:
        code = e.code
        msg = str(e)
        end_user = e.end_user
        extra = e.extra
        extra.pop('tb', None)
    except Exception as e:
        current_app.logger.error(
            "Unhandled API error of type {0} raised".format(type(e)))

    if hasattr(exc, 'error_key'):
        end_user['error_key'] = e.error_key
    end_user['success'] = False

    # ensure the message of the exception gets passed on
    end_user['message'] = msg
    response = jsonify(**end_user)
    response.status_code = code

    # logging

    # log the message using flasks logger. In the future this will use
    # logstash and other methods
    message = ('Extra: {}\nEnd User: {}'
               .format(pformat(extra), pformat(end_user)))
    getattr(current_app.logger, log)(message, exc_info=True)

    return response


class APIBase(API):
    session = db.session
    create_method = 'create'

    @classmethod
    def register(cls, mod, url):
        """ Registers the API to a blueprint or application """
        symfunc = cls.as_view(cls.__name__)
        mod.add_url_rule(url, view_func=symfunc, methods=['GET'])


class BlockAPI(APIBase):
    model = Block


class PayoutAPI(APIBase):
    model = Payout


class OneMinuteShareAPI(APIBase):
    model = OneMinuteShare


class ShareAPI(APIBase):
    model = Share


class TransactionAPI(APIBase):
    model = Transaction


class StatusAPI(APIBase):
    model = Status


class BlobAPI(APIBase):
    model = Blob


class BonusPayoutAPI(APIBase):
    model = BonusPayout


BlobAPI.register(api, '/blob')
BlockAPI.register(api, '/block')
PayoutAPI.register(api, '/payout')
BonusPayoutAPI.register(api, '/bonus_payout')
OneMinuteShareAPI.register(api, '/onemin')
ShareAPI.register(api, '/share')
TransactionAPI.register(api, '/transaction')
StatusAPI.register(api, '/status')
