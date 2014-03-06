from datetime import timedelta


CELERYBEAT_SCHEDULE = {
    'compress_min_shares': {
        'task': 'simpledoge.tasks.compress_minute',
        'schedule': timedelta(minutes=5),
    },
    'compress_five_min_shares': {
        'task': 'simpledoge.tasks.compress_minute',
        'schedule': timedelta(minutes=60),
    },
}
