from datetime import timedelta


CELERYBEAT_SCHEDULE = {
    'compress_min_shares': {
        'task': 'simpledoge.tasks.compress_minute',
        'schedule': timedelta(minutes=5),
    },
    'compress_five_min_shares': {
        'task': 'simpledoge.tasks.compress_five_minute',
        'schedule': timedelta(minutes=60),
    },
    'general_cleanup': {
        'task': 'simpledoge.tasks.general_cleanup',
        'schedule': timedelta(minutes=120),
    },
    'update_block_state': {
        'task': 'simpledoge.tasks.update_block_state',
        'schedule': timedelta(minutes=5),
    },
    'server_status': {
        'task': 'simpledoge.tasks.server_status',
        'schedule': timedelta(minutes=2),
    },
    'check_worker_down': {
        'task': 'simpledoge.tasks.check_down',
        'schedule': timedelta(minutes=1),
    },
}
