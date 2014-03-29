from datetime import timedelta


CELERYBEAT_SCHEDULE = {
    'cache_user_donations': {
        'task': 'simplecoin.tasks.cache_user_donation',
        'schedule': timedelta(minutes=15)
    },
    'compute_pplns': {
        'task': 'simplecoin.tasks.update_pplns_est',
        'schedule': timedelta(minutes=15)
    },
    'compress_min_shares': {
        'task': 'simplecoin.tasks.compress_minute',
        'schedule': timedelta(minutes=5),
    },
    'compress_five_min_shares': {
        'task': 'simplecoin.tasks.compress_five_minute',
        'schedule': timedelta(minutes=60),
    },
    'general_cleanup': {
        'task': 'simplecoin.tasks.general_cleanup',
        'schedule': timedelta(minutes=120),
    },
    'update_block_state': {
        'task': 'simplecoin.tasks.update_block_state',
        'schedule': timedelta(minutes=5),
    },
    'server_status': {
        'task': 'simplecoin.tasks.server_status',
        'schedule': timedelta(minutes=2),
    },
    'check_worker_down': {
        'task': 'simplecoin.tasks.check_down',
        'schedule': timedelta(minutes=1),
    },
    'update_diff_average': {
        'task': 'simplecoin.tasks.difficulty_avg',
        'schedule': timedelta(hours=1),
    },
}
