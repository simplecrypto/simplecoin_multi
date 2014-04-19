from datetime import timedelta
from flask import current_app


caching_tasks = {
    'cache_user_donations': {
        'task': 'simplecoin.tasks.cache_user_donation',
        'schedule': timedelta(minutes=15)
    },
    'compute_pplns': {
        'task': 'simplecoin.tasks.update_pplns_est',
        'schedule': timedelta(minutes=15)
    },
    'update_online_workers': {
        'task': 'simplecoin.tasks.update_online_workers',
        'schedule': timedelta(minutes=2)
    },
    'server_status': {
        'task': 'simplecoin.tasks.server_status',
        'schedule': timedelta(minutes=2),
    },
}

database_tasks = {
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
    'share_cleanup': {
        'task': 'simplecoin.tasks.cleanup',
        'schedule': timedelta(hours=24),
    },
    'update_block_state': {
        'task': 'simplecoin.tasks.update_block_state',
        'schedule': timedelta(minutes=5),
    },
    'check_worker_down': {
        'task': 'simplecoin.tasks.check_down',
        'schedule': timedelta(minutes=1),
    },
    'update_coin_trans': {
        'task': 'simplecoin.tasks.update_coin_transaction',
        'schedule': timedelta(minutes=10),
    },
}

CELERYBEAT_SCHEDULE = caching_tasks
# we want to let celery run in staging mode where it only handles updating
# caches while the prod celery runner is handling real work. Allows separate
# cache databases between stage and prod
if not current_app.config.get('stage', False):
    CELERYBEAT_SCHEDULE.update(database_tasks)
