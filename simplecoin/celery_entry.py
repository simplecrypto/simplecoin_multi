from simplecoin import create_app
from simplecoin.tasks import celery
from celery.bin.worker import main
from flask import current_app


app = create_app(celery=True)
# import celerybeat settings
celery.config_from_object('celeryconfig')
celery.conf.update(app.config['celery'])

with app.app_context():
    current_app.logger.info("Celery worker powering up... BBBVVVRRR!")
    main(app=celery)
