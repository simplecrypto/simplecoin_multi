from simpledoge import create_app
from simpledoge.tasks import celery
from celery.bin.worker import main
from flask import current_app


app = create_app()

with app.app_context():
    current_app.logger.info("Celery worker powering up... BBBVVVRRR!")
    main(app=celery)
