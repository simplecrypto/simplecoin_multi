import os

from . import create_app

kwargs = {}
if 'CONFIG' in os.environ:
    kwargs['config'] = os.environ['CONFIG']
app = create_app("webserver", **kwargs)
