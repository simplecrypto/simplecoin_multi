#!flask/bin/python
import subprocess
subprocess.call('pybabel extract -F babel.cfg -o messages.pot .', shell=True)
subprocess.call('pybabel update -i messages.pot -d simplecoin/translations', shell=True)
