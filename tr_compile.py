#!flask/bin/python
import subprocess
subprocess.call('pybabel compile -d simplecoin/translations', shell=True)
