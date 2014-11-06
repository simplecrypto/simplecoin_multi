#!flask/bin/python
import sys
import subprocess
if len(sys.argv) != 2:
    print "usage: tr_init <language-code>"
    sys.exit(1)

subprocess.call('pybabel extract -F babel.cfg -o messages.pot .', shell=True)
subprocess.call('pybabel init -i messages.pot -d simplecoin/translations -l '  + sys.argv[1], shell=True)