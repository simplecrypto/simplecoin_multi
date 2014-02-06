#!/bin/bash
cat util/init.sql | sudo -u postgres psql -d template1
