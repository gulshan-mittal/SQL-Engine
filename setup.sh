#!/bin/bash

virtualenv --python=$(which python3) venv 
source venv/bin/activate
pip install -r requirements.txt