#!/bin/bash

if [ ! -d "./venv" ]; then
	./setup.sh
fi

source venv/bin/activate
python run.py
deactivate
