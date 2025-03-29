#!/bin/bash

# Navigate to the script directory
cd "$(dirname "$0")" || exit 1

# Activate the virtual environment
if [ -f ../.venv/bin/activate ]; then
    source ../.venv/bin/activate
else
    echo "Virtual environment not found. Please ensure ../.venv exists."
    exit 1
fi

# Run the Python script
if [ -f ../analysis/nws_main.py ]; then
    python ../analysis/nws_main.py
else
    echo "nws_main.py not found. Please ensure the script exists in the correct location."
    exit 1
fi