#!/bin/bash

# Activate the virtual environment
if [ -f .venv/bin/activate ]; then
    source .venv/bin/activate
else
    echo "Virtual environment not found. Please ensure .venv exists."
    exit 1
fi

echo directory: $(pwd)
# Run the Python script
if [ -f ambient_main.py ]; then
    python ambient_main.py
else
    echo "ambient_main.py not found. Please ensure the script exists in the correct location."
    exit 1
fi