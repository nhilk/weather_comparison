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
if [ -f source_comparison.py ]; then
    python source_comparison.py
else
    echo "source_comparison.py not found. Please ensure the script exists in the correct location."
    exit 1
fi