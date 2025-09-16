#!/bin/bash

# Exit immediately if any command fails
set -e

echo "Installing required Python packages: infoworkssdk==5.10 and tabulate==0.9.0"
python -m pip install infoworkssdk==5.10 tabulate==0.9.0

if [ $? -ne 0 ]; then
    echo "Failed to install required Python packages."
    exit 1
fi

echo "Running main.py with config file..."
python main.py --config_ini_file ./config.ini

if [ $? -ne 0 ]; then
    echo "Execution of main.py failed."
    exit 1
fi

echo "Deployment completed successfully."