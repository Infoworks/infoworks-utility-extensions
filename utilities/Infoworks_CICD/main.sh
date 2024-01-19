#!/bin/bash
if [ ! -d "~/actions-runner/deployment_env/" ]; then
  python3 -m venv ~/actions-runner/deployment_env/
fi
source ~/actions-runner/deployment_env/bin/activate
pip list | grep infoworkssdk
if [ $? -ne 0 ]; then
  pip install infoworkssdk==3.0a3
fi
python main.py