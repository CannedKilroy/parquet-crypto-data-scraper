# Helper functions
import os
import sys
from pathlib import Path
import yaml

async def load_config():
    
    cwd = os.path.abspath(os.path.dirname(sys.argv[0]))  
    current_script_path = Path(__file__).resolve()
    project_root = current_script_path.parent.parent
    config_path = project_root / 'config' / 'config.yaml'
    
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)
