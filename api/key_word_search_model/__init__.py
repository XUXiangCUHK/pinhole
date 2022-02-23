import os
import sys
dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(dir_path)

from .model_interface import load_model

model = load_model()
