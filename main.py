"""
Car Manufacturing Data Cleaning & Transformation - Entry Point
Run the full pipeline with:  python main.py
"""

import os
import sys

# Ensure project root is on the Python path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.pipeline.pipeline_runner import run_pipeline


if __name__ == "__main__":
    run_pipeline()
