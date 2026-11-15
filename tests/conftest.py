"""Rend le paquet `fraud_pipeline` importable par les tests, dans le conteneur comme en CI."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "dags"))
