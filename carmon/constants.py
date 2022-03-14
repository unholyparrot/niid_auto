"""
Здесь предполагается размещение общих для всех компонентов констант
"""
from os.path import split as split_it


WORKING_PATH = split_it(__file__)[0]

DEFAULT_RESPONSE = {
    "success": False,
    "payload": "Default response definitely means an error"
}
