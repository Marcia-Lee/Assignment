import socket
import threading
import sys
import logging
from collections import defaultdict
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define constants
REQUEST_MAX_LENGTH = 999
SUMMARY_INTERVAL = 10


# Server-side code
class Server:
    def __init__(self, port):
        self.port = port
        self.tuple_space = {}
        self.total_clients = 0
        self.total_operations = 0
        self.total_reads = 0
        self.total_gets = 0
        self.total_puts = 0
        self.total_errors = 0
        self.lock = threading.Lock()
