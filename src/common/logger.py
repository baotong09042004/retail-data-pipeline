import logging
import sys

def setup_logger():
       
    logger = logging.getLogger("app")
    logger.setLevel(logging.DEBUG)
    
    logger.handlers.clear()
  
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO) 
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    console.setFormatter(formatter)
    
    logger.addHandler(console)
    
    return logger

def get_logger(name=None):
    if name:
        return logging.getLogger(f"app.{name}")
    return logging.getLogger("app")