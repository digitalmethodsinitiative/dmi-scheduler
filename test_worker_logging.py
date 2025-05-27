"""
Simplified test for worker logging
"""
import logging
import sys
import time

from dmi_scheduler.worker import BasicWorker
from dmi_scheduler.log_formatter import WorkerAwareFormatter

class DummyJob:
    """Mock job for testing"""
    data = {"id": 1}
    
    def finish(self):
        pass
    
    def release(self, delay=0):
        pass

class SimpleTestWorker(BasicWorker):
    """A simple test worker that logs messages"""
    type = "simple-worker"
    
    def work(self):
        """Test work method"""
        self.log.info("Starting work")
        time.sleep(0.5)
        self.log.info("Work in progress...")
        time.sleep(0.5)
        self.log.info("Work complete")

def setup_logging():
    """Set up a basic logger with our custom formatter"""
    # Set up logging to console
    handler = logging.StreamHandler(sys.stdout)
    
    # Use our custom formatter
    log_format = "%(asctime)s [%(levelname)-5.5s]%(workertype)s %(message)s"
    formatter = WorkerAwareFormatter(log_format)
    handler.setFormatter(formatter)
    
    # Configure the root logger
    logger = logging.getLogger("test")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    return logger

def main():
    """Run the test"""
    logger = setup_logging()
    
    # Log something from the main context (should have no worker type)
    logger.info("Logging from main context")
    
    # Create a worker
    job = DummyJob()
    manager = None  # We don't need a manager for this test
    worker = SimpleTestWorker(logger=logger, job=job, manager=manager)
    
    # Now log from the worker
    worker.start()
    worker.join()
    
    # Log something from the main context again
    logger.info("Logging from main context again")

if __name__ == "__main__":
    main()
