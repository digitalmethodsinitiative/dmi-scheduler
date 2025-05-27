"""
Custom log formatter to include worker type in log messages
"""
import logging


class WorkerAwareFormatter(logging.Formatter):
    """
    A formatter that includes worker type in log messages when available
    
    This formatter looks for a _worker_type attribute that's added by
    BasicWorker's WorkerTypeFilter. If found, it includes the worker type
    in brackets in the log message. If not, it uses an empty string.
    """
    def format(self, record):
        # Initialize the workertype attribute if it doesn't exist
        if not hasattr(record, 'workertype'):
            record.workertype = ""
            
            # Check for the _worker_type attribute (set by WorkerTypeFilter)
            if hasattr(record, '_worker_type'):
                record.workertype = f" [{record._worker_type}]"
        
        # Use the standard formatter
        return super().format(record)
