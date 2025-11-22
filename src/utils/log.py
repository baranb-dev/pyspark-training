"""
Logging utilities for PySpark training project.

This module provides colored logging functionality and decorators for tracking
function execution in PySpark applications.
"""

import logging
from functools import wraps
from typing import Callable, Any
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    """
    Custom logging formatter that applies color coding to log messages.
    
    Colors:
        - ERROR: Red
        - INFO (success messages): Green
        - INFO (general messages): Blue
    """
    
    def format(self, record): # type: ignore
        """
        Format the log record with appropriate color based on log level and message content.
        
        Args:
            record: LogRecord object containing the log information.
            
        Returns:
            str: Formatted log message with color codes.
        """
        if record.levelno == logging.ERROR:
            record.msg = f"{Fore.RED}{record.msg}{Style.RESET_ALL}"
        elif record.levelno == logging.INFO and "completed successfully" in str(record.msg):
            record.msg = f"{Fore.GREEN}{record.msg}{Style.RESET_ALL}"
        elif record.levelno == logging.INFO:
            record.msg = f"{Fore.BLUE}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


# Configure logging with colored formatter
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
))
logging.basicConfig(
    level=logging.INFO,
    handlers=[handler]
)


def log_execution(func: Callable) -> Callable: # type: ignore
    """
    Decorator that logs function execution details.
    
    This decorator logs when a function is called and whether it completed
    successfully or failed with an error. Success messages are displayed in
    green, while errors are displayed in red with full stack traces.
    
    Args:
        func: The function to be decorated.
        
    Returns:
        Callable: Wrapped function with logging capabilities.
        
    Example:
        @log_execution
        def my_function(param1, param2):
            # function logic
            return result
    """
    logger = logging.getLogger(func.__module__)
    
    @wraps(func) # type: ignore
    def wrapper(*args, **kwargs) -> Any: # type: ignore
        """
        Wrapper function that adds logging around the decorated function.
        
        Args:
            *args: Positional arguments passed to the decorated function.
            **kwargs: Keyword arguments passed to the decorated function.
            
        Returns:
            Any: The return value from the decorated function.
            
        Raises:
            Exception: Re-raises any exception from the decorated function after logging.
        """
        func_name = func.__name__
        logger.info(f"Calling {func_name.capitalize()}")
        
        try:
            result = func(*args, **kwargs) # type: ignore
            logger.info(f"{func_name.capitalize()} completed successfully.")
            return result # type: ignore
        except Exception as e:
            logger.error(f"{func_name.capitalize()} failed with error: {str(e)}", exc_info=True)
            raise
    
    return wrapper # type: ignore

