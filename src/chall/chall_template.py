"""
Abstract base template for PySpark challenges.

This module defines the abstract structure that all challenge modules
must implement, ensuring consistency across different challenges.
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional
from pyspark.sql.dataframe import DataFrame

from pyspark.testing import assertDataFrameEqual # type: ignore

from utils.log import log_execution # type: ignore

class Challenge(ABC):
    """
    Abstract base class for PySpark training challenges.
    
    Each challenge must implement both the answer() method (solution)
    and the solution() method (expected correct solution) for validation.
    """
    
    @abstractmethod
    def answer(self) -> Optional[DataFrame]:
        """
        Implementation of the challenge.
        
        This method should be implemented for attempting the challenge.
        It will be compared against the solution() method for validation.
        
        Returns:
        Dataframe: The result of implementation.
        """
        return None
    
    @abstractmethod
    def solution(self) -> DataFrame:
        """
        Expected correct solution for the challenge.
        
        This method contains the reference implementation used to validate
        the implementation. The output should match the answer() output
        when correctly implemented.
        
        Returns:
        Dataframe: The expected result of the challenge.
        """
        pass
    
    @abstractmethod
    def get_challenge_ressource(self) -> Dict[str,DataFrame]:
        """
        Provides necessary resources for the challenge.
        
        This method should return a dictionary of DataFrames or other
        resources required to attempt the challenge.
        
        Returns:
        Dict[Dataframe]: A dictionary containing challenge resources.
        """
        pass

    @log_execution
    def validate(self) -> bool:
        """
        Validate the student's answer against the solution.
        
        Compares the output of answer() with solution() to determine
        if the challenge was completed correctly.
        
        Returns:
        bool: True if answer matches solution, False otherwise.
        """
        try:
            student_result = self.answer()
            expected_result = self.solution()
            if assertDataFrameEqual(student_result, expected_result) is not None: # type: ignore
                return False
            return True

        except Exception as e:
            print(f"Validation failed with error: {str(e)}")
            return False
        


    


