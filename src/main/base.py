from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from src.utils.config_parser import read_config_file


class BaseClass(ABC):
    """Base class template"""

    job_name: str = None
    env: str = None

    def __init__(self, spark: SparkSession) -> None:
        """Constructor for the base Class.

        Args:
            spark (SparkSession): spark session object

        Returns:
            Object of the base class
        """
        self.spark = spark
        self.config = read_config_file(self.env)
        self.initialise_execution_variables()

    @abstractmethod
    def initialise_execution_variables(self) -> None:
        """abstract method for setting up arguments, needs to be set in the subclass declaration..
        """
