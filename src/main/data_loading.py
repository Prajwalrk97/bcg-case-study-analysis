import os

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f

from src.main.base import BaseClass


class DataLoadingClass(BaseClass):
    """Class for data loading."""

    job_name = "Data Loading"

    def __init__(self, spark: SparkSession, env: str):
        """Constructor for the Data Loading Class.

        Args:
            spark (SparkSession): Spark session object
            env (str): local/dev/stage

        Returns:
            Object of DataLoadingClass class
        """
        self.spark = spark
        self.env = env
        super().__init__(spark)

    def initialise_execution_variables(self):
        """Setup data path variables"""
        self.charges_csv = self.config["data"]["input"]["charges"]
        self.damages_csv = self.config["data"]["input"]["damages"]
        self.endorse_csv = self.config["data"]["input"]["endorse"]
        self.primary_person_csv = self.config["data"]["input"]["primary_person"]
        self.restrict_csv = self.config["data"]["input"]["restrict"]
        self.units_csv = self.config["data"]["input"]["units"]
        self.data_dir = os.path.join(self.config["data"]["input"]["data_dir"])

    def _load_data(self, file_name: str) -> DataFrame:
        """Load a CSV file into a Spark DataFrame.

        Args:
            file_name (str): The name of the CSV file to load.

        Returns:
            DataFrame: A Spark DataFrame containing the data from the CSV file.
        """
        logger.info(f"Trying to read {file_name} from {self.data_dir}")
        file_path = os.path.join(self.data_dir, file_name)
        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
        return df

    def get_charges(self) -> DataFrame:
        """Load and return the Charges DataFrame."""
        return self._load_data(self.charges_csv)

    def get_damages(self) -> DataFrame:
        """Load and return the Damages DataFrame."""
        return self._load_data(self.damages_csv)

    def get_endorse(self) -> DataFrame:
        """Load and return the Endorse DataFrame."""
        return self._load_data(self.endorse_csv)

    def get_primary_person(self) -> DataFrame:
        """Load and return the Primary Person DataFrame."""
        return self._load_data(self.primary_person_csv)

    def get_restrict(self) -> DataFrame:
        """Load and return the Restrict DataFrame."""
        return self._load_data(self.restrict_csv)

    def get_units(self) -> DataFrame:
        """Load and return the Units DataFrame."""
        return self._load_data(self.units_csv)
