import pyspark.sql.functions as f
from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from src.main.base import BaseClass
from src.main.data_loading import DataLoadingClass


class DataProcessingClass(BaseClass):

    def __init__(self, spark: SparkSession, env: str):
        """Constructor for the Data Loading Class.

        Args:
            spark (SparkSession): Spark session object
            env (str): local/dev/stage

        Returns:
            Object of DataLoadingClass class
        """
        self.data_loader = DataLoadingClass(spark, env)
        self.spark = spark
        self.env = env
        super().__init__(spark)

    def initialise_execution_variables(self):
        """Initialise variables for execution"""

        self.results_dir = self.config["data"]["output"]["results_dir"]
        self.charges_df = self.data_loader.get_charges()
        self.primary_person_df = self.data_loader.get_primary_person()
        self.units_df = self.data_loader.get_units()
        self.damages_df = self.data_loader.get_damages()
        self.restrict_df = self.data_loader.get_restrict()
        self.endorsements_df = self.data_loader.get_endorse()

    def _question1(self):
        """Find the number of crashes (accidents) where the number of males killed is greater than 2."""

        male_fatalities_df = self.primary_person_df.where(
            f.expr("PRSN_INJRY_SEV_ID == 'KILLED' and PRSN_GNDR_ID == 'MALE'")
        )
        male_fatality_count_df = male_fatalities_df.groupBy("CRASH_ID").agg(
            f.sum("DEATH_CNT").alias("total_male_deaths")
        )
        crashes_with_male_fatalities = male_fatality_count_df.filter(
            f.col("total_male_deaths") > 2
        )
        logger.success(
            f"Question 1: Accidents where Males killed is greater than 2 is - {crashes_with_male_fatalities.count()} \n\n"
        )

    def _question2(self):
        """ Count how many two-wheelers are booked for crashes."""

        motorcyle_crashes = self.units_df.filter(
            f.expr(
                "VEH_BODY_STYL_ID == 'POLICE MOTORCYCLE' or VEH_BODY_STYL_ID == 'MOTORCYCLE' "
            )
        )
        logger.success(
            f"Question 2: Accidents involving 2 wheelers  - {motorcyle_crashes.count()} \n\n"
        )

    def _question3(self):
        """Determine the Top 5 Vehicle Makes of cars involved in crashes where the driver died and airbags did not deploy"""

        fatalities_df = self.primary_person_df.where(
            f.expr(
                "PRSN_INJRY_SEV_ID == 'KILLED' and PRSN_AIRBAG_ID == 'NOT DEPLOYED' and PRSN_TYPE_ID == 'DRIVER'"
            )
        )
        filtered_units_df = self.units_df.filter(
            f.col("VEH_BODY_STYL_ID").isin(
                [
                    "PICKUP",
                    "SPORT UTILITY VEHICLE",
                    "PASSENGER CAR, 2-DOOR",
                    "PASSENGER CAR, 4-DOOR",
                    "TRUCK",
                    "POLICE CAR/TRUCK",
                ]
            )
        )
        fatalities_unit_df = fatalities_df.join(
            filtered_units_df, on=["CRASH_ID", "UNIT_NBR"], how="inner"
        )
        logger.success(
            f"Question 3: Top 5 Vehicle Makes of cars involved in crashes where the driver died and airbags did not deploy - \n\n"
        )
        fatalities_unit_df.groupBy("VEH_MAKE_ID").agg(
            f.count("VEH_MAKE_ID").alias("VEH_MAKE_COUNT")
        ).orderBy(f.desc(f.col("VEH_MAKE_COUNT"))).show(5)

    def _question4(self):
        """Determine number of Vehicles with driver having valid licences involved in hit and run?"""

        filtered_primary_person_df = self.primary_person_df.filter(
            f.expr("DRVR_LIC_TYPE_ID == 'DRIVER LICENSE' and PRSN_TYPE_ID == 'DRIVER' ")
        )
        filtered_units_df = self.units_df.filter(f.expr("VEH_HNR_FL == 'Y' "))
        joined_df = filtered_primary_person_df.join(
            filtered_units_df, on=["CRASH_ID", "UNIT_NBR"]
        )
        logger.success(
            f"Question 4: Number of Vehicles with driver having valid licences involved in hit and run - {joined_df.count()} \n\n"
        )

    def _question5(self):
        """Which state has highest number of accidents in which females are not involved"""

        female_crashes_df = self.primary_person_df.where(
            f.expr("PRSN_GNDR_ID != 'FEMALE'")
        )
        female_crashes_units_df = female_crashes_df.join(
            self.units_df, on=["CRASH_ID", "UNIT_NBR"]
        )
        logger.success(
            f"Question 5: State with highest number of accidents in which females are not involved - \n\n"
        )
        female_crashes_units_df.groupBy("VEH_LIC_STATE_ID").agg(
            f.count("VEH_LIC_STATE_ID").alias("VEH_LIC_STATE_COUNT")
        ).orderBy(f.desc("VEH_LIC_STATE_COUNT")).show(1)

    def _question6(self):
        """ Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death"""

        injuries_df = self.units_df.groupBy("VEH_MAKE_ID").agg(
            f.sum(f.col("TOT_INJRY_CNT") + f.col("DEATH_CNT")).alias("total_injuries")
        )
        injuries_df = injuries_df.withColumn(
            "dummy_col_for_partition", f.lit(1)
        )  # To avoid - WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
        window_fn = Window.orderBy(f.col("total_injuries").desc()).partitionBy(
            "dummy_col_for_partition"
        )
        logger.success(
            f"Question 6: Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death - \n\n"
        )
        injuries_df.withColumn("row_num", f.row_number().over(window_fn)).filter(
            f.col("row_num").between(3, 5)
        ).show()

    def _question7(self):
        """For all the body styles involved in crashes, mention the top ethnic user group of each unique body style"""

        units_persons_df = self.units_df.join(
            self.primary_person_df, on=["CRASH_ID", "UNIT_NBR"]
        )
        units_persons_df = units_persons_df.select(
            "VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"
        ).filter(~f.col("VEH_BODY_STYL_ID").isin(["NA", "UNKNOWN"]))
        ethnicity_counts = units_persons_df.groupBy(
            "VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID"
        ).agg(f.count("*").alias("ethnicity_count"))

        window_fn = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(
            f.desc("ethnicity_count")
        )
        top_ethnic_group = (
            ethnicity_counts.withColumn("rank", f.row_number().over(window_fn))
            .filter(f.col("rank") == 1)
            .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID", "ethnicity_count")
            .orderBy(
                f.col("VEH_BODY_STYL_ID"),
                f.col("PRSN_ETHNICITY_ID"),
                f.desc(f.col("ethnicity_count")),
            )
        )
        logger.success(
            f"Question 7: Top ethnic user group of each unique body style - \n\n"
        )
        top_ethnic_group.show(truncate=False)

    def _question8(self):
        """Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)"""

        alcohol_crash_cars_df = self.units_df.filter(
            (
                f.col("VEH_BODY_STYL_ID").isin(
                    [
                        "PICKUP",
                        "SPORT UTILITY VEHICLE",
                        "PASSENGER CAR, 2-DOOR",
                        "PASSENGER CAR, 4-DOOR",
                        "TRUCK",
                        "POLICE CAR/TRUCK",
                    ]
                )
            )
            & (
                f.col("CONTRIB_FACTR_1_ID").isin(
                    ["HAD BEEN DRINKING", "UNDER INFLUENCE - ALCOHOL"]
                )
                | f.col("CONTRIB_FACTR_2_ID").isin(
                    ["HAD BEEN DRINKING", "UNDER INFLUENCE - ALCOHOL"]
                )
            )
        )
        car_alcohol_crashes_with_zip_df = (
            alcohol_crash_cars_df.join(
                self.primary_person_df, on=["CRASH_ID", "UNIT_NBR"], how="inner"
            )
            .select("DRVR_ZIP")
            .filter(f.col("DRVR_ZIP").isNotNull())
        )

        logger.success(
            "Question 8: Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash - \n\n"
        )
        car_alcohol_crashes_with_zip_df.groupBy("DRVR_ZIP").count().orderBy(
            f.desc("count")
        ).limit(5).show()

    def _question9(self):
        """Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance"""

        unit_with_damage_df = self.units_df.join(
            self.damages_df, on="CRASH_ID", how="left"
        )

        filtered_crashes_df = unit_with_damage_df.filter(
            (f.col("DAMAGED_PROPERTY").isNull())
            & (
                (
                    f.col("VEH_DMAG_SCL_1_ID").isin(
                        ["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"]
                    )
                )
                | (
                    f.col("VEH_DMAG_SCL_2_ID").isin(
                        ["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"]
                    )
                )
            )
            & (f.col("FIN_RESP_TYPE_ID") != "NA")
        )
        logger.success(
            f"Question 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance - {filtered_crashes_df.select('CRASH_ID').distinct().count()}"
        )

    def _question10(self):
        """Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)"""

        speeding_charges_df = self.charges_df.filter(f.expr("CHARGE LIKE '%SPEED%'"))

        licensed_drivers_df = speeding_charges_df.join(
            self.endorsements_df, ["CRASH_ID", "UNIT_NBR"], "inner"
        ).join(self.restrict_df, ["CRASH_ID", "UNIT_NBR"], "inner")

        top_colors_df = (
            self.units_df.groupBy("VEH_COLOR_ID")
            .count()
            .orderBy(f.col("count").desc())
            .limit(10)
            .select("VEH_COLOR_ID")
            .collect()
        )
        top_25_colors = [row["VEH_COLOR_ID"] for row in top_colors_df]

        top_states_df = (
            speeding_charges_df.join(self.units_df, ["CRASH_ID", "UNIT_NBR"], "inner")
            .groupBy("VEH_LIC_STATE_ID")
            .count()
            .orderBy(f.col("count").desc())
            .limit(25)
            .select("VEH_LIC_STATE_ID")
            .collect()
        )

        top_25_states = [row["VEH_LIC_STATE_ID"] for row in top_states_df]

        filtered_units_df = self.units_df.filter(
            (f.col("VEH_COLOR_ID").isin(top_25_colors))
            & (f.col("VEH_LIC_STATE_ID").isin(top_25_states))
        )
        result_df = (
            licensed_drivers_df.join(
                filtered_units_df, ["CRASH_ID", "UNIT_NBR"], "inner"
            )
            .groupBy("VEH_MAKE_ID")
            .count()
            .withColumn("rank", f.rank().over(Window.orderBy(f.col("count").desc())))
            .filter(f.col("rank") <= 5)
        )

        logger.success(
            "Question 10: Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences - \n\n"
        )
        result_df.select("VEH_MAKE_ID", "count").show()

    def process_data(self):
        self._question1()
        self._question2()
        self._question3()
        self._question4()
        self._question5()
        self._question6()
        self._question7()
        self._question8()
        self._question9()
        self._question10()
