# US Motor Accidents Crash Analysis

## Pre-Requisites/Setup

To be able to submit a PySpark job using `spark-submit`, you'll need to ensure that your environment is correctly set up with the necessary Spark and Python installations, along with any dependencies your application may have. Here’s a checklist to guide you through the setup:

### 1. Install Apache Spark
   - Download Apache Spark from the [official Apache Spark website](https://spark.apache.org/downloads.html).
   - Choose a version compatible with your Hadoop version if you are running it on a cluster, or the standalone version if you are working locally.
   - Unzip the downloaded file and set the environment variables:
     ```cmd
     setx SPARK_HOME "\path\to\spark"
     setx PATH %PATH%;%SPARK_HOME%\bin
     ```
   - You can test your installation by running:
     ```bash
     spark-shell
     ```

### 2. Install Java
   - Apache Spark requires Java (Java Development Kit, JDK) to run. Spark 3.x+ typically works with Java 8 or Java 11.
   - Install JDK using a package manager (e.g., `sudo apt install openjdk-8-jdk` on Ubuntu) or from the [official Oracle Java website](https://www.oracle.com/java/technologies/javase-downloads.html).
   - Set up the JAVA_HOME environment variable:
     ```cmd
     setx JAVA_HOME "\path\to\java"
     setx PATH %PATH%;%JAVA_HOME%\bin
     ```

### 3. Set Up Python Environment (with PySpark)
   - Install poetry using `pip install poetry`
   - Run `poetry install -vvv` to install the dependencies in the pyproject.toml

### 4. Run Application
   - Run the below command to start the application
     ```cmd
     spark-submit src/jobs/case_study_job.py
     ```

## Assessment

### Dataset
You are provided with a folder containing 6 CSV files. Please refer to the attached data dictionary for understanding the dataset structure and contents.

### Required Analytics
Develop an application that performs the following analyses:

1. **Analytics 1**: Find the number of crashes (accidents) where the number of males killed is greater than 2.
2. **Analysis 2**: Count how many two-wheelers are booked for crashes.
3. **Analysis 3**: Determine the Top 5 Vehicle Makes of cars involved in crashes where the driver died and airbags did not deploy.
4. **Analysis 4**: Count the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
5. **Analysis 5**: Identify which state has the highest number of accidents where females are not involved.
6. **Analysis 6**: Determine the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including death.
7. **Analysis 7**: For all body styles involved in crashes, mention the top ethnic user group of each unique body style.
8. **Analysis 8**: Identify the Top 5 Zip Codes with the highest number of crashes attributed to alcohol (using Driver Zip Code).
9. **Analysis 9**: Count distinct Crash IDs where no damaged property was observed, and damage level is above 4, and the car is insured.
10. **Analysis 10**: Determine the Top 5 Vehicle Makes where drivers charged with speeding-related offenses have licensed drivers, use the top 10 vehicle colors, and are licensed in the Top 25 states with the highest number of offenses.

### Expected Output
1. Develop a modular application that adheres to software engineering best practices (e.g., classes, docstrings, functions, config-driven, command-line executable through `spark-submit`).
2. Organize code into appropriate folders as a project.
3. Use a config-driven approach for input data sources and output.
4. Code must strictly use DataFrame APIs (avoid using Spark SQL).
5. Share the entire project as a zip file or provide a link to the project on a GitHub repository.
