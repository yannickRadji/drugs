from os import path
from typing import Dict, List, Any
import traceback

from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, col, when, to_date, first


class Sanitizer:
    @staticmethod
    def deduplication(logger, df_dict: Dict[str, DataFrame], rules: Dict[str, List[str]]):
        """
        Deduplicate lines considering few columns and merge data from those duplicate
        Args:
            logger: Logger instance used to log events
            df_dict: Dictionary of the datasets with the structure {Name: Dataframe}
            rules: {Dataset Name: [column1, column2]

        Returns: Dic updated in place

        """
        try:
            for df_name, columns in rules.items():
                df_dict[df_name] = df_dict.get(df_name).groupBy(*columns) \
                    .agg(
                    *[first(x, ignorenulls=True).alias(x) for x in df_dict.get(df_name).columns if x not in columns])
            logger.info("Dataframes cleaning deduplication applied")
        except Exception as e:
            logger.error("Cleaning duplicate rows couldn't be performed: {}".format(e), traceback.format_exc())
            raise e

    @staticmethod
    def empty_str_cleaning(logger, df_dict: Dict[str, DataFrame], regex: str = r"^\s+$") -> Dict[str, DataFrame]:
        """
        Cleaning the empty string in the whole dataframe by setting them a null/none cell
        Args:
            logger: Logger instance used to log events
            df_dict: Dictionary of the datasets with the structure {Name: Dataframe}
            regex: The regex pattern

        Returns: An new dataset dict

        """
        try:
            res_dict = {}
            for df_name, df in df_dict.items():
                res_dict[df_name] = df
                for col_name in df.columns:
                    res_dict[df_name] = res_dict[df_name].withColumn(col_name, when(col(col_name).rlike(regex), None)
                                                      .otherwise(col(col_name)))
            logger.info("Dataframes cleaning empty string applied")
            return res_dict
        except Exception as e:
            logger.error("Cleaning empty string couldn't be performed: {}".format(e), traceback.format_exc())
            raise e

    @staticmethod
    def clean_date(logger, df_dict: Dict[str, DataFrame], date: str = "date") -> Dict[str, DataFrame]:
        """
        Casting to date the string date that have the following format dd/MM/yyyy, yyyy-MM-dd, d MMMM yyyy
        Args:
            logger: Logger instance used to log events
            df_dict: Dictionary of the datasets with the structure {Name: Dataframe}
            date: Name of the date column

        Returns: An new dataset dict

        """
        try:
            null_before = [df.filter(col(date).isNull()).count() for df in df_dict.values() if date in df.columns]
            df_dict.update({name: df.withColumn(date, when(col(date).rlike("(\d\d/\d\d/\d{4})"),
                                                           to_date(col(date), 'dd/MM/yyyy'))
                                                .when(col(date).rlike("(\d{4}-\d\d-\d\d)"),
                                                      to_date(col(date), 'yyyy-MM-dd'))
                                                .otherwise(to_date(col(date), 'd MMMM yyyy')))
                            for name, df in df_dict.items() if date in df.columns})
            null_after = [df.filter(col(date).isNull()).count() for df in df_dict.values() if date in df.columns]
            if null_before != null_after:
                logger.error("Some null have been introduced, you may have a non supported date format")
            logger.info("Dataframes dates cleaning applied")
        except Exception as e:
            logger.error("Cleaning dates couldn't be performed: {}".format(e), traceback.format_exc())
            raise e
        return df_dict

    @staticmethod
    def read_files(logger, spark, params_json: Dict[str, Any], input_path: str, encoding: str = "UTF-8") -> Dict[
        str, DataFrame]:
        """
        Read json & csv files based on schema & filename specified in the JSON with the correct encoding. Union files with same names & columns.
        To stay easily readable we choose to leave keep it simple but this could be more parametrized
        if more supported files option are needed in the future. Data is read as string to enable correct parsing before type casting.
        Args:
            logger: Logger instance used to log events
            spark: Spark instance
            params_json: {"csv" : [filename1, filename2], "json": [filename3]}
            input_path: Data folder path
            encoding: Needed encoding to read the data

        Returns: A dict with name: DataFrame

        """
        try:
            csv_filenames = params_json.get('csv')
            json_filenames = params_json.get('json')
            df_dict = {data_name: spark.read.format("csv").option("header", True).csv(
                "{0}.csv".format(path.join(input_path, data_name)), encoding=encoding) for data_name in csv_filenames}
            df_dict_json = {data_name: spark.read.format("json").option("multiLine", True).json(
                "{0}.json".format(path.join(input_path, data_name)), encoding=encoding) for data_name in json_filenames}
            for df_name in df_dict_json.keys():
                df_dict[df_name] = df_dict.get(df_name).unionByName(df_dict_json.get(df_name))
            logger.info("Dataframes retrieved: {}".format(df_dict))
        except Exception as e:
            logger.error("Reading data files couldn't be performed: {}".format(e), traceback.format_exc())
            raise e
        return df_dict

    @staticmethod
    def clean_strings(logger, df_dict: Dict[str, DataFrame], regex_literal_utf: str = r"(\\x.{2})+"):
        """
        Clean Dataframes strings by dropping anything that match the regex
        Args:
            logger: Logger instance used to log events
            df_dict: Dictionary of the datasets with the structure {Name: Dataframe}
            regex_literal_utf: The regex pattern

        Returns: Modification in place

        """
        try:
            for df_name in df_dict.keys():
                for col_name in df_dict.get(df_name).columns:
                    df_dict[df_name] = df_dict.get(df_name) \
                        .withColumn(col_name, regexp_replace(col(col_name), regex_literal_utf, ""))
            logger.info("Dataframes cleaning Regex applied")
        except Exception as e:
            logger.error("Cleaning Regex couldn't be performed: {}".format(e), traceback.format_exc())
            raise e
