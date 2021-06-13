import argparse
from os import path
from typing import List

from pyspark.sql.functions import col, explode, count, max, from_json
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

from entities.abstract import Execute
from entities.base_execute import BaseExecute


class Executor(BaseExecute, Execute):
    def __init__(self, spark_session=None):
        BaseExecute.__init__(self, spark_session)

    def parse_args(self):
        self.logger.info("Parsing arguments")

        parser = argparse.ArgumentParser()
        parser.add_argument("conf_path", type=str, help="File path of the params.json")
        parser.add_argument("input_path", type=str, help="Folder path to read raw files")

        args = parser.parse_args()
        args_dict = args.__dict__
        self.logger.info("Arguments retrieved: {}".format(args_dict))

        return args_dict

    def execute(self, conf_path: str, input_path: str) -> List[str]:
        """
        Get the list of journals that have the most distinct drugs

        Args:
            conf_path: Folder path to read raw files
            input_path: File path of the params.json

        Returns: The result list

        """
        self.load_params(conf_path)
        graph_filename = self.params.get("names").get("graph_filename")
        drug = self.params.get("names").get("drug")
        journal = self.params.get("names").get("journal")
        struct_col = "struct_col"
        count_name = "count"
        json_path = path.join(input_path, *graph_filename)

        self.logger.info("Reading JSON data from: {}".format(json_path))
        df_graph = self.spark.read.json(json_path)\
            .withColumn("journal", from_json(col(journal), ArrayType(StructType([StructField("date", StringType(), True), StructField("journal", StringType(), True)])))).select(drug, journal)

        self.logger.info("Aggregate data to have number of distinct drugs per journal")
        df_exploded = df_graph.select(col(drug), explode(col(journal)).alias(struct_col))

        agg_df = df_exploded.select(col(drug), col(f"{struct_col}.{journal}")).distinct()\
            .groupBy(col(journal)).agg(count(col(drug)).alias(count_name))

        maxint = agg_df.select(max(count_name).alias(count_name)).toPandas()[count_name].to_list()[0]
        result = agg_df.filter(col(count_name) == maxint).toPandas()[journal].to_list()
        self.logger.info("The journal that have the most are: {}".format(result))

        return result
