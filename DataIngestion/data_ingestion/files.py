import traceback
from os import path
from typing import Dict, Set

from pyspark.sql import DataFrame, SparkSession


class Files:
    @staticmethod
    def merge_write(logger, df_dict: Dict[str, DataFrame], rules: Dict[str, str], output_path: str, spark: SparkSession):
        """
        Csx
        :param spark:
        :param output_path:
        :param rules:
        :param logger:
        :param df_dict:
        :return:
        """
        try:
            from delta.tables import DeltaTable
            for df_name, df in df_dict.items():
                file_path = path.join(output_path, df_name)
                if DeltaTable.isDeltaTable(spark, file_path):
                    delta_table = DeltaTable.forPath(spark, file_path)
                    delta_table.alias("old").merge(df.alias("new"), rules.get(
                        df_name)).whenMatchedUpdateAll().whenNotMatchedInsertAll()
                else:
                    df.write.format("delta").save(file_path)

        except Exception as e:
            logger.error("Writing sanitized data couldn't be performed: {}".format(e), traceback.format_exc())
            raise e
        else:
            logger.info("Sanitized dataframes written in {} folder".format(output_path))

    @staticmethod
    def read_delta(logger, files_names: Set[str], files_path: str, spark: SparkSession) -> Dict[str, DataFrame]:
        """
        sss
        :param spark:
        :param files_path:
        :param files_names:
        :param logger:
        :return:
        """
        try:
            logger.info("Reading dataframes {0} from {1} folder".format(files_names, files_path))
            return {file: spark.read.format("delta").load(path.join(files_path, file)) for file in files_names}
        except Exception as e:
            logger.error("Writing sanitized data couldn't be performed: {}".format(e), traceback.format_exc())
            raise e