import traceback
from typing import Dict, List

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, array_contains, lit


class DrugsExtractor:
    @staticmethod
    def df_to_words(logger, df: DataFrame, input_col: str, output_col: str = "words", pattern: str = "\\W+",
                    to_lowercase: bool = True,
                    case_sensitive: bool = False) -> DataFrame:
        try:
            intermediate_output = output_col + "intermediate"
            regex_tokenizer = RegexTokenizer(inputCol=input_col, outputCol=intermediate_output, pattern=pattern,
                                             toLowercase=to_lowercase)
            remover = StopWordsRemover(inputCol=intermediate_output, outputCol=output_col, caseSensitive=case_sensitive)
            logger.info("Parsing to words the dataframe")
            return remover.transform(regex_tokenizer.transform(df)).drop(intermediate_output)
        except Exception as e:
            logger.error("Parsing to words failed: {}".format(e), traceback.format_exc())
            raise e

    @staticmethod
    def to_words(logger, df_dict: Dict[str, DataFrame], params: Dict[str, str]):
        """
        We create a words list from the string/sentences (the empty string are discarded) by tokenizing & removing stop
        words.
        :param logger:
        :param df_dict:
        :param params:
        :return:
        """
        for df_name, col_name in params.items():
            df_dict[df_name] = df_dict.get(df_name).filter(col(col_name).isNotNull())
            df_dict[df_name] = DrugsExtractor.df_to_words(logger, df_dict.get(df_name), col_name).drop(col_name)

    @staticmethod
    def pivot(logger, drugs_list: List[str], df_dict: Dict[str, DataFrame], words_col: str = "words"):
        """
        Create a new column by drug and set True if the drug is contain in the list of words in each row
        :param logger:
        :param drugs_list:
        :param df_dict:
        :param words_col:
        :return: Nothing update the dictionary in-place
        """
        try:
            for drug_name in drugs_list:
                for name, df in df_dict.items():
                    df_dict[name] = df.withColumn(drug_name, array_contains(col(words_col), drug_name))
            logger.info("Pivoted to {}".format(df_dict))
        except Exception as e:
            logger.error("Pivoting failed: {}".format(e), traceback.format_exc())
            raise e

    @staticmethod
    def shift(logger, drugs_list: List[str], df_dict: Dict[str, DataFrame], drug_col_name: str, spark: SparkSession,
              columns_kept: List[str]) -> Dict[str, DataFrame]:
        """
        From pivoted data (see dedicated method) shift the drug's name column to one column,
        we'll have a line per publication/drug
        :param logger:
        :param drugs_list:
        :param df_dict:
        :param drug_col_name:
        :param spark:
        :param columns_kept:
        :return:
        """
        try:
            res_dict = {
                name: spark.createDataFrame([], df.select(*columns_kept).withColumn(drug_col_name, lit("0")).schema) for
                name, df in df_dict.items()}
            for drug in drugs_list:
                for name, df in df_dict.items():
                    res_dict[name] = res_dict.get(name).unionByName(
                        df.filter(col(drug) == True).withColumn(drug_col_name, lit(drug)).select(drug_col_name, *columns_kept))

            logger.info("Shifted to {}".format(res_dict))
            return res_dict
        except Exception as e:
            logger.error("Shifting failed: {}".format(e), traceback.format_exc())
            raise e
