import traceback
from typing import Dict, List

from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, array_contains, lit


class DrugsExtractor:
    """
    Give abilities to parse a dataframe with Drug's word to change its data model
    """
    @staticmethod
    def df_to_words(logger, df: DataFrame, input_col: str, output_col: str = "words", pattern: str = "\\W+",
                    to_lowercase: bool = True,
                    case_sensitive: bool = False) -> DataFrame:
        """
        Take each string in a column and parse it to a list of words via Tokenization and remove stop words.
        Args:
            logger: Logger instance used to log events
            df: Dataframe used
            input_col: Selected input column name
            output_col: Output column name
            pattern: The regex pattern used to tokenized
            to_lowercase: If all the word should be trim
            case_sensitive: Does the stop words should be case sensitive

        Returns: The modified dataframe

        """
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
        Args:
            logger: Logger instance used to log events
            df_dict: Dictionary of the datasets with the structure {Name: Dataframe}
            params:

        Returns: Nothing update the dictionary in-place

        """
        for df_name, col_name in params.items():
            df_dict[df_name] = df_dict.get(df_name).filter(col(col_name).isNotNull())
            df_dict[df_name] = DrugsExtractor.df_to_words(logger, df_dict.get(df_name), col_name).drop(col_name)

    @staticmethod
    def pivot(logger, drugs_list: List[str], df_dict: Dict[str, DataFrame], words_col: str = "words") -> None:
        """
        Create a new column by drug and set True if the drug is contain in the list of words in each row
        Args:
            logger: Logger instance used to log events
            drugs_list:
            df_dict: Dictionary of the datasets with the structure {Name: Dataframe}
            words_col:

        Returns: Nothing update the dictionary in-place

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
        Args:
            logger: Logger instance used to log events
            drugs_list: The list of drugs to shift
            df_dict: Dictionary of the datasets with the structure {Name: Dataframe}
            drug_col_name: The name that will have the added column
            spark: Spark instance
            columns_kept: The list of columns to keep at the end of the process

        Returns: A new modified datasets' dictionary

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
