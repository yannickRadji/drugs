import argparse
from os import path

from pyspark.sql.functions import col, lower, struct, collect_set, collect_list, lit, array_distinct, concat, to_json
from pyspark.sql.types import StringType, ArrayType, StructType, StructField

from data_ingestion.sanitizer import Sanitizer
from data_ingestion.files import Files
from data_ingestion.drugsextractor import DrugsExtractor
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
        parser.add_argument("output_path", type=str, help="Folder path to write files")
        parser.add_argument("on_dbfs", type=bool, help="DataBricks Filesystem is mounted", nargs="?", default=True)

        args = parser.parse_args()
        args_dict = args.__dict__
        self.logger.info("Arguments retrieved: {}".format(args_dict))

        return args_dict

    def execute(self, conf_path: str, input_path: str, output_path: str, on_dbfs: bool) -> None:
        """
        Pipeline that sanitize data, extract drugs and change the data model finally save to a JSON.
        This is the main entrypoint of the package. The parameters are the job's arguments.
        Args:
            conf_path: If DataBricks Filesystem is mounted
            input_path: Folder path to write files
            output_path: Folder path to read raw files
            on_dbfs: File path of the params.json

        Returns: Nothing only modify inplace the instanced class

        """

        self.load_params(conf_path)

        df_dict = Sanitizer.read_files(self.logger, self.spark, self.params, input_path)
        Sanitizer.clean_strings(self.logger, df_dict)
        df_dict = Sanitizer.clean_date(self.logger, df_dict)
        df_dict = Sanitizer.empty_str_cleaning(self.logger, df_dict)
        Sanitizer.deduplication(self.logger, df_dict, self.params.get("deduplication rules"))
        Files.merge_write(self.logger, df_dict, self.params.get("merge sanitized rules"),
                          path.join(output_path, "sanitized"), self.spark)

        df_dict = Files.read_delta(self.logger, set(self.params.get("csv") + self.params.get("json")),
                                   path.join(output_path, "sanitized"), self.spark)
        Sanitizer.deduplication(self.logger, df_dict, self.params.get("deduplication rules"))

        DrugsExtractor.to_words(self.logger, df_dict, self.params.get("to words"))

        drug_df_name = self.params.get("names").get("drugs")
        drug_col_name = self.params.get("names").get("drug")

        df_dict[drug_df_name] = df_dict.get(drug_df_name).withColumn(drug_col_name, lower(col(drug_col_name))).filter(
            col(drug_col_name).isNotNull())
        # To be refactor as it don't work in case of really large drug list because of collect to driver (below) and column creation (above)
        # need to drop duplicate because several drugs can have different atc code
        drugs_list = df_dict.get(drug_df_name).select(drug_col_name).drop_duplicates().toPandas()[
            drug_col_name].to_list()
        df_dict.pop(drug_df_name)

        for df in df_dict.values():
            df.cache()
        self.logger.info("Prepared drug list and cached dataframes for following intensive computation: {}".format(df_dict))

        DrugsExtractor.pivot(self.logger, drugs_list, df_dict)

        date = self.params.get("names").get("date")
        id_col = self.params.get("names").get("id")
        journal = self.params.get("names").get("journal")
        columns_kept = [date, id_col, journal]
        df_dict = DrugsExtractor.shift(self.logger, drugs_list, df_dict, drug_col_name, self.spark, columns_kept)

        # Construct publication objects and journal object
        for df_name in self.params.get("to words").keys():
            df_dict[df_name] = df_dict.get(df_name).withColumn(date, col(date).cast(StringType()))
            df_dict[df_name] = df_dict.get(df_name).withColumn(id_col, struct(col(date).alias(date), col(id_col).alias(id_col))) \
                .withColumn(journal, struct(col(date).alias(date), col(journal).alias(journal)))
        self.logger.info("Publication objects and journal object constructed: {}".format(df_dict))

        trial = self.params.get("names").get("clinical_trials")
        pubmed = self.params.get("names").get("pubmed")
        # Get of each drug a the list of journal and publication (we use set on journal to avoid duplicates)
        merge_trial_df = \
            df_dict.get(trial).groupby(drug_col_name)\
            .agg(collect_set(col(journal)).alias(journal), collect_list(col(id_col)).alias(trial))\
            .withColumn(pubmed, lit(None)
                        .cast(ArrayType(StructType([StructField('date', StringType(), True), StructField('id', StringType(), True)]))))
        self.logger.info("Created publication per drug for trials: {}".format(merge_trial_df))
        merge_pub_df = df_dict.get(pubmed).groupby(drug_col_name).agg(collect_set(col(journal)).alias(journal),
                                                                      collect_list(col(id_col)).alias(pubmed))
        self.logger.info("Created publication per drug for pubmed: {}".format(merge_pub_df))

        # Merge clinical trials publications with pubmed publication by drug with their associated journal (without repetition)
        merge_path = path.join(output_path, "enriched")
        Files.merge_write(self.logger, {trial: merge_trial_df},
                          self.params.get("merge sanitized rules"), merge_path, self.spark)
        delta_path = path.join(merge_path, trial)
        from delta.tables import DeltaTable
        delta_trial = DeltaTable.forPath(self.spark, delta_path)
        update_match = "trial.{0} = pub.{0}".format(drug_col_name)
        update = {pubmed: col(f"pub.{pubmed}"),
                  journal: array_distinct(concat(col(f"pub.{journal}"), col(f"trial.{journal}")))}
        insert = {
            pubmed: col(f"pub.{pubmed}"),
            journal: col(f"pub.{journal}"),
            drug_col_name: col(f"pub.{drug_col_name}"),
            trial: lit(None)
        }
        self.logger.info("Merging publications with the matching rule: {}".format(update_match))
        (
            delta_trial.alias("trial")
                .merge(merge_pub_df.alias("pub"), update_match)
                .whenMatchedUpdate(set=update)
                .whenNotMatchedInsert(values=insert)
                .execute()
        )
        # Save the end result
        graph_filename = self.params.get("names").get("graph_filename")
        json_df = self.spark.read.format("delta").load(delta_path)

        # To use the filesystem mounted on databricks with python process we need to prefix "/dbfs/" but Spark process don't work with this prefix
        pythonic_path = "/dbfs"+output_path if on_dbfs else output_path
        graph_path = path.join(pythonic_path, *graph_filename)
        json_df.withColumn(journal, to_json(col(journal))).withColumn(trial, to_json(col(trial))).withColumn(pubmed, to_json(col(pubmed))).toPandas().to_json(graph_path, orient="records", date_format="iso")
        # when used multiLine need to be enable on the reading spark process

        self.logger.info("Wrote the resulting JSON to: {}".format(graph_path))
