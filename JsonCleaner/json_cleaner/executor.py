import argparse
from os import path

from entities.abstract import Execute
from entities.base_execute import BaseExecute
from utils import utils as uts


class Executor(BaseExecute, Execute):
    def __init__(self, spark_session=None, spark_app=True):
        BaseExecute.__init__(self, spark_session, spark_app)

    def parse_args(self):
        self.logger.info("Parsing arguments")

        parser = argparse.ArgumentParser()
        parser.add_argument("conf_path", type=str, help="File path of the params.json")
        parser.add_argument("input_path", type=str, help="Folder path to read raw files")
        parser.add_argument("output_path", type=str, help="Folder path to write cleaned files")
        parser.add_argument("on_adls", type=bool, help="Data on Data Lake file system", nargs="?", default=False)

        args = parser.parse_args()
        args_dict = args.__dict__
        self.logger.info("Arguments retrieved: {}".format(args_dict))

        return args_dict

    def execute(self, conf_path: str, input_path: str, output_path: str, on_adls: bool) -> None:
        """
        Clean the list of json file gave through the params.json (create new data file)
        :param on_adls: Do the data are on the Data Lake
        :param output_path: Folder path to write files
        :param input_path: Folder path to read raw files
        :param conf_path: File path of the params.json
        :return: Nothing
        """
        self.load_params(conf_path)
        self.params.get("json")
        self.data_lake = uts.connect_to_data_lake_store(self.params) if on_adls else None

        for file in self.params.get("json"):
            json_file_name = "{}.json".format(file)
            read_path = path.join(input_path, json_file_name)
            self.logger.info("Reading and parsing JSON from: {}".format(read_path))
            data = uts.read_json(read_path, self.data_lake, advanced_parsing=True)

            write_path = path.join(output_path, json_file_name)
            self.logger.info("Writing the parsed JSON to: {}".format(write_path))
            uts.write_json(data, write_path, self.data_lake)
