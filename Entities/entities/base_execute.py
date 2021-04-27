import os
import traceback

from utils import utils as uts


class BaseExecute:
    """
    Base class for any Python Application to have consistency across them
    """
    def __init__(self, spark_session=None, spark_app: bool = True):
        """
        Initialize the application
        Args:
            spark_session: A spark instance if it is already existing like on DataBricks that provider their custom
            spark_app: If the application uses Spark
        """
        try:
            # Instantiate logger
            self.logger = uts.create_logger_basic()

            # Instantiate Spark Session
            if spark_app:
                # Imported there in order to not need to retrieve this large library if you don't use Spark
                from pyspark.sql import SparkSession

                self.spark = spark_session
                if not self.spark:
                    self.spark = SparkSession \
                        .builder \
                        .config("spark.jars.packages", "io.delta:delta-core_2.12:0.8.0") \
                        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                        .getOrCreate()
                    from delta.tables import DeltaTable

            # Variable for application parameters
            self.params = None

            # DataBricks mount data lake filesystem if we don't use it we may need our data lake connection
            if not spark_app:
                self.data_lake = None

            self.logger.info("Initialization of logging{} done".format("and spark" if spark_app else ""))
        except Exception as e:
            self.logger.error("Initialization of logging{0} couldn't be performed: {1}"
                              .format("and spark" if spark_app else "", e), traceback.format_exc())
            raise e

    def load_params(self, conf_path: str) -> None:
        """
        Retrieve the parameters file and store it in the class attributes.
        We arbitrary set that it should call params.json
        Args:
            conf_path:  path of the params.json

        Returns: Nothing the dict is stored in an attribute

        """
        try:
            # Parameters
            params_path = os.path.join(conf_path, "params.json")
            self.logger.info("Get parameters {}".format(params_path))
            self.params = uts.read_json(params_path)
            if self.spark:
                self.spark.sparkContext.broadcast(self.params)

        except Exception as e:
            self.logger.error("Initialization of params.json couldn't be performed: {}".format(e),
                              traceback.format_exc())
            raise e
