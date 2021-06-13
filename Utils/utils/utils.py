import json
import logging

encoding = "utf-8"


def create_logger_basic():
    # TODO add logs sinking to Azure Monitor (already

    # Get parameters for logger
    log_level = "INFO"
    log_format = '%(asctime)s - %(filename)s - %(funcName)s() : %(message)s'
    log_dateformat = "%Y-%m-%d %H:%M:%S"

    # Instantiate logger
    logger = logging.getLogger("Processor Logger")

    # Avoid duplicates logging
    if not logger.hasHandlers():
        logger.setLevel(log_level)

        # create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        # create formatter and add it to the handlers
        formatter = logging.Formatter(log_format, log_dateformat)

        console_handler.setFormatter(formatter)

        # add the handlers to logger
        logger.addHandler(console_handler)

    return logger


def connect_to_data_lake_store(config):
    from azure.datalake.store import core, lib
    # Imported there in order to not need to retrieve this large library if you don't use this function

    """
        Connection to Data Lake Store
        This connector is based on conf file and provide a AzureDLFileSystem object in order to read
        and write on the Data Lake Store.
        :return: environment file system
        :rtype: AzureDLFileSystem object
    """
    token = lib.auth(tenant_id=config.get('tenantId'),
                     username=config.get('username'),
                     password=config.get('password'))
    adls_account_name = config.get('accountName')
    adl = core.AzureDLFileSystem(token, store_name=adls_account_name)
    return adl


class CustomOpen(object):
    """
        Class that open file according to the environment

        :param1 filename: string
        :param2 mode: 'adl' | ''
        :param3 mode: 'rb' by default
        :return: file object

        :Example:

        >>> with CustomOpen(path, 'adl', 'wb') as f:
                f.write(str.encode(csv_file))
                f.close()
        >>> with CustomOpen(path, '', '') as f:
                df= pd.read_excel(f, sheetname=None)

    """

    def __init__(self, filename, mode="rb", adl=None):
        if adl:
            self.file = adl.open(filename, mode)
        else:
            self.file = open(filename, mode)

    def __enter__(self):
        return self.file

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        self.file.close()


def __write(data, path, adl, formatting):
    with CustomOpen(filename=path, mode='wb', adl=adl) as f:
        f.write(str.encode(formatting(data), encoding=encoding))
        f.close()


def write_csv(csv_data, path, adl):
    """
        Write a CSV file in a path

        :param1 csv_file: csv data to output
        :param2 path: path in the file system
        :param3 adl: environments azure data lake

        :Example:

        >>> write_csv(data_csv_file, "/home/data.csv", adl)

    """
    __write(csv_data, path, adl, lambda x: x)


def write_json(json_data, path, adl):
    """
    Write a JSON file in a path with by overwriting/truncate
    :param json_data:
    :param path:
    :param adl:
    :return:
    """
    __write(json_data, path, adl, json.dumps)


def read_json(paramfilepath, adl=None, advanced_parsing=False):
    """
    Read JSON file by reading in binary (bytes) then decoding in UTF8 (not possible with jsoncomment)
    :param paramfilepath:
    :param adl:
    :param advanced_parsing: If this set to True read in text mode then parse:
    trailing commas, comments and multi line data strings
    :return:
    """
    if advanced_parsing:
        from jsoncomment import JsonComment
        parser = JsonComment(json)
        with CustomOpen(paramfilepath, mode="r", adl=adl) as data_file:
            data = parser.load(data_file)
        return data
    else:
        with CustomOpen(filename=paramfilepath, adl=adl) as data_file:
            data = json.loads(data_file.read().decode(encoding))
        return data
