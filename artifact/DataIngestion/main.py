from data_ingestion.executor import Executor

if __name__ == '__main__':
    ingestion = Executor()
    kwargs = ingestion.parse_args()
    ingestion.execute(**kwargs)
