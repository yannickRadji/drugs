from json_cleaner.executor import Executor

if __name__ == '__main__':
    cleaner = Executor()
    kwargs = cleaner.parse_args()
    cleaner.execute(**kwargs)
