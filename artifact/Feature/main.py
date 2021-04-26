from feature.executor import Executor

if __name__ == '__main__':
    feature = Executor()
    kwargs = feature.parse_args()
    feature.execute(**kwargs)
