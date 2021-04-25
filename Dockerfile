FROM databricksruntime/standard:latest
RUN pip install --upgrade pip
RUN pip install pipenv
RUN pipenv install