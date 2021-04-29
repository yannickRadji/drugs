FROM databricksruntime/standard:latest
ENV PATH="/databricks/conda/bin:${PATH}"
ARG PACKAGE_FOLDER=feature
ARG PARENT_PK_FOLDER=Feature
COPY ./drugs/ /opt/
COPY ./data /opt/data/
COPY ./delta /databricks/conda/lib/python3.7/site-packages
WORKDIR /opt/drugs/$PARENT_PK_FOLDER
RUN pip install --upgrade pip
RUN pip install pipenv
RUN pipenv install
RUN pipenv shell
RUN python setup.py bdist_egg
ENTRYPOINT ["spark-submit"]
CMD ["--master", "local", "--py-files", "dist/{$PACKAGE_FOLDER}-1.0.0-py3.7.egg", "{$PACKAGE_FOLDER}/main.py"]