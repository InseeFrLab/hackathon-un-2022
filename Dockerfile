FROM inseefrlab/onyxia-python-datascience:py3.10.4

# set api as the current work dir
WORKDIR /app

# copy the requirements lists
COPY ./requirements.txt /app/requirements.txt

# install all the requirements
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# copy the main code
COPY utils /app/utils
COPY app.py /app/app.py

# set up python path for the added source
ENV PYTHONPATH "${PYTHONPATH}:/app"

# call the function
CMD ["python3", "app.py"]
