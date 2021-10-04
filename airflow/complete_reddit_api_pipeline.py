from __future__ import print_function

import datetime

# -*- coding: utf-8 -*-
from airflow import models
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators import bash_operator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from datetime import datetime
timestamp=datetime.now()
date_val = timestamp.strftime("%Y_%m_%d")

with models.DAG(
        'reddit_api_pipeline',
        schedule_interval: '@hourly', //schedule hourly for 100 hottest api
        schedule_interval=None,
        catchup = False,
        default_args=default_dag_args) as dag:

    # An instance of an operator is called a task. In this case, the
    # hello_python task calls the "greeting" Python function.
    create_table = bash_operator.BashOperator(
        task_id='create_table_palego_assignment',
        bash_command="python3 /home/ubuntu/kafka-client/kafka_2.12-2.2.1/create_table.py")
    kafka_producer = bash_operator.BashOperator(
        task_id='kafka_producer_code',
        bash_command="python3 /home/ubuntu/kafka-client/kafka_2.12-2.2.1/producer.py")
     kafka_consumer = bash_operator.BashOperator(
        task_id='kafka_consumer_code',
        bash_command="python3 /home/ubuntu/kafka-client/kafka_2.12-2.2.1/consumer.py")
###There will be dependency only for kafka producer once table is created, but kafka consumer will be running in parellel on hourly basis, so no dependency defined for the same
create_table >> kafka_producer   
   
   
