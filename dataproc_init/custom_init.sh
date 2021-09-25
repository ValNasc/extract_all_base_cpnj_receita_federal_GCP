#!/bin/bash

# instalando prerequisitos
apt-get -y update
apt-get -y install python3 python-dev build-essential python3-pip libpq-dev
easy_install3 -U pip
pip3 install --upgrade setuptools
apt-get -y install default-libmysqlclient-dev

# instalando packages
pip install --upgrade joblib
pip install --upgrade numpy
pip install --upgrade bs4
pip install --upgrade orjson
pip install --upgrade pandas
pip install --upgrade pandas-gbq
pip install --upgrade psycopg2-binary
pip install --upgrade pyspark
pip install --upgrade six
pip install --upgrade google-api-core==1.22.2
pip install --upgrade google-auth==1.21.1
pip install --upgrade google-auth-oauthlib==0.4.1
pip install --upgrade google-cloud==0.34.0
pip install --upgrade google-cloud-bigquery==1.27.2
pip install --upgrade google-cloud-core==1.4.1
pip install --upgrade google-cloud-storage==1.31.0
pip install --upgrade google-crc32c==1.0.0
pip install --upgrade google-resumable-media==1.0.0
pip install --upgrade google-cloud-bigquery-storage
pip install --upgrade gcsfs
pip install --upgrade python-dateutil
pip3 install --upgrade python-dateutil

# copiar credentials do bucket para o cluster 
cd /opt
gsutil -m cp -r gs://bucket/app/deploy/process_cnpj .

#criando variáveis de ambiente 
echo "export GOOGLE_APPLICATION_CREDENTIALS=/opt/process_cnpj/gcp-project-id.json" | tee -a /etc/profile.d/spark_config.sh /etc/*bashrc /etc/environment
echo "spark.jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar" >> /etc/spark/conf/spark-defaults.conf