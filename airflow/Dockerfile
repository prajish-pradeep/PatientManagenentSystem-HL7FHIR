FROM apache/airflow:2.10.4

#switch to root user to install Java
USER root

#install Java and clean up
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean


#install required packages, including gcloud
RUN apt-get update && apt-get install -y \
    curl \
    gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && apt-get install -y google-cloud-sdk && \
    apt-get clean

#ensure the gcloud CLI is in PATH
ENV PATH="/usr/lib/google-cloud-sdk/bin:$PATH"
#sitch back to airflow user
USER airflow

#install Python packages
RUN pip install pandas pyodbc sqlalchemy faker requests