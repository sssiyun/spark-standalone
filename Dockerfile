FROM bitnami/spark:latest

RUN pip install pandas numpy scikit-learn matplotlib

# Add the MySQL JDBC driver and the demo script
ADD jars/mysql-connector-j-8.4.0.jar /opt/bitnami/spark/jars/
