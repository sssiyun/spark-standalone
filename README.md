# spark-standalone
This is a demo to deploy standalone spark with docker.

# Getting Started
Please ensure you have docker installed.

Build spark image
```
docker build -t custom-spark:latest .
```

Start master and worker
```
docker-compose up -d
```

Test Spark UI http://localhost:8080

# Test script
Run without using the standalone cluster
```
python spark_job.py
```

Run by spark submit
```
docker run --network="host" -v $(pwd):/app custom-spark:latest spark-submit --master spark://localhost:7077 /app/spark_job.py
```

