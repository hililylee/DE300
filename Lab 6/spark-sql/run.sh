docker run -v /home/ubuntu/DE300/DE300/lab_doc/lab6/spark-sql:/tmp/spark-sql -it \
           -p 8888:8888 \
           --name spark-sql-container \
	   pyspark-image
