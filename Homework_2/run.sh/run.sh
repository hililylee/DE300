<<<<<<< HEAD
#!/bin/bash

#Exit immediately if a command exits with a non-zero status
=======
t immediately if a command exits with a non-zero status
>>>>>>> dd05f990fffc311aaff457ae0681a86259bd1fc0
set -e

# Give docker permission
sudo chmod 666 /var/run/docker.sock


# Create a Docker volume for data persistence
<<<<<<< HEAD
echo "Creating Docker volume: homework2-heart-disease"
docker volume create --name heartdisease-database

# Create a Docker network for container communication
echo "Creating Docker network: etl2-database"
docker network create etl2-database
=======
echo "Creating Docker volume: homework1-heart-disease"
docker volume create --name hw1-database

# Create a Docker network for container communication
echo "Creating Docker network: etl-database"
docker network create etl-database
>>>>>>> dd05f990fffc311aaff457ae0681a86259bd1fc0


# Build Jupyter Docker image
echo "Building Jupyter Docker image from dockerfile-jupyter"
<<<<<<< HEAD
docker build -f dockerfiles/dockerfile-jupyter -t jupyter-image .

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --network etl2-database \
		   --name etl2-container \
=======
docker build -f dockerfiles/dockerfile-jupyter -t jupyter-image

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --network etl-database \
		   --name etl-container \
>>>>>>> dd05f990fffc311aaff457ae0681a86259bd1fc0
		   	   -v ./src:/app/src \
			   	   -v ./staging_data:/app/staging_data \
				   	   -p 8888:8888 \
					   	   jupyter-image
