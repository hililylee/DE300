t immediately if a command exits with a non-zero status
set -e

# Give docker permission
sudo chmod 666 /var/run/docker.sock


# Create a Docker volume for data persistence
echo "Creating Docker volume: homework1-heart-disease"
docker volume create --name hw1-database

# Create a Docker network for container communication
echo "Creating Docker network: etl-database"
docker network create etl-database


# Build Jupyter Docker image
echo "Building Jupyter Docker image from dockerfile-jupyter"
docker build -f dockerfiles/dockerfile-jupyter -t jupyter-image

# Run Jupyter container with volume and network setup
echo "Starting Jupyter container"
docker run -it --network etl-database \
		   --name etl-container \
		   	   -v ./src:/app/src \
			   	   -v ./staging_data:/app/staging_data \
				   	   -p 8888:8888 \
					   	   jupyter-image
