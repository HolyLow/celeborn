# [WIP] Celeborn Cpp Support

## Environment Setup
We provide several methods to setup dev environment for CelebornCpp.
Note that currently the scripts only take care of the cpp-related dependencies, 
and java dependencies are not included.

### Use container with prebuilt image
We provide a pre-built image ready to be pulled and used so you could launch a container directly:
```
export PROJECT_DIR=/your/path/to/celeborn/dir
docker run \
    -v ${PROJECT_DIR}:/celeborn \
    -w /celeborn \
    -it --rm \
    --name celeborn-cpp-dev-container \
    holylow/celeborn-cpp-dev:0.1 \
    /bin/bash
```

### Build image and use container
We provide the dev image building scripts so you could build the image and launch a container as follows:
```
cd scripts

# build image
bash ./build-docker-image.sh

# launch container with image above
bash ./launch-docker-container.sh
```

### Build on local machine (Ubuntu-Only)
Currently, we only provide the dev-environment setup script for Ubuntu:
```
cd scripts
bash setup-ubuntu.sh
```
Other platforms are not supported yet, and you could use the container above as your dev environment.
