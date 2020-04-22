IMAGE_NAME := oxidb
IMAGE_TAG := latest

build:
	docker build . -t ${IMAGE_NAME}:${IMAGE_TAG}

run:
	docker run --rm -it ${IMAGE_NAME}:${IMAGE_TAG}
