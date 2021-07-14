.PHONY: out build

IMAGE_TAG=$(shell whoami)

out:
	mkdir -p out

build: out
	GOOS=linux go build -o out/wlapi-csi-driver
	docker build . -t gcr.io/f5-gcs-7899-ptg-nginx-mesh-dev/wlapi-csi-driver:$(IMAGE_TAG)

container:
	docker push gcr.io/f5-gcs-7899-ptg-nginx-mesh-dev/wlapi-csi-driver:$(IMAGE_TAG)


clean:
	rm -rf out