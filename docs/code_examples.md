# Some Code Examples
## How to use the IoT Core Example
```bash
### For Distro Ubuntu20
# move iot device connector to server
scp -i KEY_FILE.pem connect_device_package.zip ubuntu@IP:/home/ubuntu/

# install necessary packages
sudo apt update
sudo apt install docker
sudo apt install docker-compose
sudo apt install pip
sudo apt install unzip

# set python3 to python for execution
sudo update-alternatives --install /usr/bin/python python /usr/bin/python3 1

chmod -x start.sh
./start.sh
```

## Necessary steps to run docker-container
```bash
apt install docker
apt install docker-compose

mkdir aws-certs

# move certificate files to directory for docker volume binding
mv root-CA.crt NAME.cert.pem NAME.private.key NAME.public.key aws-certificates/

docker-compose build
docker-compose up -d
```