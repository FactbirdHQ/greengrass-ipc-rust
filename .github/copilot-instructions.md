Create a rust port of the Greengrass IPC python library. The rust version should be async and tokio compatible.

Reference python implementation can be found in https://github.com/aws/aws-iot-device-sdk-python-v2, with the event stream implementation found in https://github.com/awslabs/aws-c-event-stream

You can run it using `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT=~/greengrass-data/ipc.socket SVCUID=K2G457TMH0JX7005 cargo r --example pubsub --release`, and check the server logs using `docker exec -it aws-iot-greengrass sh -c "cat /greengrass/v2/logs/greengrass.log"`, and the received messages using `docker exec -it aws-iot-greengrass sh -c "cat /greengrass/v2/logs/test"`
The connection part works as is, so focus on the publish part. You can use test_python.py as a reference for what ends up in the logs by executing `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT=~/greengrass-data/ipc.socket SVCUID=K2G457TMH0JX7005 python test_python.py`
