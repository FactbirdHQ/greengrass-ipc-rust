#!/usr/bin/env python3

import awsiot.greengrasscoreipc
from awsiot.greengrasscoreipc.model import (
    PublishToTopicRequest,
    PublishMessage,
    BinaryMessage
)

def main():
    try:
        ipc_client = awsiot.greengrasscoreipc.connect()

        message = "Hello from Python! Test message"

        request = PublishToTopicRequest()
        request.topic = "/test"
        request.publish_message = PublishMessage()
        request.publish_message.binary_message = BinaryMessage()
        request.publish_message.binary_message.message = message.encode('utf-8')

        # Debug: Print the request structure and exact bytes
        print(f"Python request topic: {request.topic}")
        print(f"Python request publish_message: {request.publish_message}")
        print(f"Python request binary_message: {request.publish_message.binary_message}")
        print(f"Python request binary_message.message: {request.publish_message.binary_message.message}")
        print(f"Python request binary_message.message bytes: {list(request.publish_message.binary_message.message)}")
        print(f"Python request has json_message: {hasattr(request.publish_message, 'json_message')}")
        if hasattr(request.publish_message, 'json_message'):
            print(f"Python request json_message: {request.publish_message.json_message}")

        operation = ipc_client.new_publish_to_topic()
        operation.activate(request)
        future = operation.get_response()

        result = future.result(timeout=10.0)
        print("Python publish successful!")

    except Exception as e:
        print(f"Python publish failed: {e}")
    finally:
        if 'ipc_client' in locals():
            ipc_client.close()

if __name__ == "__main__":
    main()
