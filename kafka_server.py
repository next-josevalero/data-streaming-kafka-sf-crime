import producer_server


def run_kafka_server():
    # The json file path
    input_file = "police-department-calls-for-service.json"

    # Instantiate the producer
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.udacity.sfcrime.calls",
        bootstrap_servers="localhost:9092",
        client_id="0"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
