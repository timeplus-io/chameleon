
run:
	docker-compose up -d

stop:
	docker-compose down

consume:
	docker exec -it kafka kafka-console-consumer  --topic device_utils --bootstrap-server localhost:9092

produce_avro:
	docker exec -it kafka kafka-console-consumer  --topic device_utils --bootstrap-server localhost:9092
