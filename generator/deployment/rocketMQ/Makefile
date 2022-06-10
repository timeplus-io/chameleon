
createTopic:
	docker exec -it rmqbroker /home/rocketmq/rocketmq-4.9.3/bin/mqadmin updateTopic -c DefaultCluster -n namesrv:9876 -t test

consume:
	docker exec -it rmqbroker /home/rocketmq/rocketmq-4.9.3/bin/mqadmin consumeMessage -t test -n namesrv:9876