

generate-devops-data:
	docker run -it --rm --entrypoint /tsbs_generate_data timeplus/tsbs:20220629 \
	--use-case="devops" --seed=123 --scale=10 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-02T00:00:00Z" \
    --log-interval="60s" --format="timescaledb" \
	| gzip > ./data/devops-data-devops.gz

generate-iot-data:
	docker run -it --rm --entrypoint /tsbs_generate_data timeplus/tsbs:20220629 \
	--use-case="iot" --seed=123 --scale=10 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-02T00:00:00Z" \
    --log-interval="60s" --format="timescaledb" \
	| gzip > ./data/iot-data-devops.gz


generate-queries:
	docker run -it --rm --entrypoint /tsbs_generate_queries timeplus/tsbs:20220629 \
		--use-case="iot" --seed=123 --scale=1 \
		--timestamp-start="2016-01-01T00:00:00Z" \
		--timestamp-end="2016-01-02T00:00:00Z" \
		--queries=1 --query-type="last-loc" --format="questdb" \
		| gzip > ./query/questdb-queries.gz


build:
	go build 

run:
	go run main.go