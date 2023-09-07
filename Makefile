.PHONY:

up:
	@cd ./deployments && mkdir -m 0777 kafka zookeeper
	@cd ./deployments && docker-compose up -d

down:
	@cd ./deployments && docker-compose down

clean:
	@cd ./deployments && sudo rm -rf ./kafka && sudo rm -rf ./zookeeper
