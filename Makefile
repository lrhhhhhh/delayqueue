.PHONY:

up:
	@cd ./deployments && docker-compose up -d

down:
	@cd ./deployments && docker-compose down

clean:
	@cd ./deployments && sudo rm -rf ./kafka/* && sudo rm -rf ./zookeeper/*
