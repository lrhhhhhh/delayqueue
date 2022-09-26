.PHONY:

up:
	@cd ./kafka && docker-compose up -d

down:
	@cd ./kafka && docker-compose down


clean:
	@cd ./kafka && sudo rm -rf ./kafka/* && sudo rm -rf ./zookeeper/*
