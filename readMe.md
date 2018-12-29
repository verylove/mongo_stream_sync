mongoDB replica set reference by following url
https://blog.ruanbekker.com/blog/2017/08/27/setup-a-local-mongodb-development-3-member-replica-set/

mkdir -p ./mongodb/rs0-0 ./mongodb/rs0-1 ./mongodb/rs0-2
mkdir -p ./log/mongodb/rs0-0 ./log/mongodb/rs0-1 ./log/mongodb/rs0-2

mongod --port 27011 --dbpath ./mongodb/rs0-0 --replSet rs0 --smallfiles --oplogSize 128 --logpath ./log/mongodb/rs0-0/server.log --fork
mongod --port 27012 --dbpath ./mongodb/rs0-1 --replSet rs0 --smallfiles --oplogSize 128 --logpath ./log/mongodb/rs0-1/server.log --fork
mongod --port 27013 --dbpath ./mongodb/rs0-2 --replSet rs0 --smallfiles --oplogSize 128 --logpath ./log/mongodb/rs0-2/server.log --fork
