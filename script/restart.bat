docker-compose down
docker-compose --env-file .\.env up -d --remove-orphans --build
docker ps