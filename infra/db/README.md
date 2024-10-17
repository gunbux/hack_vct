## db infra

This infrastructure aims to convert the compressed json files into a queryable database. There are two docker compose files, one for the importer, and one for
the deployment of grafana + db.

## Setting up

Ensure docker is installed to run this.

Copy the docker compose you want to run:

```bash
cp docker-compose-import.yml docker-compose.yml
```

To run the db, and import all games, run:

```bash
cd $DB
docker compose up --build --force-recreate
```

## To inspect docker containers

### Inspecting mysql
```bash
docker exec -it mysql bash
mysql -u esports_user -p esports_db # Password is esports_password
SELECT * FROM games LIMIT 5;
```

### Inspecting importer.py

```bash
docker exec -it importer bash
python importer.py
```

## For a clean build

```bash
docker system prune --force --all
docker volume rm db_mysql_data
```
