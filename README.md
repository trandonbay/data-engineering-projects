1. docker compose up airflow-init
2. docker exec -i postgres psql -U admin -d crypto > schema.sql
3. docker compose up
4. Go to Admin -> Connections -> Add(+)
  Connection Id: postgres_default
  Connection Type: Postgres
  Host: postgres
  Schema: crypto
  Login: admin
  Password: admin
  Port: 5432
5. Run your DAG
