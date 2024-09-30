FROM postgres:15

WORKDIR /src
COPY ./postgres.sh .

EXPOSE 5432

HEALTHCHECK --interval=5s CMD [ "sh", "postgres.sh" ]

# volumes: "db-data:/var/lib/postgresql/data"
