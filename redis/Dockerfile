FROM redis:alpine

WORKDIR /src
COPY ./redis.sh .
HEALTHCHECK --interval=5s CMD [ "sh", "redis.sh" ]

EXPOSE 6379