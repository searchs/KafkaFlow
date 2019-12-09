#!/usr/bin/env bash

#Producer
kafka-console-consumer --bootstrap-server localhost:9092 --topic movie_topic --group --from-beginning


#Consumer
