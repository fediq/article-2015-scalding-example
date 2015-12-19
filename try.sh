#!/usr/bin/env bash

mvn clean package

CLASSPATH=$(find target -name "*.jar" | tr "\n" ":")

java -classpath "$CLASSPATH" com.twitter.scalding.Tool \
    ru.fediq.example.scalding1.TopicSwitchesJob \
	--local \
	--clicks src/test/data/clicks.tsv \
	--sites src/test/data/siteinfo.tsv \
	--output target/output.tsv

cat target/output.tsv
