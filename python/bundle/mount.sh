#!/bin/sh

mount -o bind ~/Projects/sourced.ml $(dirname "$0")/ml
mount -o bind ~/Projects/spark-2.2.0-bin-hadoop2.7 $(dirname "$0")/spark
mount -o bind ~/Projects/sourced-engine $(dirname "$0")/engine
