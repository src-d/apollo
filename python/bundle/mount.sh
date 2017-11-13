#!/bin/sh

mount -o bind /home/sourced/Projects/ast2vec $(dirname "$0")/ast2vec
mount -o bind /home/sourced/Projects/spark-2.2.0-bin-hadoop2.7 $(dirname "$0")/spark
mount -o bind /home/sourced/Projects/sourced-engine $(dirname "$0")/engine
