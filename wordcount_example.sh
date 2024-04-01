#!/bin/bash

python -m apache_beam.examples.wordcount_minimal --input words.txt --output wordcounts.txt
