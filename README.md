# apache-beam-exploration

Repo for exploring the use of [Apache Beam](https://beam.apache.org/).

Repo currently focuses on following along with the beam "getting started"
materials: https://beam.apache.org/get-started/


## System check

To start, run the built-in copy of the word-count example with the following command,
just to make sure that Apache Beam is correctly installed.

```bash
python -m apache_beam.examples.wordcount_minimal \
  --input data/words.txt \
  --output data/wordcounts_official_example.txt
```

This outputs a file `wordcounts_official_example.txt-00000-of-00001`. Why doesn't it
match the requested output file name?


## Our own implementaiton of the example

```bash
python -m wordcount_example \
  --input data/words.txt \
  --output data/wordcounts_our_example.txt
```

The output file looks the same as the output file from the above example. There is
significantly less log output, however. Why is that?


## Seal tag data spike

```bash
python -m seal_csv_to_gpkg
```


## Useful resources

* [Basics of the Beam model](https://beam.apache.org/documentation/basics/)
* [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/)
