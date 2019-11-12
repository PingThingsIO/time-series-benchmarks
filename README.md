# Ping Things Time Series benchmarks

* NOTE * these are a work in progress, please read the roadmap below

There are many time series databases on the market at the moment and they each have their own
strengths and weaknesses. Some of these characteristics are qualitative or can be quantified without an experiment
for example "this database can scale out to a cluster" or "this database supports query X". Others need an experiment
run on controlled hardware in order to quantify the differences.

This repository is aimed at providing tools for benchmarking the latter. For a few databases, it will:

* Set up a cluster for you on AWS using KOPS
* Install and configure the database
* Run a set of tests against the database`that explore performance under different conditions
* Generate a report
* Clean up the cluster

Of course, the performance of a database depends very much on the configuration. We have used
the default configurations here, but if you know how the numbers for any database can be improved
with a configuration tweak, please open a PR or an issue.

## Roadmap

This repository is still new and we are actively working on it. In the near future we aim to

* Add more databases
* Add a variety of tests that represent real workloads, especially AI and ML.
* Add richer reporting, such as a generated PDF or notebook with graphs

## Using

Clone the repository and run

```
make cluster
# wait a bit
make cluster_validate
# wait for the ok
source activate
make <db>_system
# wait for ok
make <db>_benchmark
```
