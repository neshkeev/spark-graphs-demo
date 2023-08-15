[![Binder](https://img.shields.io/badge/Open%20In%20Binder-908a85?logo=jupyter)](https://mybinder.org/v2/gh/neshkeev/spark-graphs-demo/HEAD)
[![Gitpod](https://img.shields.io/badge/Open%20in%20Gitpod-908a85?logo=gitpod)](https://gitpod.io/#https://github.com/neshkeev/spark-graphs-demo)

# Large Graph Processing with Apache Spark

The repo contains graph algorithms based on [Pregel](https://dl.acm.org/doi/abs/10.1145/1807167.1807184) implemented from scratch.

# Quick start

The easiest way to start is to use one of the cloud providers (free):

- [![Binder](https://img.shields.io/badge/Open%20In%20Binder-908a85?logo=jupyter)](https://mybinder.org/v2/gh/neshkeev/spark-graphs-demo/HEAD). When loaded go to: `src` -> `pregel.ipynb`;
- [![Gitpod](https://img.shields.io/badge/Open%20in%20Gitpod-908a85?logo=gitpod)](https://gitpod.io/#https://github.com/neshkeev/spark-graphs-demo). When loaded go to `work` -> `pregel.ipynb`.

## Local setup

In order to setup the local environment one needs to install Docker and Git.

When the needed software installed, please execute:

1. clone the repo: `git clone https://github.com/neshkeev/spark-graphs-demo.git`
1. enter the directory: `cd spark-graphs-demo`
1. execute: `docker compose up`
1. open Jupyter Lab in the web browser: [http://localhost:8888/lab](http://localhost:8888/lab);
1. In the sidebar with files go to: `work` -> `pregel.ipynb`.

