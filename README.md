# ExternalScheduler
## Installation 
To setup the project, you should make a python VENV, which needs some packages:

First install Dask by using:
```python -m pip install dask distributed --upgrade```

Then, for the dashboard, dask will need bokeh>=3.1.0 to work.
- Install with conda: ```conda install "bokeh>=3.1.0"```
- Install with pip: ```pip install "bokeh>=3.1.0"```

## Execution
To run the project, just use the run button of your IDE.
After it has been run it should have a dashboard available at 
```http://127.0.0.1:8787/status```


To enter DAS virtual python environment input command:
source /.venv/bin/activate

# Use proper python version:
- wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
- chmod +x Miniconda3-latest-Linux-x86_64.sh
- ./Miniconda3-latest-Linux-x86_64.sh
- conda create -n myenv python=3.10
- conda activate myenv

# For evaluation:
- cd $HOME
- wget https://download.java.net/java/GA/jdk23.0.1/c28985cbf10d4e648e4004050f8781aa/11/GPL/openjdk-23.0.1_linux-x64_bin.tar.gz
- tar -xvf openjdk-23.0.1_linux-x64_bin.tar.gz
- export JAVA_HOME=~/jdk-23.0.1
- python ./BasicExperimentSetup.py data:./data/baby_dataset.parquet
- python ./BasicExperimentSetup.py data:./data/baby_dataset.parquet dstr
