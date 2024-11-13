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