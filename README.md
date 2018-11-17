# Split the Monolith

BE SURE THAT `python3` and `pip3` are referring to `python 3.7.x`.
To find your `python` and `pip` version, run this commands:

```bash
$ python3 --version
> Python 3.7.0
$ pip3 --version
> pip 18.0 from /usr/local/lib/python3.7/site-packages/pip (python 3.7)
```


Once you found commands refering to the correct version, use them in the following scripts.

- Open a new terminal and run `redis-server`

  `$ redis-server`

- Open a new terminal and run strata-worker

  ```bash
  cd <YOUR_DIRECTORY>/BeepBeep-data-pump/
  pip3 install -r requirements.txt
  export DATA_SERVICE=http://127.0.0.1:5002
  celery -A datapump.datapump beat -l info
  ```
