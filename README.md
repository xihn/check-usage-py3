# check-usage-py3

```plaintext
python3 check-usage.py --help
usage: check-usage.py [-h] [-u USER] [-a ACCOUNT] [-E] [-s START] [-e END]

[version: 3.0]

options:
  -h, --help  show this help message and exit
  -u USER     Check usage of this user
  -a ACCOUNT  Check usage of this account
  -E          Expand user/account usage
  -s START    Start time (YYYY-MM-DD[THH:MM:SS])
  -e END      End time (YYYY-MM-DD[THH:MM:SS])
```



The old script was written in python2. Rewritten in python3. \
Can be used as `./check-usage.sh` if users want to save themselves from typing *seven* more characters


## Improvements

- updated to python 3
- replaced `urllib2`/`urllib` with `requests` library
- all main logic is encapsulated into functions with entry point (`main()`)
- timestamps are managed with timezone‑aware datetime objects (assuming naive datetimes are in UTC)
