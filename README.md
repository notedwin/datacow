# Data Cow

```bash
 _________________________________
| I never did it that way before. |
 ---------------------------------
        \   ^__^
         \  (oo)\_______
            (__)\       )\/\
                ||----w |
                ||     ||
```

Data Cow is a collection of small tools used to move data around. It is composed of a bunch of python scripts currently.

All scripts are designed to be run from the command line with a .env file in the same directory.

Current work is on unifying scripts into a standalone executable that runs pipelines on a scheduled basis.
This will be done by moving pipelines into an orchestrator such as dagster.


Work is done to improve grafana dashboard:
[map.notedwin.com](https://map.notedwin.com)

Most of the files are standalone files that process json or API requests.
