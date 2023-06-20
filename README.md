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


## Update 2023-06-23

I have been working on a bunch of various aspects to an all emcompassing data platform for myself.

As I learn new things about what information is valueable and what information I can get, I will be adding to this repo.

Each folder is a roughly a differnt project/data source that I am working on.
There needs to be some data exploration before I can decide what to do with it.
For example: The access logs processing is roughly finalized, you can view the dashboard at [map.notedwin.com](https://map.notedwin.com).

The screentime data is still being explored, I am not sure what I want to display.
Also, this data is incomplete since I am using a third party app to track it.
I would like to get the data directly from my phone, but I have not found an easy way to do this.

The spotify data is also incomplete, I am still working on getting the data from spotify.
I can't continously get the data as their API only lets you get the last 50 songs played.

The FPL data is the newest project, I am still working on getting the data using web scraping.

I want to make a small machine learning application that will predict what Fantasy premier league will perform the best given previous seasons stats and some other data.