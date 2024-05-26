
# Big Data Project - Identifying Live Stream Highlights

A project for practice using Big Data tools (and a little bit of ML ;D)


## Run Locally

Clone the project

```bash
  git clone https://github.com/almostbilly/bd_project
```

Go to the project directory

```bash
  cd bd_project
```

Create virtual environment

```bash
  python -m venv venv
```

Activate virtual environment

```bash
  .\venv\Scripts\activate - for Windows
  source venv/bin/activate - for Linux and MacOS
```

Install dependencies

```bash
  pip install -r requirements.txt
```

### REQUIRED: 
  - Fill .env file

    `CLIENT_ID=<API_TOKEN>` from Twitch

    `SECRET_KEY=<SECRET_TOKEN>` from Twitch
  - Specify VOD IDs of Twitch in configs/parser.yaml

    For example copy from https://www.twitch.tv/videos/{some_video_id} ``some_video_id`` and put in ``configs/parser.yaml``
    ```yaml
    video_ids:
      - {some_video_id}
      - {some_video_id_2}
    ```

  - (Optional) Specify credentials of Clickhouse-server in ``configs/clickhouse``

### Run [ClickHouse Server Docker Image](https://hub.docker.com/r/clickhouse/clickhouse-server/)

```bash
  docker run -d --name some-clickhouse-server -p 8123:8123 -p 9000:9000 --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

`-p 8123:8123` is needed for HTTP protocol (for example, you can use DBeaver)

`-p 9000:9000` is needed for TCP protocol (it is required for working python clickhouse-driver)

### Run ``src/collect.py``

```bash
  python -m src.collect
```

