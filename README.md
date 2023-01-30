# scraper

Fetches and stores raw bus api requests

# Setup Poetry

scraper uses Poetry for dependency management. To install Poetry, follow the instructions [here](https://python-poetry.org/docs/#installation).

If you are using VSCode, run the following command:

```bash
poetry config virtualenvs.in-project true
```

More information can be found [here](https://stackoverflow.com/questions/59882884/vscode-doesnt-show-poetry-virtualenvs-in-select-interpreter-option).

Once Poetry is installed, setup the virtual environment every time you start a new shell session by running:

```bash
poetry shell
```

More details can be found [here](https://python-poetry.org/docs/basic-usage/#activating-the-virtual-environment).

Install dependencies on first project run:

```bash
poetry install
```

You're good to go!

# Running

```bash
python -m scraper
# or
python scraper
```

# Environment details

By default this runs on an AWS EC2 instance

The script requires a .env file for api secrets. Contact the project owner for the .env file.

# Code details:

Scraping scheduling handled with the `apscheduler` library:

https://apscheduler.readthedocs.io/en/3.x/

# Supervisord

- Process managed using Supervisor
  - http://supervisord.org/introduction.html
- Handles auto-starting the process, restarting if it crashes, and redirecting stdout/stderr to a log file.
- Config file is located in `/etc/supervisord.conf`
- Monitor processes using the `supervisorctl` command (will need sudo)

## Using supervisord

- Start service if not running already
  - `sudo service supervisord start`
- Monitor processes using the `supervisorctl` command (will need sudo)
  - Launches you into its own shell
  - `start [program]` to start a program
  - `stop [program]` to stop a program

## Monitoring live logs

- In the `/home/ec2-user/rts-scraper/` directory, run `tail -f [file].log`
  - `scraper_stdout.log`
  - `scraper_stderr.log`

## Editing config

- Edit the config file in `/etc/supervisord.conf`
- Run `supervisorctl reread`
- Run `supervisorctl update`

## Config for scraper

```
[program:scraper]
directory=/home/ec2-user/rts-scraper
command=/home/ec2-user/rts-scraper/.venv/bin/python scraper
user=ec2-user
redirect_stderr=True
stdout_logfile=/home/ec2-user/rts-scraper/scraper_stdout.log
stderr_logfile=/home/ec2-user/rts-scraper/scraper_stderr.log
autorestart=true
autostart=true
```

## Session Manager:

- Add these to the PATH because they're not there for some reason
  - `export PATH="/usr/local/bin:$PATH"`
  - `export PATH="/root/.local/bin:$PATH"`
