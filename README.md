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
