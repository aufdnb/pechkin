# -*- coding: utf-8 -*-

"""Console script for pechkin."""
import json
import sys
import click
from .pechkin import Space
from datetime import datetime
import logging
import dateparser

@click.command()
@click.option("--start-date", help='Use the following format: %Y-%m-%d-%H-%M-%S')
@click.option("--end-date")
@click.option('--format', type=click.Choice(['JSON', 'CSV'], case_sensitive=False))
@click.option("--destination")
def main(start_date, end_date, format, destination):
    start_date = dateparser.parse(start_date)
    end_date = dateparser.parse(end_date)

    assert start_date < end_date, "Start date should be smaller than end date"

    with open("/home/router/auf_dnb/.config") as fp:
        configs = json.load(fp)

    space = Space(aws_access_key_id=configs["aws_access_key_id"],
                  aws_secret_access_key=configs["aws_secret_access_key"])

    space.save_posts(start_date,
                     end_date,
                     destination,
                     is_json=format == "JSON",
                     select_fields=["title", "created_utc", "selftext", "author_premium", "author_fullname", "id"])

    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover