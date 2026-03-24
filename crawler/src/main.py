from __future__ import annotations

from common import bootstrap  # noqa: F401
from common.cli import parse_args
from ktv.crawler import run as run_ktv
from movie.crawler import run as run_movie
from scenic.crawler import run as run_scenic
from show.crawler import run as run_show
from sport.crawler import run as run_sport


RUNNERS = {
    "scenic": run_scenic,
    "show": run_show,
    "ktv": run_ktv,
    "movie": run_movie,
    "sport": run_sport,
}


def main() -> None:
    args = parse_args()
    runner = RUNNERS[args.category]
    runner(sample=args.sample)


if __name__ == "__main__":
    main()
