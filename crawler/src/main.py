from __future__ import annotations

from common import bootstrap  # noqa: F401
from common.cli import parse_args
from rental.crawler import run as run_rental


RUNNERS = {
    "rental": run_rental,
}


def main() -> None:
    args = parse_args()
    runner = RUNNERS[args.category]
    runner(sample=args.sample)


if __name__ == "__main__":
    main()
