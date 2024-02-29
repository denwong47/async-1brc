"""
A quick utility to generate lines for me to test on a train without internet.
"""

import argparse
import random
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field, field_validator
from tqdm import tqdm

PRESET_TOWNS=[
    "Springfield",
    "Hogwarts",
    "Sodor",
    "Whiterun",
    "Falador",
]

MAX_TEMP = 100
MIN_TEMP = -100

DEFAULT_ROWS = 1_000

class CliArgs(BaseModel):
    """
    The arguments passed in from CLI.
    """
    path: Path
    rows: int = Field(DEFAULT_ROWS)

    @field_validator("path", mode="before")
    @classmethod
    def _pathify(cls, value: str) -> Path:
        path = (Path(".") / value).absolute()
        if path.is_dir():
            raise ValueError(
                f"Cannot write to a directory: {path}"
            )
        
        return path

def gen_one_line(
    place_name: Optional[str] = None,
) -> str:
    """
    Generate one line.
    """
    place_name = place_name or random.sample(PRESET_TOWNS, 1)[0]
    temp = random.randrange(MIN_TEMP * 10, MAX_TEMP * 10, 1) / 10
    return f"{place_name};{temp:.1f}"

def init_args() -> CliArgs:
    """
    Initialize the arguments captured.
    """
    parser = argparse.ArgumentParser(
        prog="line_gen",
        description="Generate lines for 1brc testing.",
    )
    parser.add_argument(
        "--file",
        dest="file",
        default="test_rows.txt",
        help=(
            "The file name to generate to. "
            "This can be a relative or absolute path."
        ),
        required=False,
    )
    parser.add_argument(
        "-n",
        "--rows",
        dest="rows",
        default=DEFAULT_ROWS,
        help=(
            "The number of rows to generate."
        ),
        required=False,
    )

    args = parser.parse_args()

    return CliArgs.model_validate(
        dict(
            path=args.file,
            rows=args.rows,
        )
    )

def gen_lines_to_file(
    path: Path,
    rows: int,
):
    with open(path, "w") as _f:
        for _ in tqdm(range(rows)):
            _f.write(gen_one_line())
            _f.write("\n")

if __name__ == "__main__":
    args = init_args()
    gen_lines_to_file(args.path, args.rows)