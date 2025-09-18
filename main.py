""" BACKUP PROGRAM
-- Author : Ahda Akmalul Ilmi

The program will check for file size and make decision to compress it or not, then save the hash for later be used to check any file changes.

This program main purpose is to be used as a steam and epic games backup.
"""

from BACKUP import backup_file, backup_folder

import time
from pathlib import Path
from rich import print

import typer

app = typer.Typer(no_args_is_help=True)


@app.command(no_args_is_help=True)
def backup(path: Path, destination: Path, meta_file: Path = Path.home().joinpath("backup_meta.json")):
    """
    Backup your file or folder.

    --path          : "C:/Program Files/HWiNFO64/HWiNFO Manual.pdf"

    --destination   : "C:/Program Files"
    """
    if not path.exists():
        print(f"[red][x] Error[/red]: Path '{path}' does not exist.")
        return

    if path.is_file():
        backup_file(path, destination, meta_file)
        return

    if path.is_dir():
        backup_folder(path, destination)
        return
    return


@app.command(no_args_is_help=True)
def restore(backup_path: Path, destination: Path):
    print(f"[red]No implementation yet.[/red]")
    return


if __name__ == "__main__":
    app()  # Typer
