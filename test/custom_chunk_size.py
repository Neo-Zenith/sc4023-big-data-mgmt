import argparse
import sys
import os
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../src')))


def custom_chunk_size():
    from main import main
    from constants import MAX_FILE_LINES

    # Get args from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_size", type=int, default=MAX_FILE_LINES)

    args = parser.parse_args()
    max_file_lines = args.chunk_size
    main(max_file_lines=max_file_lines)


if __name__ == "__main__":
    custom_chunk_size()
