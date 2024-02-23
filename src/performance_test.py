import argparse
from main import main
from constants import *

def test():
    ## Get args from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk_size", type=int, default=MAX_FILE_LINES)

    args = parser.parse_args()
    max_file_lines = args.chunk_size
    main(max_file_lines=max_file_lines)

test()