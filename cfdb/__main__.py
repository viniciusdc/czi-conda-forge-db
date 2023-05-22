import sys

from cfdb.main import app


def main():
    app(sys.argv[1:])


if __name__ == "__main__":
    main()
