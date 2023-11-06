import os

from clowder.investigations import Investigations


def main():
    investigations = Investigations()
    investigation = investigations.get_investigation("17Hk0tmveLWqLnJH-qMeqyU7cUepZKUQW")
    investigation.setup_investigation()


if __name__ == "__main__":
    main()
