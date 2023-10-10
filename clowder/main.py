import os

from clowder.investigations import Investigations


def main():
    invs = Investigations()
    inv = invs.get_investigation("1vqj5NAhQFufODAbU94h50upfwbjRfml-")


if __name__ == "__main__":
    main()
