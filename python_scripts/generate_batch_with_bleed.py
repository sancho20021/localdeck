#!/usr/bin/env python3

from generate_batch import main
from generate_card_with_bleed import generate_card_with_bleed


if __name__ == "__main__":
    main(generate_card_with_bleed, ".pdf")
