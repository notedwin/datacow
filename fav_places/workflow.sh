#!/bin/bash

main() {
    poetry run python3 process_takeout.py
    # take excel file to google sheet and add my ratings manually

    # manually export images then resize using
    poetry run python3 resize_images.py

    poetry run python3 place_id.py
    poetry run python3 place_info.py
    poetry run python3 hood_labels.py
    poetry run python3 create_map.py
}