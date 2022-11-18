#!/usr/bin/env python3
# coding: utf-8

from langx_tutti_client.scatt_controller import ScattController
from os import path, makedirs

def main(arguments):
    if arguments.transcript_file_path is None:
        scatt_data = ScattController.generate_empty_data()
    else:
        with open(arguments.transcript_file_path, 'rt') as file:
            scatt_data = ScattController.convert_to_data_for_cefr_scoring_from_intella_transcript(file.read())

    output_directory = arguments.output_directory
    if output_directory is None:
        output_directory = path.dirname(arguments.transcript_file_path)

    makedirs(output_directory, exist_ok=True)

    output_file_path = path.join(output_directory, path.splitext(arguments.transcript_file_path)[0] + '.json')
    with open(output_file_path, 'wt') as file:
        file.write(scatt_data)


if __name__ == '__main__':
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument(
        'transcript_file_path',
        help='video file path',
    )
    parser.add_argument(
        '-d', '--outdir',
        help='output directory path',
        dest='output_directory'
    )

    parsed_args = parser.parse_args()
    main(parsed_args)
