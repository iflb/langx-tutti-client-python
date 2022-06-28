from os import path
import re

FFPROBE_COMMAND = 'ffprobe'
FFMPEG_COMMAND = 'ffmpeg'
SCATT_MODULES_DIR = path.dirname(__file__)
FFMPEG_MD5_PATH = path.join(SCATT_MODULES_DIR, '.{0:s}.md5'.format(FFMPEG_COMMAND))
SUPPORTED_FILE_EXTENSIONS_LIST_PATH = path.join(path.dirname(__file__), '.supported_video_file_extensions')

LINE_FEED_REGEX = re.compile('\n')
DEMUXER_FILE_EXTENSION_NAMES_REGEX = re.compile('Common extensions: (.+)\.$', re.MULTILINE)

class ScattVideoUtilsError(Exception):
    def __init__(self, message):
        self.message = message


def get_waveform_digest_generator_path():
    import platform
    platform_system_name = platform.system()
    platform_machine_name = platform.machine()
    waveform_digest_generator_path = None
    waveform_digest_generator_filename = 'create_waveform_digest'
    if platform_system_name == 'Darwin':
        if platform_machine_name == 'x86_64':
            waveform_digest_generator_path = path.join(SCATT_MODULES_DIR, 'bin', 'macosx_x86_64', waveform_digest_generator_filename)
        elif platform_machine_name == 'arm64':
            waveform_digest_generator_path = path.join(SCATT_MODULES_DIR, 'bin', 'macosx_arm64', waveform_digest_generator_filename)
        else:
            raise ScattVideoUtilsError('{0:s}/{1:s} is unexpected platform.'.format(platform_system_name, platform_machine_name))

    elif platform_system_name == 'Linux':
        if platform_machine_name == 'x86_64':
            waveform_digest_generator_path = path.join(SCATT_MODULES_DIR, 'bin', 'linux_x86_64', waveform_digest_generator_filename)
        elif platform_machine_name == 'x86':
            waveform_digest_generator_path = path.join(SCATT_MODULES_DIR, 'bin', 'linux_x86', waveform_digest_generator_filename)
        else:
            raise ScattVideoUtilsError('{0:s}/{1:s} is unexpected platform.'.format(platform_system_name, platform_machine_name))

    elif platform_system_name == 'Windows':
        if platform_machine_name == 'x86_64':
            waveform_digest_generator_path = path.join(SCATT_MODULES_DIR, 'bin', 'windows_x86_64', waveform_digest_generator_filename + '.exe')
        elif platform_machine_name == 'x86':
            waveform_digest_generator_path = path.join(SCATT_MODULES_DIR, 'bin', 'windows_x86', waveform_digest_generator_filename + '.exe')
        else:
            raise ScattVideoUtilsError('{0:s}/{1:s} is unexpected platform.'.format(platform_system_name, platform_machine_name))

    if not path.exists(waveform_digest_generator_path):
        raise ScattVideoUtilsError('{0:s} is not found.'.format(waveform_digest_generator_path))
    
    return waveform_digest_generator_path


def _generate_normalized_video_file_h264(input_video_file_path: str, output_video_file_path: str) -> None:
    import subprocess
    commands = [
        FFMPEG_COMMAND,
        '-i', input_video_file_path,
        '-ss', '0',
        '-acodec', 'copy',
        '-fflags', '+genpts',
        '-loglevel', 'level+error',
        '-y',
        output_video_file_path,
    ]
    process_result = subprocess.run(commands, capture_output=True)
    if process_result.returncode != 0:
        error_message = process_result.stderr.decode('utf-8')
        raise ScattVideoUtilsError('failed to normalize video file: \n{0:s}'.format(error_message))


def _get_start_frame_idx(input_video_file_path: str) -> int:
    import subprocess
    import json
    commands = [
        FFPROBE_COMMAND,
        input_video_file_path,
        '-select_streams', 'v',
        '-show_entries', 'frame=coded_picture_number',
        '-print_format', 'json',
        '-read_intervals', '%1',
        '-loglevel', 'level+error',
    ]
    process_result = subprocess.run(commands, capture_output=True)
    if process_result.returncode != 0:
        error_message = process_result.stderr.decode('utf-8')
        raise ScattVideoUtilsError('failed to get start frame index: \n{0:s}'.format(error_message))

    ffprobe_out = json.loads(process_result.stdout.decode('utf-8'))
    coded_picture_numbers = list(map(lambda x: int(x['coded_picture_number']), ffprobe_out['frames']))
    return sorted(coded_picture_numbers)[0]


def get_start_offset_time(input_video_file_path: str) -> None:
    import subprocess
    import json
    import math
    from fractions import Fraction
    from datetime import datetime, MINYEAR
    commands = [
        FFPROBE_COMMAND,
        input_video_file_path,
        '-select_streams', 'v',
        '-show_entries', 'stream=r_frame_rate',
        '-print_format', 'json',
        '-loglevel', 'level+error',
    ]
    process_result = subprocess.run(commands, capture_output=True)
    if process_result.returncode != 0:
        error_message = process_result.stderr.decode('utf-8')
        raise ScattVideoUtilsError('failed to get frame rate: \n{0:s}'.format(error_message))

    ffprobe_out = json.loads(process_result.stdout.decode('utf-8'))
    frame_rate = Fraction(ffprobe_out['streams'][0]['r_frame_rate'])
    start_frame_idx = _get_start_frame_idx(input_video_file_path)
    start_offset_sec = start_frame_idx / frame_rate
    start_offset_time = datetime(
        MINYEAR,
        1,
        1,
        math.floor(start_offset_sec / (60 * 60)),
        math.floor(start_offset_sec / 60),
        math.floor(start_offset_sec % 60),
        math.floor((start_offset_sec * 1000000) % 1000000),
    )
    return start_offset_time


def is_normalization_needed(input_video_file_path: str) -> bool:
    try:
        return (_get_start_frame_idx(input_video_file_path) != 0)
    except:
        return False


def generate_normalized_video_file(input_video_file_path: str, output_video_file_path: str) -> None:
    import subprocess
    import json

    commands = [
        FFPROBE_COMMAND,
        input_video_file_path,
        '-select_streams', 'v',
        '-show_entries', 'stream=codec_name,codec_type',
        '-print_format', 'json',
        '-loglevel', 'level+error',
    ]
    process_result = subprocess.run(commands, capture_output=True)
    if process_result.returncode != 0:
        error_message = process_result.stderr.decode('utf-8')
        raise ScattVideoUtilsError('failed to get video codec: \n{0:s}'.format(error_message))

    ffprobe_out = json.loads(process_result.stdout.decode('utf-8'))
    first_input_video_stream_info = ffprobe_out['streams'][0]
    if first_input_video_stream_info['codec_type'] == 'video':
        video_codec_name = first_input_video_stream_info['codec_name']
        if video_codec_name == 'h264':
            _generate_normalized_video_file_h264(input_video_file_path, output_video_file_path)
        else:
            raise ScattVideoUtilsError('{0:s} is unsupported video codec for normalizing video file.'.format(video_codec_name))


def get_supported_video_file_extensions():
    import subprocess
    import shutil
    from io import StringIO
    ffmpeg_path = shutil.which(FFMPEG_COMMAND)
    with open(ffmpeg_path, 'rb') as file:
        import hashlib
        ffmpeg_md5 = hashlib.md5(file.read()).digest()
    
    update_needed = False
    if not path.exists(FFMPEG_MD5_PATH) or (not path.exists(SUPPORTED_FILE_EXTENSIONS_LIST_PATH)):
        update_needed = True
    else:
        with open(FFMPEG_MD5_PATH, 'rb') as file:
            existing_ffmpeg_md5 = file.read()
            if ffmpeg_md5 != existing_ffmpeg_md5:
                update_needed = True

    if not update_needed:
        with open(SUPPORTED_FILE_EXTENSIONS_LIST_PATH) as file:
            return file.read().splitlines()

    else:
        with open(FFMPEG_MD5_PATH, 'wb') as file:
            import hashlib
            file.write(ffmpeg_md5)

        commands = [
            FFMPEG_COMMAND,
            '-demuxers',
            '-loglevel', 'level+error',
        ]
        process_result = subprocess.run(commands, capture_output=True)
        if process_result.returncode != 0:
            error_message = process_result.stderr.decode('utf-8')
            raise ScattVideoUtilsError('failed to get file extensions: \n{0:s}'.format(error_message))

        format_string = process_result.stdout.decode('utf-8')
        file_format_list_starts_at = format_string.index('File formats:')
        file_format_list_header_ends_at = format_string.index(' --', file_format_list_starts_at)
        file_format_list_body_starts_at = LINE_FEED_REGEX.search(format_string, file_format_list_header_ends_at).end()
        file_format_list_body_string_io = StringIO(format_string[file_format_list_body_starts_at:-1])
        supported_video_file_extensions = list()
        line = file_format_list_body_string_io.readline()
        while len(line) > 0:
            demuxer_name = line[4:-1].split(' ', 1)[0]
            commands = [
                FFMPEG_COMMAND,
                '-h', 'demuxer={0:s}'.format(demuxer_name),
                '-loglevel', 'level+error',
            ]
            process_result = subprocess.run(commands, capture_output=True)
            if process_result.returncode != 0:
                error_message = process_result.stderr.decode('utf-8')
                raise ScattVideoUtilsError('failed to get video file extension: \n{0:s}'.format(error_message))

            demuxer_help_string = process_result.stdout.decode('utf-8')
            file_extension_names_match_result = DEMUXER_FILE_EXTENSION_NAMES_REGEX.search(demuxer_help_string)
            if file_extension_names_match_result is not None:
                file_extension_names = file_extension_names_match_result.group(1)
                for file_extension_name in file_extension_names.split(','):
                    if file_extension_name not in supported_video_file_extensions:
                        supported_video_file_extensions.append(file_extension_name)
            line = file_format_list_body_string_io.readline()

        with open(SUPPORTED_FILE_EXTENSIONS_LIST_PATH, 'wt') as file:
            for supported_video_file_extension in sorted(supported_video_file_extensions):
                file.write('{0:s}\n'.format(supported_video_file_extension))

        return supported_video_file_extensions


def test_operating_condition() -> None:
    import shutil
    if shutil.which(FFMPEG_COMMAND) is None:
        raise ScattVideoUtilsError('{0:s} is not found. make sure that PATH is set correctly.'.format(FFMPEG_COMMAND))
    if shutil.which(FFPROBE_COMMAND) is None:
        raise ScattVideoUtilsError('{0:s} is not found. make sure that PATH is set correctly.'.format(FFPROBE_COMMAND))


def generate_waveform_digest_file_from_video_file(input_video_file_path: str) -> bytes:
    import subprocess

    supported_video_file_extensions = get_supported_video_file_extensions()
    split_result = input_video_file_path.lower().rsplit('.', 1)
    if len(split_result) == 0:
        raise ScattVideoUtilsError('{0:s} has no file extension.'.format(input_video_file_path))

    input_video_file_base_path, input_video_file_extension = split_result
    if input_video_file_extension not in supported_video_file_extensions:
        raise ScattVideoUtilsError('{0:s} is not supported.'.format(input_video_file_extension))

    output_wav_file_path = input_video_file_base_path + '.wav'
    commands = [
        FFMPEG_COMMAND,
        '-i', input_video_file_path,
        '-loglevel', 'level+error',
        output_wav_file_path,
        '-y',
    ]
    process_result = subprocess.run(commands, capture_output=True)
    if process_result.returncode != 0:
        error_message = process_result.stderr.decode('utf-8')
        raise ScattVideoUtilsError('failed to generate waveform digest: \n{0:s}'.format(error_message))

    return generate_waveform_digest_file(output_wav_file_path)


def generate_waveform_digest_file(input_wav_file_path: str) -> bytes:
    import subprocess
    waveform_digest_generator_path = get_waveform_digest_generator_path()
    commands = [
        waveform_digest_generator_path,
        input_wav_file_path,
    ]
    process_result = subprocess.run(commands, capture_output=True)
    if process_result.returncode != 0:
        error_message = process_result.stderr.decode('utf-8')
        raise ScattVideoUtilsError('failed to generate waveform digest: \n{0:s}'.format(error_message))

    return process_result.stdout
    
