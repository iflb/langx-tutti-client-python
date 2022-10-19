class IntellaDataUtilsError(Exception):
    def __init__(self, message):
        self.message = message


def convert_time_string_to_milliseconds(intella_time_string):
    import re
    hours, minutes, seconds, milliseconds = map(int, re.split('[:.]', intella_time_string))
    return milliseconds + seconds * 1000 + minutes * 60000 + hours * 3600000


def parse_transcript_header(transcript_header_string):
    import re
    transcript_header_search_result = re.search('format:\s*intella-transcript-v(?P<version_number>\S+)', transcript_header_string)
    if transcript_header_search_result is None:
        raise IntellaDataUtilsError('invalid intella transcript header.')
    else:
        return {
          'version_number': transcript_header_search_result['version_number'],
        }


def parse_transcript_v2_0_line(transcript_line):
    stripped_transcript_line = transcript_line.strip()
    if len(stripped_transcript_line) == 0:
        return None
    else:
        begin_time_string, end_time_string, topic, speaker, transcript = stripped_transcript_line.split('\t')
        begin_time_msec = convert_time_string_to_milliseconds(begin_time_string)
        end_time_msec = convert_time_string_to_milliseconds(end_time_string)
        return {
            'begin_time_string': begin_time_string,
            'begin_time_msec': begin_time_msec,
            'end_time_string': end_time_string,
            'end_time_msec': end_time_msec,
            'topic': topic,
            'speaker': speaker,
            'transcript': transcript,
        }


def parse_log_header(log_header_string):
    import re
    log_header_search_result = re.search('format:\s*intella-log-v(?P<version_number>\S+)', log_header_string)
    if log_header_search_result is None:
        raise IntellaDataUtilsError('invalid intella log header.')
    else:
        return {
          'version_number': log_header_search_result['version_number'],
        }


def parse_log_v2_0_line(log_line):
    import json
    stripped_log_line = log_line.strip()
    if len(stripped_log_line) == 0:
        return None
    else:
        time_string, log_type, log_data_json_string = stripped_log_line.split('\t')
        time_msec = convert_time_string_to_milliseconds(time_string)
        return {
            'time_string': time_string,
            'time_msec': time_msec,
            'log_type': log_type,
            'log_data': json.loads(log_data_json_string),
        }