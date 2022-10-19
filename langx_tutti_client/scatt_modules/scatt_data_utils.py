from enum import Enum
from datetime import datetime
import csv


LATEST_SCATT_DATA_VERSION = '0.5'


class ScattDataUtilsError(Exception):
    def __init__(self, message):
        self.message = message


class ScattDataForCefrScoring():
    DEFAULT_CEFR_SCORE = 5

    class CefrQualitativeFeatureNames(Enum):
        @classmethod
        def get_all_enums(cls):
            return list(cls)

        @classmethod
        def get_all_values(cls):
            return list(map(lambda x: x.value, cls.get_all_enums()))

        RANGE = 'Range'
        ACCURACY = 'Accuracy'
        FLUENCY = 'Fluency'
        PHONOLOGY = 'Phonology'
        INTERACTION = 'Interaction'
        COHERENCE = 'Coherence'


    @staticmethod
    def generate_empty_data_ver_0_5() -> dict:
        scatt_data = dict()
        scatt_data['data_version'] = '0.5'
        scatt_data['timeline_data_set'] = dict()
        scatt_data['references'] = list()
        return scatt_data

    @classmethod
    def _generate_label_timeline_segment_data(cls, label: str, topic: str = '') -> dict:
        timeline_segment_data = dict()
        timeline_segment_data['label'] = label
        timeline_segment_data['topic'] = topic
        return timeline_segment_data

    @classmethod
    def _generate_label_timeline_segment(cls, begin_time_msec: float, end_time_msec: float, label: str, topic: str = '') -> dict:
        import math
        timeline_segment = dict()
        timeline_segment['begin'] = math.floor(begin_time_msec)
        timeline_segment['end'] = math.floor(end_time_msec)
        timeline_segment['locked'] = False
        timeline_segment['data'] = cls._generate_label_timeline_segment_data(label, topic)
        return timeline_segment

    @classmethod
    def _generate_empty_label_timeline_data(cls, timeline_data_idx: int, timeline_data_name: str, author_id: str, author_name: str, created_at: datetime) -> dict:
        timeline_data = dict()
        timeline_data['name'] = timeline_data_name
        timeline_data['segment_data_type'] = 'LabelForCefrSegmentData'
        timeline_data['segments'] = dict()
        timeline_data['created_at'] = created_at.isoformat()
        timeline_data['modified_at'] = created_at.isoformat()
        timeline_data['author_id'] = author_id
        timeline_data['author_name'] = author_name
        timeline_data['timeline_data_idx'] = timeline_data_idx
        timeline_data['hidden'] = False
        timeline_data['readonly'] = False
        timeline_data['locked'] = False
        return timeline_data

    @staticmethod
    def convert_intella_time_string_to_milliseconds(intella_time_string):
        import re
        hours, minutes, seconds, milliseconds = map(int, re.split('[:.]', intella_time_string))
        return milliseconds + seconds * 1000 + minutes * 60000 + hours * 3600000

    @classmethod
    def convert_intella_transcript_v2_0_to_data_ver_0_5_for_cefr_scoring(cls, intella_transcript_string: str, author_id: str, author_name: str, created_at: datetime) -> dict:
        from io import StringIO
        scatt_data = cls.generate_empty_data_ver_0_5()
        if len(scatt_data['timeline_data_set'].keys()) > 0:
            new_timeline_data_id_number = int(list(scatt_data['timeline_data_set'].keys())[-1]) + 1
        else:
            new_timeline_data_id_number = 0
        current_target_timeline_data = None
        intella_transcript_string_io = StringIO(intella_transcript_string)
        intella_transcript_string_io.readline() # intella transcript header
        intella_transcript_string_io.readline() # data header
        line = intella_transcript_string_io.readline()
        while len(line) > 0:
            stripped_line = line.strip()
            if len(stripped_line) > 0:
                begin_time_string, end_time_string, topic, timeline_data_name, label = stripped_line.strip().split('\t', 9)
                begin_time_msec = cls.convert_intella_time_string_to_milliseconds(begin_time_string)
                end_time_msec = cls.convert_intella_time_string_to_milliseconds(end_time_string)
                target_timeline_data = None
                if (current_target_timeline_data is not None) and (current_target_timeline_data['name'] == timeline_data_name):
                    target_timeline_data = current_target_timeline_data
                else:
                    for timeline_data in scatt_data['timeline_data_set'].values():
                        if timeline_data['name'] == timeline_data_name:
                            target_timeline_data = timeline_data
                            break
                    if target_timeline_data is None:
                        target_timeline_data_idx = new_timeline_data_id_number
                        target_timeline_data = cls._generate_empty_label_timeline_data(
                            target_timeline_data_idx,
                            timeline_data_name,
                            author_id,
                            author_name,
                            created_at,
                        )
                        new_timeline_data_id = str(new_timeline_data_id_number)
                        scatt_data['timeline_data_set'][new_timeline_data_id] = target_timeline_data
                        new_timeline_data_id_number = new_timeline_data_id_number + 1
                new_timeline_segment_id_number = len(target_timeline_data['segments'].keys())
                new_timeline_segment_id = str(new_timeline_segment_id_number)
                target_timeline_data['segments'][new_timeline_segment_id] = cls._generate_label_timeline_segment(begin_time_msec, end_time_msec, label, topic)
                current_target_timeline_data = target_timeline_data
            line = intella_transcript_string_io.readline()
        return scatt_data


    @classmethod
    def convert_csv_to_data_ver_0_5_for_cefr_scoring(cls, csv_string: str, author_id: str, author_name: str, created_at: datetime) -> dict:
        from io import StringIO
        scatt_data = cls.generate_empty_data_ver_0_5()
        if len(scatt_data['timeline_data_set'].keys()) > 0:
            new_timeline_data_id_number = int(list(scatt_data['timeline_data_set'].keys())[-1]) + 1
        else:
            new_timeline_data_id_number = 0
        current_target_timeline_data = None
        csv_string_io = StringIO(csv_string)
        reader = csv.reader(csv_string_io)
        next(reader)
        for line in reader:
            timeline_data_name = line[0]
            label = line[1]
            begin_time_msec = float(line[2])
            end_time_msec = float(line[3])
            topic = line[4]
            target_timeline_data = None
            if (current_target_timeline_data is not None) and (current_target_timeline_data['name'] == timeline_data_name):
                target_timeline_data = current_target_timeline_data
            else:
                for timeline_data in scatt_data['timeline_data_set'].values():
                    if timeline_data['name'] == timeline_data_name:
                        target_timeline_data = timeline_data
                        break
                if target_timeline_data is None:
                    target_timeline_data_idx = new_timeline_data_id_number
                    target_timeline_data = cls._generate_empty_label_timeline_data(
                        target_timeline_data_idx,
                        timeline_data_name,
                        author_id,
                        author_name,
                        created_at,
                    )
                    new_timeline_data_id = str(new_timeline_data_id_number)
                    scatt_data['timeline_data_set'][new_timeline_data_id] = target_timeline_data
                    new_timeline_data_id_number = new_timeline_data_id_number + 1
            new_timeline_segment_id_number = len(target_timeline_data['segments'].keys())
            new_timeline_segment_id = str(new_timeline_segment_id_number)
            target_timeline_data['segments'][new_timeline_segment_id] = cls._generate_label_timeline_segment(begin_time_msec, end_time_msec, label, topic)
            current_target_timeline_data = target_timeline_data
        return scatt_data


def generate_empty_data(
    data_version: str = LATEST_SCATT_DATA_VERSION
) -> dict:
    if data_version == '0.5':
        return ScattDataForCefrScoring.generate_empty_data_ver_0_5()
    else:
        raise ScattDataUtilsError("data version {0:s} is not supported.")


def convert_to_data_for_cefr_scoring_from_intella_transcript(
    intella_transcript_string: str,
    author_id: str,
    author_name: str,
    created_at: datetime,
    data_version: str = LATEST_SCATT_DATA_VERSION,
) -> dict:
    import re
    from io import StringIO
    intella_transcript_string_io = StringIO(intella_transcript_string)
    intella_transcript_header = intella_transcript_string_io.readline()
    intella_transcript_header_search_result = re.search('format:\s*intella-transcript-v(?P<version_number>\S+)', intella_transcript_header)
    if intella_transcript_header_search_result is None:
        raise ScattDataUtilsError("invalid intella transcript header.")
    else:
        if data_version == '0.5':
            if intella_transcript_header_search_result['version_number'] == '2.0':
                return ScattDataForCefrScoring.convert_intella_transcript_v2_0_to_data_ver_0_5_for_cefr_scoring(
                    intella_transcript_string,
                    author_id,
                    author_name,
                    created_at,
                )
        else:
            raise ScattDataUtilsError("data version {0:s} is not supported.")


def convert_to_data_for_cefr_scoring_from_csv(
    tsv_string: str,
    author_id: str,
    author_name: str,
    created_at: datetime,
    data_version: str = LATEST_SCATT_DATA_VERSION,
) -> dict:
    if data_version == '0.5':
        return ScattDataForCefrScoring.convert_csv_to_data_ver_0_5_for_cefr_scoring(
            tsv_string,
            author_id,
            author_name,
            created_at,
        )
    else:
        raise ScattDataUtilsError("data version {0:s} is not supported.")
