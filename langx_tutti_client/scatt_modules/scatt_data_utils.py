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
    def _generate_label_timeline_segment(cls, begin_time_sec: float, end_time_sec: float, label: str, topic: str = '') -> dict:
        import math
        timeline_segment = dict()
        timeline_segment['begin'] = math.floor(begin_time_sec * 1000)
        timeline_segment['end'] = math.floor(end_time_sec * 1000)
        timeline_segment['locked'] = False
        timeline_segment['data'] = cls._generate_label_timeline_segment_data(label, topic)
        return timeline_segment

    @classmethod
    def _generate_empty_label_timeline_data(cls, timeline_data_idx: int, timeline_data_name: str, author_id: str, author_name: str, created_at: datetime) -> dict:
        timeline_data = dict()
        timeline_data['name'] = timeline_data_name
        timeline_data['segment_data_type'] = 'LabelSegmentData'
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

    @classmethod
    def convert_to_data_ver_0_5_for_cefr_scoring(cls, tsv_string: str, author_id: str, author_name: str, created_at: datetime) -> dict:
        from io import StringIO
        scatt_data = cls.generate_empty_data_ver_0_5()
        if len(scatt_data['timeline_data_set'].keys()) > 0:
            new_timeline_data_id_number = int(list(scatt_data['timeline_data_set'].keys())[-1]) + 1
        else:
            new_timeline_data_id_number = 0
        current_target_timeline_data = None
        tsv_string_io = StringIO(tsv_string)
        line = tsv_string_io.readline()
        while len(line) > 0:
            stripped_line = line.strip()
            if len(stripped_line) > 0:
                split_line = stripped_line.strip().split('\t', 9)
                timeline_data_name = split_line[0]
                begin_time_sec = float(split_line[3])
                end_time_sec = float(split_line[5])
                label = split_line[8]
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
                target_timeline_data['segments'][new_timeline_segment_id] = cls._generate_label_timeline_segment(begin_time_sec, end_time_sec, label)
                current_target_timeline_data = target_timeline_data
            line = tsv_string_io.readline()
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
            print(line)
            timeline_data_name = line[0]
            label = line[1]
            begin_time_sec = float(line[2])
            end_time_sec = float(line[3])
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
            target_timeline_data['segments'][new_timeline_segment_id] = cls._generate_label_timeline_segment(begin_time_sec, end_time_sec, label, topic)
            current_target_timeline_data = target_timeline_data
        return scatt_data


def generate_empty_data(
    data_version: str = LATEST_SCATT_DATA_VERSION
) -> dict:
    if data_version == '0.5':
        return ScattDataForCefrScoring.generate_empty_data_ver_0_5()
    else:
        raise ScattDataUtilsError("data version {0:s} is not supported.")


def convert_to_data_for_cefr_scoring_from_elan_tsv(
    tsv_string: str,
    author_id: str,
    author_name: str,
    created_at: datetime,
    data_version: str = LATEST_SCATT_DATA_VERSION,
) -> dict:
    if data_version == '0.5':
        return ScattDataForCefrScoring.convert_to_data_ver_0_5_for_cefr_scoring(
            tsv_string,
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
