from enum import Enum
from datetime import datetime


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


    @classmethod
    def _generate_default_cefr_score_timeline_segment_data(cls) -> dict:
        timeline_segment_data = dict()
        timeline_segment_data['score'] = cls.DEFAULT_CEFR_SCORE
        timeline_segment_data['aspects'] = list()
        return timeline_segment_data

    @classmethod
    def _generate_default_cefr_score_timeline_segment(cls) -> dict:
        timeline_segment = dict()
        timeline_segment['begin'] = None
        timeline_segment['end'] = None
        timeline_segment['data'] = cls._generate_default_cefr_score_timeline_segment_data()
        timeline_segment['locked'] = False
        return timeline_segment

    @classmethod
    def _generate_default_cefr_score_timeline_data(cls, timeline_data_idx: int, qualitative_feature_name: str, author_id: str, author_name: str, created_at: datetime) -> dict:
        timeline_data = dict()
        timeline_data['name'] = qualitative_feature_name
        timeline_data['segment_data_type'] = 'CefrScoreSegmentData'
        timeline_segment_id_number = 0
        timeline_segment_id = str(timeline_segment_id_number)
        timeline_data['segments'] = dict()
        timeline_data['segments'][timeline_segment_id] = cls._generate_default_cefr_score_timeline_segment()
        timeline_data['created_at'] = created_at.isoformat()
        timeline_data['modified_at'] = created_at.isoformat()
        timeline_data['author_id'] = author_id
        timeline_data['author_name'] = author_name
        timeline_data['timeline_data_idx'] = timeline_data_idx
        timeline_data['hidden'] = True
        timeline_data['readonly'] = False
        timeline_data['locked'] = False
        return timeline_data

    @classmethod
    def generate_default_data_ver_0_5(cls, author_id: str, author_name: str, created_at: datetime) -> dict:
        scatt_data = dict()
        scatt_data['data_version'] = '0.5'
        scatt_data['timeline_data_set'] = dict()
        timeline_data_id_number = 0
        for qualitative_feature_name in cls.CefrQualitativeFeatureNames.get_all_values():
            timeline_data_idx = timeline_data_id_number
            timeline_data_id = str(timeline_data_id_number)
            scatt_data['timeline_data_set'][timeline_data_id] = cls._generate_default_cefr_score_timeline_data(
                timeline_data_idx,
                qualitative_feature_name,
                author_id,
                author_name,
                created_at,
            )
            timeline_data_id_number = timeline_data_id_number + 1
        scatt_data['references'] = list()
        return scatt_data

    @classmethod
    def _generate_label_timeline_segment_data(cls, label: str) -> dict:
        timeline_segment_data = dict()
        timeline_segment_data['label'] = label
        return timeline_segment_data

    @classmethod
    def _generate_label_timeline_segment(cls, begin_time_sec: float, end_time_sec: float, label: str) -> dict:
        import math
        timeline_segment = dict()
        timeline_segment['begin'] = math.floor(begin_time_sec * 1000)
        timeline_segment['end'] = math.floor(end_time_sec * 1000)
        timeline_segment['locked'] = False
        timeline_segment['data'] = cls._generate_label_timeline_segment_data(label)
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
        scatt_data = cls.generate_default_data_ver_0_5(
            author_id,
            author_name,
            created_at,
        )
        new_timeline_data_id_number = int(list(scatt_data['timeline_data_set'].keys())[-1]) + 1
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


def generate_default_data_for_cefr_scoring(
    author_id: str,
    author_name: str,
    created_at: datetime,
    data_version: str = LATEST_SCATT_DATA_VERSION
) -> dict:
    if data_version == '0.5':
        return ScattDataForCefrScoring.generate_default_data_ver_0_5(
            author_id,
            author_name,
            created_at,
        )
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
