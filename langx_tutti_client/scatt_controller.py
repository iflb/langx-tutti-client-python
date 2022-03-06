from .scatt_modules import scatt_data_utils, scatt_video_utils, scatt_file_storage
from datetime import datetime, timezone
import json

DEFAULT_AUTHOR_ID = 'langx_tutti_client'
DEFAULT_AUTHOR_NAME = 'LangX Tutti Client'

class ScattController:
    def __init__(self, duct):
        self._duct = duct

    async def open(self, wsd_url: str):
        await self._duct.open(wsd_url)

    @staticmethod
    def generate_empty_data_for_cefr_scoring_from_elan_tsv(
        author_id: str = DEFAULT_AUTHOR_ID,
        author_name: str = DEFAULT_AUTHOR_NAME,
    ) -> str:
        scatt_data = scatt_data_utils.generate_default_data_for_cefr_scoring(
            author_id,
            author_name,
            datetime.now(timezone.utc)
        )
        return json.dumps(scatt_data)

    @staticmethod
    def convert_to_data_for_cefr_scoring_from_elan_tsv(
        tsv_string: str,
        author_id: str = DEFAULT_AUTHOR_ID,
        author_name: str = DEFAULT_AUTHOR_NAME,
    ) -> str:
        scatt_data = scatt_data_utils.convert_to_data_for_cefr_scoring_from_elan_tsv(
            tsv_string,
            author_id,
            author_name,
            datetime.now(timezone.utc)
        )
        return json.dumps(scatt_data)

    @staticmethod
    def get_start_offset_time(input_video_path: str) -> datetime:
        return scatt_video_utils.get_start_offset_time(input_video_path)

    @staticmethod
    def is_normalization_needed(input_video_path: str) -> bool:
        return scatt_video_utils.is_normalization_needed(input_video_path)

    @staticmethod
    def generate_normalized_video_file(
        input_video_path: str,
        output_video_path: str,
    ) -> None:
        scatt_video_utils.generate_normalized_video_file(
            input_video_path,
            output_video_path,
        )

    @staticmethod
    def get_supported_video_file_extensions() -> list:
        scatt_video_utils.get_supported_video_file_extensions()

    @staticmethod
    def test_operating_condition() -> None:
        scatt_video_utils.test_operating_condition()

    @staticmethod
    def generate_waveform_digest_file(input_wav_file_path: str) -> bytes:
        return scatt_video_utils.generate_waveform_digest_file(input_wav_file_path)

    @staticmethod
    def generate_waveform_digest_file_from_video_file(input_video_file_path: str) -> bytes:
        return scatt_video_utils.generate_waveform_digest_file_from_video_file(input_video_file_path)

    async def upload_scatt_resources_to_server(
        self,
        student_id: str,
        video_id: str,
        video_file_path: str,
        scatt_data: str,
        waveform_digest_data: bytes,
        overwrite: bool,
    ) -> None:
        from os import path
        video_file_name = path.basename(video_file_path)
        with open(video_file_path, 'rb') as file:
            video_data = file.read()
            await scatt_file_storage.upload_resources(
                self._duct,
                student_id,
                video_id,
                video_file_name,
                video_data,
                scatt_data,
                waveform_digest_data,
                overwrite,
            )

    async def get_uploaded_resource_info(self) -> dict:
        return await scatt_file_storage.get_uploaded_resource_info(self._duct)

    @staticmethod
    def show_resource_tree(resource_info: dict) -> None:
        scatt_file_storage.show_resource_tree(resource_info)
