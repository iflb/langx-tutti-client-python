from .scatt_modules import scatt_data_utils, scatt_video_utils, scatt_file_storage
from datetime import datetime, timezone
import json

DEFAULT_AUTHOR_ID = 'langx_tutti_client'
DEFAULT_AUTHOR_NAME = 'LangX Tutti Client'

class ScattController:
    '''Scattを利用する際に必要となるデータの生成、変換、リソースサーバーへのアップロードなどの処理を行うクラスです。

    Scattリソースサーバーへの接続を必要としない処理は、ScattControllerインスタンスを必要としないため静的関数として実装されています。
    '''
    def __init__(self, duct):
        self._duct = duct

    async def open(self, wsd_url: str):
        '''Scattリソースサーバーへの接続を行います。

        Args:
            wsd_url: ScattリソースサーバーのWSDのURL
        '''
        await self._duct.open(wsd_url)

    @staticmethod
    def generate_empty_data() -> str:
        '''空のScattデータをオンメモリで作成します。
        '''
        scatt_data = scatt_data_utils.generate_empty_data()
        return json.dumps(scatt_data)

    @staticmethod
    def convert_to_data_for_cefr_scoring_from_elan_tsv(
        tsv_string: str,
        author_id: str = DEFAULT_AUTHOR_ID,
        author_name: str = DEFAULT_AUTHOR_NAME,
    ) -> str:
        '''予めELANを用いて作成したアノテーションデータをTSV形式でエクスポートしたデータをもとに、
        CEFRスコアリングタスク向けのScattデータをオンメモリで作成します。

        Args:
            tsv_string: TSV形式でエクスポートされたELANのアノテーションデータの文字列
            author_id: Scattデータに埋め込まれる、データ作成者のID(Scattが識別子として利用します)
            author_name: Scattデータに埋め込まれる、データ作成者の名前
        '''
        scatt_data = scatt_data_utils.convert_to_data_for_cefr_scoring_from_elan_tsv(
            tsv_string,
            author_id,
            author_name,
            datetime.now(timezone.utc)
        )
        return json.dumps(scatt_data)

    @staticmethod
    def convert_to_data_for_cefr_scoring_from_csv(
        csv_string: str,
        author_id: str = DEFAULT_AUTHOR_ID,
        author_name: str = DEFAULT_AUTHOR_NAME,
    ) -> str:
        '''予め作成したCSV形式のアノテーションデータをもとに、
        CEFRスコアリングタスク向けのScattデータをオンメモリで作成します。

        Args:
            csv_string: TSV形式でエクスポートされたELANのアノテーションデータの文字列
            author_id: Scattデータに埋め込まれる、データ作成者のID(Scattが識別子として利用します)
            author_name: Scattデータに埋め込まれる、データ作成者の名前
        '''
        scatt_data = scatt_data_utils.convert_to_data_for_cefr_scoring_from_csv(
            csv_string,
            author_id,
            author_name,
            datetime.now(timezone.utc)
        )
        return json.dumps(scatt_data)

    @staticmethod
    def get_start_offset_time(input_video_path: str) -> datetime:
        '''入力された動画ファイルの開始フレームに設定された時刻のオフセットを ``datetime`` 型の値として取得します。

        この値が0以外の場合、Scattに読み込まれた動画の音声と映像の再生開始時刻がずれる可能性があります(ブラウザ依存)。
        なお、この関数は内部的に ``ffprobe`` コマンドを実行するため、コマンドへのパスを設定する必要があります。

        Args:
            input_video_path: 入力動画ファイルのパス
        '''
        return scatt_video_utils.get_start_offset_time(input_video_path)

    @staticmethod
    def is_normalization_needed(input_video_path: str) -> bool:
        '''入力された動画ファイルをScattに読み込んだ場合、動画の音声と映像の再生時刻がずれる可能性があるかどうかを取得します。

        取得した値がFalseであっても、本ライブラリが想定していない特殊なデータ形式の場合ずれる可能性があることに留意してください。
        なお、この関数は内部的に ``ffprobe`` コマンドを実行するため、コマンドへのパスを設定する必要があります。

        Args:
            input_video_path: 入力動画ファイルのパス
        '''
        return scatt_video_utils.is_normalization_needed(input_video_path)

    @staticmethod
    def generate_normalized_video_file(
        input_video_path: str,
        output_video_path: str,
    ) -> None:
        '''入力された動画ファイルの開始フレームの時刻オフセットを0に補正して、新しい動画ファイルとして出力します。

        なお、この関数は内部的に ``ffmpeg`` および ``ffprobe`` コマンドを実行するため、コマンドへのパスを設定する必要があります。
        また、現時点では入力された動画ファイルの動画ストリームのコーデックはH264のみをサポートしています。

        Args:
            input_video_path: 入力動画ファイルのパス
            output_video_path: 出力動画ファイルのパス
        '''
        scatt_video_utils.generate_normalized_video_file(
            input_video_path,
            output_video_path,
        )

    @staticmethod
    def get_supported_video_file_extensions() -> list:
        '''実行環境にインストールされた ``ffmpeg`` のヘルプから ``ffmpeg`` で読み込み可能なファイルの拡張子を取得し、文字列の配列として返します。

        ``ffmpeg`` コマンドへのパスを設定する必要があります。
        '''
        scatt_video_utils.get_supported_video_file_extensions()

    @staticmethod
    def test_operating_condition() -> None:
        '''本モジュールの動作要件を満たしているか検査します。

        満たしていない場合、例外が発生します。
        '''
        scatt_video_utils.test_operating_condition()

    @staticmethod
    def generate_waveform_digest_file(input_wav_file_path: str) -> bytes:
        '''入力された音声ファイルからScattタイムラインの背景に表示する波形のダイジェストデータをオンメモリで生成します。

        なお、この関数は内部的に ``ffmpeg`` コマンドを実行するため、コマンドへのパスを設定する必要があります。
        また、実行環境向けにビルドされた波形ダイジェストデータ生成ツールが、本ライブラリに同梱されていない場合、例外が発生します。
        新規にビルドが必要なため、お手数ですが開発者までご連絡ください。

        Args:
            input_wav_file_path: 入力音声ファイルのパス
        '''
        return scatt_video_utils.generate_waveform_digest_file(input_wav_file_path)

    @staticmethod
    def generate_waveform_digest_file_from_video_file(input_video_file_path: str) -> bytes:
        '''入力されたビデオファイルから音声ストリームを抽出し、
        Scattタイムラインの背景に表示する波形のダイジェストデータをオンメモリで生成します。

        なお、この関数は内部的に ``ffmpeg`` コマンドを実行するため、コマンドへのパスを設定する必要があります。
        また、実行環境向けにビルドされた波形ダイジェストデータ生成ツールが、本ライブラリに同梱されていない場合、例外が発生します。
        新規にビルドが必要なため、お手数ですが開発者までご連絡ください。

        Args:
            input_video_path: 入力動画ファイルのパス
        '''
        return scatt_video_utils.generate_waveform_digest_file_from_video_file(input_video_file_path)

    @staticmethod
    def normalize_video_offset(video_file_path: str):
        if ScattController.is_normalization_needed(video_file_path):
            offset_time = ScattController.get_start_offset_time(video_file_path)
            print('video normalization is needed. (offset: {0} sec)'.format(offset_time.second))

            print('>>> normalizing video file...')
            video_file_base_path, video_file_extension = video_file_path.rsplit('.', 1)
            normalized_video_file_path = video_file_base_path + '.normalized.' + video_file_extension
            ScattController.generate_normalized_video_file(video_file_path, normalized_video_file_path)
            print('<<< normalizing video file completed.')
            return normalized_video_file_path
        else:
            print('skipping video normalization')
            return video_file_path

    async def upload_scatt_resources_to_server(
        self,
        resource_id: str,
        video_file_path: str,
        scatt_data: str,
        waveform_digest_data: bytes,
        overwrite: bool,
    ) -> None:
        '''Scattリソース(動画ファイル、Scattデータ、音声波形ダイジェストデータ)をサーバーにアップロードします。

        Args:
            resource_id: Scatt リソース ID
            video_file_path: 動画ファイルのパス
            scatt_data: Scattデータ(JSON文字列)
            waveform_digest_data: 音声波形ダイジェストデータ
            overwrite: 同じIDのリソースが存在する場合に上書きするかどうか
        '''
        from os import path
        with open(video_file_path, 'rb') as file:
            video_data = file.read()
            await scatt_file_storage.upload_resources(
                self._duct,
                resource_id,
                video_data,
                scatt_data,
                waveform_digest_data,
                overwrite,
            )

    async def prepare_and_upload_files(
        self,
        resource_id: str,
        video_file_path: str,
        elan_tsv_file_path: str = None,
        overwrite: bool = False,
    ) -> None:
        '''入力された動画ファイルから音声波形ダイジェストデータを、
        ELANを用いて作成されたアノテーションデータをTSV形式でエクスポートしたファイルからScattデータを生成し、
        それらをScattリソースサーバーにアップロードします。

        このクラスに実装された関数を組み合わせてバッチ的に複合的な処理を行う関数であるため、
        用途に合わない場合はこの関数を利用せず、個別の関数をご利用ください。
        なお、これらの複合的な処理のうち、いくつかの処理は内部的に ``ffmpeg`` コマンドを実行するため、コマンドへのパスを設定する必要があります。
        また、入力された動画ファイルをScattに読み込むと動画の音声と映像の再生時刻がずれる可能性がある場合、
        開始フレームの時刻オフセットを0に補正した新しい動画ファイルの生成を試みますが、
        現時点では、入力された動画ファイルの動画ストリームのコーデックがH264でない場合、この処理は失敗します。
        そのため、動画の音声と映像の再生時刻がずれる可能性のあるH264コーデックの動画ファイルを入力しないようにしてください。

        Args:
            resource_id: Scatt リソース ID
            video_file_path: 動画ファイルのパス
            elan_tsv_file_path: ELANを用いて作成されたアノテーションデータをTSV形式でエクスポートしたファイルのパス
            overwrite: 同じIDのリソースが存在する場合に上書きするかどうか
        '''
        video_file_path = ScattController.normalize_video_offset(video_file_path)

        print('>>> generating waveform digest...')
        waveform_digest_data = ScattController.generate_waveform_digest_file_from_video_file(video_file_path)
        print('<<< generating waveform digest completed.')

        print('>>> generating Scatt data...')
        if elan_tsv_file_path is None:
            scatt_data = ScattController.generate_empty_data()
        else:
            with open(elan_tsv_file_path, 'rt') as file:
                scatt_data = ScattController.convert_to_data_for_cefr_scoring_from_elan_tsv(file.read())
        print('<<< generating Scatt data completed.')

        await self.upload_scatt_resources_to_server(
            resource_id,
            video_file_path,
            scatt_data,
            waveform_digest_data,
            overwrite,
        )

    async def prepare_and_upload_files_with_csv(
        self,
        resource_id: str,
        video_file_path: str,
        csv_file_path: str = None,
        overwrite: bool = False,
    ) -> None:
        '''入力された動画ファイルから音声波形ダイジェストデータを、
        CSV形式のアノテーションデータファイルからScattデータを生成し、
        それらをScattリソースサーバーにアップロードします。

        このクラスに実装された関数を組み合わせてバッチ的に複合的な処理を行う関数であるため、
        用途に合わない場合はこの関数を利用せず、個別の関数をご利用ください。
        なお、これらの複合的な処理のうち、いくつかの処理は内部的に ``ffmpeg`` コマンドを実行するため、コマンドへのパスを設定する必要があります。
        また、入力された動画ファイルをScattに読み込むと動画の音声と映像の再生時刻がずれる可能性がある場合、
        開始フレームの時刻オフセットを0に補正した新しい動画ファイルの生成を試みますが、
        現時点では、入力された動画ファイルの動画ストリームのコーデックがH264でない場合、この処理は失敗します。
        そのため、動画の音声と映像の再生時刻がずれる可能性のあるH264コーデックの動画ファイルを入力しないようにしてください。

        Args:
            resource_id: Scatt リソース ID
            video_file_path: 動画ファイルのパス
            csv_file_path: CSV形式のアノテーションデータファイルのパス
            overwrite: 同じIDのリソースが存在する場合に上書きするかどうか
        '''
        video_file_path = ScattController.normalize_video_offset(video_file_path)

        print('>>> generating waveform digest...')
        waveform_digest_data = ScattController.generate_waveform_digest_file_from_video_file(video_file_path)
        print('<<< generating waveform digest completed.')

        print('>>> generating Scatt data...')
        if csv_file_path is None:
            scatt_data = ScattController.generate_empty_data()
        else:
            with open(csv_file_path, 'rt') as file:
                scatt_data = ScattController.convert_to_data_for_cefr_scoring_from_csv(file.read())
        print('<<< generating Scatt data completed.')

        await self.upload_scatt_resources_to_server(
            resource_id,
            video_file_path,
            scatt_data,
            waveform_digest_data,
            overwrite,
        )

    async def get_uploaded_resource_info(self) -> dict:
        '''アップロード済みのScattリソース情報を取得します。
        '''
        return await scatt_file_storage.get_uploaded_resource_info(self._duct)

    @staticmethod
    def show_resource_tree(resource_info: dict) -> None:
        '''Scattリソース情報からディレクトリツリーを構築し、標準出力します。
        '''
        scatt_file_storage.show_resource_tree(resource_info)
