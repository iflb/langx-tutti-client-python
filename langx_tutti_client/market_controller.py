from typing import Optional
import hashlib

class TuttiMarketController:
    '''Tutti.marketに関連する操作を行うオブジェクトです。

    現状、必要最低限のメソッド群のみを定義しています。JavaScriptにおいて既に `こちら`_ に定義された
    操作のうち、このクラスに実装が必要なものは別途問い合わせてください。

    .. _こちら:
        https://github.com/iflb/tutti-market/blob/a4b6b9054183f761a1692ff9633a84e80d93ea3c/frontend/src/scripts/ducts.js#L142
    '''
    def __init__(self, duct):
        self._duct = duct

    async def open(self, wsd_url: str):
        '''Tutti.marketサーバーへ接続します。

        Args:
            wsd_url: DUCTSサーバーへ接続するエンドポイント
        '''
        await self._duct.open(wsd_url)

    def close(self):
        '''Tutti.marketサーバーへの接続を切断します。
        '''
        self._duct.close()

    async def get_job_class(
        self,
        job_class_id: str,
    ):
        '''ジョブクラスを取得します。

        Args:
            job_class_id: ジョブクラスのID
        '''
        data = await self._duct.call(self._duct.EVENT['GET_JOB_CLASS'], {
                'access_token': self.access_token,
                'job_class_id': job_class_id,
            })
        return data

    async def create_job_class(
        self,
        url: str,
        title: str,
        expired_at: int,
        time_limit: int,
        target_party_ids: Optional[list] = ['all'],
        rewards: Optional[int] = 0,
        job_class_parameter: Optional[None] = None,
        description: Optional[str] = None,
        closed_at: Optional[int] = None,
        assignability: Optional[str] = None,
        job_progress_data_type: Optional[str] = None,
        priority_score: Optional[int] = None,
        choose_job_by: Optional[str] = None,
    ):
        '''ジョブクラスを作成します。

        Args:
            url: ジョブクラスのURL
            title: ジョブクラスのタイトル
            expired_at: ジョブクラスの有効期限UTC時刻のタイムスタンプ(ミリ秒)
            time_limit: のタイムスタンプ(ミリ秒)
            target_party_ids: 割り当て対象となるワーカー・ワーカーグループのIDリスト
            rewards: ワーカーへの報酬(単位は未規定)
            job_class_parameter: ジョブから参照されるジョブクラス固有のパラメータ
            description: ジョブクラスの説明文
            closed_at: ジョブクラスの開始期限UTC時刻のタイムスタンプ(ミリ秒)
            assignability: ワーカーへの割り当て制限種別名(unlimited/once_per_job/once_per_job_class)
            job_progress_data_type: ジョブの進捗状況データ型(no_data/rational/percentage/rate/label)
            priority_score: 優先度。値が小さいほど優先度が高く、優先的にワーカーへ割り当てられます。
            choose_job_by: 最高優先度がジョブが複数ある場合にジョブを1つ選択する基準(random/first_registered_at/last_registered_at)
        '''
        data = await self._duct.call(self._duct.EVENT['CREATE_JOB_CLASS'], {
                'access_token': self.access_token,
                'url': url,
                'title': title,
                'rewards': rewards,
                'expired_at': expired_at,
                'time_limit': time_limit,
                'target_party_ids': target_party_ids,
                'job_class_parameter': job_class_parameter,
                'description': description,
                'closed_at': closed_at,
                'assignability': assignability,
                'job_progress_data_type': job_progress_data_type,
                'priority_score': priority_score,
                'choose_job_by': choose_job_by,
            })
        return data

    async def close_job_class(
        self,
        job_class_id: str,
    ):
        '''ジョブクラスをクローズします。

        Args:
            job_class_id: ジョブクラスのID
        '''
        data = await self._duct.call(self._duct.EVENT['CLOSE_JOB_CLASS'], {
                'access_token': self.access_token,
                'job_class_id': job_class_id,
            })
        return data

    async def register_job(
        self,
        job_class_id: str,
        job_parameter: Optional[dict] = None,
        description: Optional[str] = None,
        num_job_assignments_max: Optional[int] = None,
        priority_score: Optional[int] = None
    ) -> dict:
        '''ジョブを発注します。

        Args:
            job_class_id: ジョブクラスID
            job_parameter: 発注するジョブへ与えるパラメータ群
            description: ジョブの説明文（リクエスタから見えるメモ用）
            num_job_assignments_max: 収集する回答数上限
            priority_score: 優先度。値が小さいほど優先度が高く、優先的にワーカーへ割り当てられます。
        '''
        data = await self._duct.call(self._duct.EVENT['REGISTER_JOB'], {
                'access_token': self.access_token,
                'job_class_id': job_class_id,
                'job_parameter': job_parameter,
                'description': description,
                'num_job_assignments_max': num_job_assignments_max,
                'priority_score': priority_score
            })
        return data

    async def get_job(self, job_id: str) -> dict:
        '''ジョブを取得します。

        Args:
            job_id: ジョブID
        '''
        data = await self._duct.call(self._duct.EVENT['GET_JOB'], {
                'access_token': self.access_token,
                'job_id': job_id,
            })
        return data

    async def close_job(
        self,
        job_id: str,
    ):
        '''ジョブをクローズします。

        Args:
            job_id: ジョブのID
        '''
        data = await self._duct.call(self._duct.EVENT['CLOSE_JOB'], {
                'access_token': self.access_token,
                'job_id': job_id,
            })
        return data

    async def get_worker_ids(self) -> dict:
        '''全てのワーカーのIDを取得します。
        '''
        data = await self._duct.call(self._duct.EVENT['GET_USER_IDS'], {
                'access_token': self.access_token,
                'user_role_groups': [ 'workable' ],
            })
        return data

    async def get_worker_group_ids(self) -> dict:
        '''全てのワーカーグループのIDを取得します。
        '''
        data = await self._duct.call(self._duct.EVENT['GET_WORKER_GROUP_IDS'], {
                'access_token': self.access_token,
            })
        return data

    async def sign_in(
        self,
        user_id: str,
        password: Optional[str] = None,
        password_hash: Optional[str] = None,
        access_token_lifetime: Optional[int] = None,
    ):
        '''Tutti.marketにサインインします。
        '''
        if password is not None:
            password_hash = hashlib.sha512(password.encode('ascii')).digest()
        data = await self._duct.call(self._duct.EVENT['SIGN_IN'], {
            'user_id': user_id,
            'password_hash': password_hash,
            'access_token_lifetime': access_token_lifetime
        })
        self.access_token = data['body']['access_token']

    async def sign_out(self):
        '''Tutti.marketからサインアウトします。
        '''
        data = await self._duct.call(self._duct.EVENT['SIGN_OUT'], {
            'access_token': self.access_token
        })
