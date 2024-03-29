import asyncio
from typing import Optional, Tuple, Callable

from tutti_client import TuttiClient
from ducts_client import Duct
from .scatt_controller import ScattController
from .scatt_modules.scatt_video_utils import ScattVideoUtilsError
from .market_controller import TuttiMarketController

class LangXTuttiClientConnectionError(Exception):
    def __init__(self, resource, err):
        self.resource = resource
        self.err = err

class LangXTuttiClientEnvironmentError(Exception):
    def __init__(self, err):
        self.err = err

class LangXTuttiClient:
    '''LangXクライアントの各種機能を使うための窓口（Facade）となるクラスです。

    このオブジェクトがメソッドとして直接管理する操作は以下の２つのみです。
    
    - :meth:`publish_scatt_tasks_to_market` ... アノテーション対象データをタスクにし、Tutti.marketへタスク発注する
    - :meth:`watch_responses_for_scatt_tasks` ... タスクへの回答の取得およびプッシュ通知を受ける

    これに加えて、Scatt、Tutti.works、Tutti.marketへの個別の操作が必要となる場合は、それぞれ :attr:`scatt` 、 :attr:`tutti` 、 :attr:`market` メンバーから各種メソッドにアクセス可能です。

    Attributes:
        scatt (ScattController): Scattリソースアクセス用オブジェクト
        tutti (`tutti_client.TuttiClient (外部リンク)`_): Tutti.worksリソースアクセス用オブジェクト
        market (TuttiMarketController): Tutti.marketリソースアクセス用オブジェクト

    .. _tutti_client.TuttiClient (外部リンク):
        https://iflb.github.io/tutti-client-python/references/facade.html
    '''
    def __init__(self):
        super()
        self.scatt = ScattController(Duct())
        self.tutti = TuttiClient()
        self.market = TuttiMarketController(Duct())

        try:
            ScattController.test_operating_condition()
        except ScattVideoUtilsError as e:
            raise LangXTuttiClientEnvironmentError(e) from e

    async def open(self, scatt_host: Optional[str] = None, works_host: Optional[str] = None, market_host: Optional[str] = None) -> None:
        '''Scatt、Tutti.works、Tutti.marketのサーバーへ接続します。
        
        LangXTuttiClientのメソッドを呼び出す前に必須の処理です。
        内部的には、:meth:`open_scatt`、:meth:`open_tutti`、:meth:`open_market` を一度に実行します。

        Args:
            scatt_host: Scattサーバーのホスト名
            works_host: Tutti.worksサーバーのホスト名
            market_host: Tutti.marketサーバーのホスト名
        '''
        tasks = []
        if scatt_host: tasks.append(self.open_scatt(scatt_host))
        if works_host: tasks.append(self.open_works(works_host))
        if market_host: tasks.append(self.open_market(market_host))

        await asyncio.gather(*tasks)

    async def open_scatt(self, host: str) -> None:
        '''Scattサーバーへ接続します。
        
        LangXTuttiClientのメソッドを呼び出す前に必須の処理です。

        Args:
            host: Scattサーバーのホスト名
        '''
        err = []
        async def on_error(event):
            err.append(event)

        if host[-1] == '/': host = host[:-1]
        self.scatt._duct.connection_listener.onerror = on_error
        await self.scatt.open(host + '/ducts/wsd')
        await asyncio.sleep(0.1)
        if err:
            raise LangXTuttiClientConnectionError('scatt', err[0])

    async def open_works(self, host: str) -> None:
        '''Tutti.worksサーバーへ接続します。
        
        LangXTuttiClientのメソッドを呼び出す前に必須の処理です。

        Args:
            host: Tutti.worksサーバーのホスト名
        '''
        err = []
        async def on_error(event):
            err.append(event)

        if host[-1] == '/': host = host[:-1]
        self.tutti._duct.connection_listener.onerror = on_error
        await self.tutti.open(host + '/ducts/wsd')
        await asyncio.sleep(0.1)
        if err:
            raise LangXTuttiClientConnectionError('Tutti.works', err[0])

    async def open_market(self, host: str) -> None:
        '''Tutti.marketサーバーへ接続します。
        
        LangXTuttiClientのメソッドを呼び出す前に必須の処理です。

        Args:
            host: Tutti.marketサーバーのホスト名
        '''
        err = []
        async def on_error(event):
            err.append(event)

        if host[-1] == '/': host = host[:-1]
        self.market._duct.connection_listener.onerror = on_error
        await self.market.open(host + '/ducts/wsd')
        await asyncio.sleep(0.1)
        if err:
            raise LangXTuttiClientConnectionError('Tutti.market', err[0])

    def close(self) -> None:
        '''Scatt、Tutti.works、Tutti.marketのサーバー接続を切断します。
        '''
        self.close_scatt()
        self.close_works()
        self.close_market()

    def close_scatt(self) -> None:
        '''Scattサーバーへの接続を切断します。
        '''
        self.scatt.close()

    def close_works(self) -> None:
        '''Tutti.worksサーバーへの接続を切断します。
        '''
        self.tutti.close()

    def close_market(self) -> None:
        '''Tutti.marketサーバーへの接続を切断します。
        '''
        self.market.close()

    async def sign_in(self, works_params: dict, market_params: dict) -> None:
        '''Tutti.worksおよびTutti.marketへサインインします。

        内部的には、:meth:`sign_in_works` および :meth:`sign_in_market` を順に実行します。

        Args:
            works_params: :meth:`sign_in_works` へ渡すキーワード引数をメンバーとして持つdict。
            market_params: :meth:`sign_in_market` へ渡すキーワード引数をメンバーとして持つdict。
        '''
        await self.sign_in_works(**works_params)
        await self.sign_in_market(**market_params)

    async def sign_in_market(self, user_id: str, password: str, access_token_lifetime: int = 60*60*24*7*1000) -> None:
        '''Tutti.marketへサインインします。

        Args:
            user_id: ユーザーID
            password: パスワード
            access_token_lifetime: アクセストークンの有効期限（ミリ秒）
        '''
        await self.market.sign_in(user_id, password, access_token_lifetime)

    async def sign_in_works(self, user_name: Optional[str] = None, password_hash: Optional[str] = None, access_token: Optional[str] = None, **kwargs) -> None:
        '''Tutti.worksへサインインします。

        引数の渡し方は複数通り存在します。

        1. ``access_token`` のみを指定する
        2. ``user_name`` と ``password_hash`` を指定する
        3. ``user_name`` と ``password`` （ ``kwargs`` として受け取られる）を指定する

        ※ 1→2→3の順に優先されます。
        :meth:`sign_in` においては3が適用されています。

        Args:
            user_name: ユーザー名
            password_hash: パスワードのハッシュ値（MD5）
            access_token: アクセストークン
            kwargs: ``password`` が指定可能です。
        '''
        await self.tutti.resource.sign_in(user_name, password_hash, access_token, **kwargs)

    async def sign_out(self) -> None:
        '''Tutti.market、Tutti.worksからサインアウトします。

        内部的には :meth:`sign_out_market` 、 :meth:`sign_out_works` が順に呼ばれます。
        '''
        await self.sign_out_market()
        await self.sign_out_works()

    async def sign_out_market(self) -> None:
        '''Tutti.marketからサインアウトします。
        '''
        await self.market.sign_out()

    async def sign_out_works(self) -> None:
        '''Tutti.worksからサインアウトします。
        '''
        await self.tutti.resource.sign_out()


    async def publish_scatt_tasks_to_market(
        self,
        automation_parameter_set_id: str,
        resource_id: str,
        video_file_path: Optional[str] = None,
        csv_file_path: Optional[str] = None,
        overwrite: bool = False,
    ) -> Tuple[str, str]:
        '''アノテーション対象データをScattサーバーへ登録し、タスクをTutti.marketのJobとして発行します。

        Args:
            automation_parameter_set_id: Tutti.worksにおいて発行されるAutomation Parameter Set ID (Automation ID)。
            resource_id: ScattリソースID(Scattに読み込まれるリソースを特定するためのID)です。
            video_file_path: ビデオファイル（mp4）のローカルパス。
            csv_file_path: 音声認識結果や事前ラベル情報等を持ったファイル（CSV）のローカルパス。
            overwrite: ``True`` の時、 ``resource_id`` に対応するScattリソースがすでに存在していてもファイルを上書きします。

        Returns:
            tuple: 生成されたリソースのIDを返します。0番目はTutti.worksのNanotask Group ID、1番目はTutti.marketのJob IDです。

        Raises:
            ScattFileStorageError: ``resource_id`` がすでにScattサーバー上に登録済みで、``video_file_path`` が引数に指定されており、かつ ``overwrite=True`` の時。
        '''

        if video_file_path:
            await self.scatt.prepare_and_upload_files_with_csv(resource_id, video_file_path, csv_file_path, overwrite)

        data = await self.tutti._duct.call(
                self.tutti._duct.EVENT['AUTOMATION_PARAMETER_SET_GET'],
                {
                    'automation_parameter_set_id': automation_parameter_set_id,
                    'access_token': self.tutti.account_info['access_token']
                }
            )
        if data and data['content']:
            aps = data['content']
        else:
            raise Exception(f'Automation parameter set ID "{automation_parameter_set_id}" is not defined')

        data = await self.tutti._duct.call(
                self.tutti._duct.EVENT['PLATFORM_PARAMETER_SET_GET'],
                {
                    'platform_parameter_set_id': aps['platform_parameter_set_id'],
                    'access_token': self.tutti.account_info['access_token']
                }
            )
        if data:
            pps = data['content']
        else:
            raise Exception('Platform parameter set ID "{}}" is not defined'.format(aps['platform_parameter_set_id']))

        if pps['platform']!='market':
            raise Exception('Platform parameter set ID "{}" is not set for market'.format(aps['platform_parameter_set_id']))

        import time
        time = int(time.time())

        nanotask = {
            'id': f'{resource_id}-{time}',
            'props': {
                'resource_id': resource_id,
            }
        }

        ret = await self.tutti.resource.create_nanotasks(
            project_name = aps['project_name'],
            template_name = 'ScattApp',
            nanotasks = [nanotask],
            tag = '',
            priority = 100,
            num_assignable = 0,
        )
        nids = ret['nanotask_ids']
        print('Nanotask IDs:', nids)
        ngid = await self.tutti.resource.create_nanotask_group(
            name = f'{resource_id}-{time}',
            nanotask_ids = nids,
            project_name = aps['project_name'],
            template_name = 'ScattApp',
        )
        print('Nanotask Group ID:', ngid)

        job_parameter = {
            'nanotask_group_ids': [ngid],
            'automation_parameter_set_id': automation_parameter_set_id,
            'platform_parameter_set_id': aps['platform_parameter_set_id']
        }

        try:
            int_or_none = lambda x: int(x) if x is not None and x!='' else None
            res = await self.market.register_job(
                    job_class_id = pps['parameters']['job_class_id'],
                    job_parameter = job_parameter,
                    description = f'Resource ID: {resource_id}',
                    num_job_assignments_max = int_or_none(pps['parameters']['num_job_assignments_max']),
                    priority_score = int_or_none(pps['parameters']['priorityScore']),
                )

            if res['success']:
                jid = res['body']
            else:
                raise Error('Failed to create a job')

            print('Job Class ID:', pps['parameters']['job_class_id'])
            print('Job ID:', jid)

            return ngid, jid
        except:
            import traceback
            traceback.print_exc()

    async def watch_responses_for_scatt_tasks(
        self,
        automation_parameter_set_id: str,
        handler: Callable[[dict], None],
        last_watch_id: str = '+'
    ) -> None:
        '''発注したScatt UIのJobにおいて、Tutti.worksに保存された回答をプッシュ通知で受け取ります。

        また、このメソッドの実行時、 :attr:`last_watch_id` よりも後に収集された回答をサーバーから順番に返します。
        これは例えば、前回 :meth:`watch_responses_for_scatt_tasks` を実行していたプロセスが終了してから、
        次回再びプロセスを呼ぶまでの間に既に発行済みのJobにおいて新たな回答があった場合に、それらの回答を次回実行時に
        まとめて取得するという目的で有効に用いることができます。

        Args:
            automation_parameter_set_id: Tutti.worksで発行されたAutomation Parameter Set ID
            handler: 回答を受信するたびに実行される（asyncコルーチン）関数。引数を１つとり、回答情報を受け取ります。
                この引数はdict型で、 ``last_watch_id`` と ``data`` の２つのキーを持ちます。
            last_watch_id: 過去に受け取った回答のWatch IDを指定すると、指定値よりも後のタイムスタンプに相当する
                回答を全て返します。Watch IDの形式は ``<ミリ秒タイムスタンプ>-<0始まり通し番号>`` で、
                Tutti.worksの内部データベースであるRedisの `Stream ID`_ に準拠します。したがって、この値に
                ハイフン以降を除いた ``<ミリ秒タイムスタンプ>`` を渡した場合はその時間以降の全ての回答が返され、
                またデフォルト値の ``+`` は正の方向の無限大値を指すため、メソッド実行時に過去の回答は返されません。
                ``0`` を指定することで 履歴に存在する全ての回答を受け取ることが可能ですが、収集済み回答数が多く
                なっている場合は受信データが膨大になる可能性があるため、非推奨です。
        
        .. _Stream ID:
            https://redis.io/topics/streams-intro
        '''
        self.tutti.resource.on('watch_responses_for_automation_parameter_set', handler)
        await self.tutti.resource.watch_responses_for_automation_parameter_set.send(
            automation_parameter_set_id = automation_parameter_set_id,
            last_watch_id = last_watch_id,
            exclusive = True
        )
