import asyncio
from typing import Optional, Tuple, Callable

from tutti_client import TuttiClient
from ducts_client import Duct
from .market_controller import TuttiMarketController

class LangXTuttiClient:
    def __init__(self):
        super()
        self.tutti = TuttiClient()
        self.market = TuttiMarketController(Duct())

    async def open(self, works_host: Optional[str] = None, market_host: Optional[str] = None) -> None:
        if works_host: await self.open_works(works_host)
        if market_host: await self.open_market(market_host)

    async def open_market(self, host: str) -> None:
        if host[-1] == '/': host = host[:-1]
        await self.market.open(host + '/ducts/wsd')

    async def open_works(self, host: str) -> None:
        if host[-1] == '/': host = host[:-1]
        await self.tutti.open(host + '/ducts/wsd')

    def close(self) -> None:
        self.close_market()
        self.close_works()

    def close_market(self) -> None:
        self.market.close()

    def close_works(self) -> None:
        self.tutti.close()

    async def sign_in(self, works_params: Optional[dict], market_params: Optional[dict]) -> None:
        await self.sign_in_works(**works_params)
        await self.sign_in_market(**market_params)

    async def sign_in_market(self, user_id: str, password: str, access_token_lifetime: int = 60*60*24*7*1000) -> None:
        await self.market.sign_in(user_id, password, access_token_lifetime)

    async def sign_in_works(self, user_name: str = None, password_hash: str = None, access_token: str = None, called = True, **kwargs) -> None:
        await self.tutti.resource.sign_in(user_name, password_hash, access_token, called, **kwargs)

    async def sign_out(self) -> None:
        await self.sign_out_market()
        await self.sign_out_works()

    async def sign_out_market(self) -> None:
        await self.market.sign_out()

    async def sign_out_works(self) -> None:
        await self.tutti.resource.sign_out()


    async def publish_scatt_tasks_to_market(self, automation_parameter_set_id: str, student_id: str, video_id: str) -> Tuple[str, str]:
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
            'id': f'{student_id}-{video_id}-{time}',
            'props': {
                'student_id': student_id,
                'video_id': video_id,
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
            name = f'{student_id}-{video_id}-{time}',
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
                    description = f'Student ID: {student_id} / Video ID: {video_id}',
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

    async def watch_responses_for_scatt_tasks(self, automation_parameter_set_id: str, handler: Callable[[dict], None]) -> None:
        async def _handler(rid, eid, data):
            await handler(data)

        eid = self.tutti._duct.EVENT['RESPONSE_WATCH_FOR_AUTOMATION_PARAMETER_SET']
        self.tutti._duct.set_event_handler(eid, _handler)
        await self.tutti._duct.send(
            self.tutti._duct.next_rid(),
            eid,
            {
                'automation_parameter_set_id': automation_parameter_set_id,
                'access_token': self.tutti.account_info['access_token']
            }
        )
