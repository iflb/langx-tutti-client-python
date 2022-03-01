import sys
import asyncio

from langx_tutti_client.main import LangXTuttiClient

#AUTOMATION_PARAMETER_SET_ID = '28e806fa133d4b3979690c198dbfbc824d15ce52'

async def on_scatt_response(data):
    print('Data received!')
    data = data['content']['answers']['timeline_data_set']
    print({
        index: {
            'name': val['name'],
            'segment_data_type': val['segment_data_type'],
            'segment0': val['segments']['0']
        } for index, val in data.items()
    })

async def on_error(msg):
    print('on_error', msg)

async def main():
    mode = sys.argv[1]

    client = LangXTuttiClient()

    try:
        await client.open(
                scatt_host='https://teai.scatt.io',
                works_host='https://teai.tutti.work',
                market_host='https://teai.tutti.market'
            )
        await client.sign_in(
                works_params={ 'user_name': 'admin', 'password': 'admin' },
                market_params={ 'user_id': 'susumu', 'password': 'susumu' }
            )

        if mode == 'publish':
            automation_parameter_set_id = sys.argv[2]
            if len(sys.argv) != 5:
                print('Usage:  python example.py publish <automation_parameter_set_id> <student_id> <video_id>')
                print('')
            else:
                student_id = sys.argv[3]
                video_id = sys.argv[4]

                ngid, jid = await client.publish_scatt_tasks_to_market(
                        automation_parameter_set_id,
                        student_id = student_id,
                        video_id = video_id
                    )

        elif mode == 'watch_response':
            automation_parameter_set_id = sys.argv[2]
            if len(sys.argv) != 3:
                print('Usage:  python example.py watch_response <automation_parameter_set_id>')
                print('')
            else:
                print('Started watching response...')
                await client.watch_responses_for_scatt_tasks(
                        automation_parameter_set_id,
                        handler = on_scatt_response,
                    )

        elif mode == 'only_connect':
            print('open and sign_in finished')

    except Exception as e:
        print(e.resource, e.err.state, e.err.source)


if __name__=='__main__':
    if len(sys.argv)==1:
        print('Usage:  python example.py <mode> <automation_parameter_set_id> [args]')
        print('Available modes ... "publish", "watch_response"')
        print('')
    else:
        loop = asyncio.get_event_loop()
        loop.create_task(main())
        loop.run_forever()
