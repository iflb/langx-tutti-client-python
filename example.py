import sys
import asyncio

from langx_tutti_client.main import LangXTuttiClient

#AUTOMATION_PARAMETER_SET_ID = '28e806fa133d4b3979690c198dbfbc824d15ce52'

async def on_scatt_response(data):
    print('Data received! (watch_id: {})'.format(data['last_watch_id']))
    timeline_data_set = data['data']['answers']['scattDataRawObj']['timeline_data_set']
    #print({
    #    index: {
    #        'name': val['name'],
    #        'segment_data_type': val['segment_data_type'],
    #        'segment0': val['segments']['0']
    #    } for index, val in timeline_data_set.items()
    #})

async def on_error(msg):
    print('on_error', msg)

async def main():
    mode = sys.argv[1]

    client = LangXTuttiClient()

    try:
        await client.open(
                scatt_host='https://teai.scatt.io',
                works_host='https://teai.tutti.works',
                market_host='https://teai202206.tutti.market'
            )
        await client.sign_in(
                works_params={ 'user_name': 'requester', 'password': 'requester' },
                market_params={ 'user_id': 'requester', 'password': 'requester' }
            )

        if mode == 'publish':
            automation_parameter_set_id = sys.argv[2]
            if len(sys.argv) < 4:
                print('Usage:  python example.py publish <automation_parameter_set_id> <resource_id> <video_file_path> <csv_file_path>')
                print('')
            else:
                resource_id = sys.argv[3]
                try:
                    video_file_path = sys.argv[5]
                except:
                    video_file_path = None
                try:
                    csv_file_path = sys.argv[6]
                except:
                    csv_file_path = None

                ngid, jid = await client.publish_scatt_tasks_to_market(
                        automation_parameter_set_id,
                        resource_id = resource_id,
                        video_file_path = video_file_path,
                        csv_file_path = csv_file_path,
                        overwrite = False,
                    )

        elif mode == 'watch_response':
            if len(sys.argv) < 3:
                print('Usage:  python example.py watch_response <automation_parameter_set_id> <last_watch_id>')
                print('')
            else:
                automation_parameter_set_id = sys.argv[2]
                last_watch_id = sys.argv[3] if len(sys.argv) == 4 else '+'
                print('Started watching response...')
                await client.watch_responses_for_scatt_tasks(
                        automation_parameter_set_id,
                        handler = on_scatt_response,
                        last_watch_id = last_watch_id
                    )

        elif mode == 'test_connection':
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
