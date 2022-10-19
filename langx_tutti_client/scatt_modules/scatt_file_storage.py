ANNOTATION_GROUP_NAME = 'annotation'
WAV_DIGEST_GROUP_NAME = 'wav.digest'
VIDEO_GROUP_NAME = 'video'
ROOT_DIR = '/'
RESOURCES_ROOT_DIR_NAME = '/resources'


class ScattFileStorageError(Exception):
    def __init__(self, message):
        self.message = message


class ScattFileResourceInfo():
    def __init__(
        self,
        last_modified: str,
        content_length: str,
        raw_content_metadata: dict,
    ):
        from datetime import datetime, timezone
        self.last_modified = datetime.fromtimestamp(int(last_modified), timezone.utc)
        self.content_length = int(content_length)
        self.raw_content_metadata = raw_content_metadata


async def _has_file(duct, group_name, parent_content_path, child_content_path):
    from os import path 
    file_info_list = await duct.call(
        duct.EVENT['BLOBS_DIR_LIST_FILES'],
        { 'group_key': group_name, 'content_key': parent_content_path },
    )
    return path.basename(child_content_path) in file_info_list.keys()


async def _content_exists(duct, group_name, content_path):
    return await duct.call(
        duct.EVENT['BLOBS_CONTENT_EXISTS'],
        { 'group_key': group_name, 'content_key': content_path },
    )


async def _get_latest_content_metadata(duct, group_name, content_path):
    return await duct.call(
        duct.EVENT['BLOBS_CONTENT_METADATA'],
        { 'group_key': group_name, 'content_key': content_path },
    )


async def _get_latest_content_revision_id(duct, group_name, content_path):
    return (await _get_latest_content_metadata(duct, group_name, content_path))[0]


async def _create_directory(duct, group_name, content_path):
    return await duct.call(
        duct.EVENT['BLOBS_CONTENT_ADD_DIR'],
        { 'group_key': group_name, 'content_key': content_path },
    )


async def _add_file_to_directory(duct, group_name, parent_content_path, child_content_path):
    from os import path 
    if not await _content_exists(duct, group_name, parent_content_path):
        raise ScattFileStorageError('{0:s} does not exist.'.format(parent_content_path))

    if await _has_file(duct, group_name, parent_content_path, child_content_path):
        raise ScattFileStorageError('{0:s} already has {1:s}.'.format(parent_content_path, child_content_path))

    await duct.call(
        duct.EVENT['BLOBS_DIR_ADD_FILES'],
        {
            'group_key': group_name,
            'content_key': parent_content_path,
            'files': [
                {
                    'group_key': group_name,
                    'content_key': child_content_path,
                    'filename': path.basename(child_content_path),
                },
            ],
        },
    )


async def _upload_data(
    duct,
    data: bytes,
    group_name: str,
    content_path: str,
    overwrite: bool,
    last_modified_sec: float,
    **metadata
) -> None:
    from io import BytesIO
    file_exists = await _content_exists(duct, group_name, content_path)
    if file_exists and not overwrite:
        raise ScattFileStorageError('{0:s} already exists'.format(content_path))

    buffer_key = await duct.call(duct.EVENT['BLOBS_BUFFER_OPEN'], None)
    buffer_size = 1024 * 512
    bytes_io = BytesIO(data)
    buffer = bytes_io.read(buffer_size)
    while len(buffer) > 0:
        await duct.call(
            duct.EVENT['BLOBS_BUFFER_APPEND'],
            [ buffer_key, buffer ],
        )
        buffer = bytes_io.read(buffer_size)

    if file_exists and overwrite:
        await duct.call(
            duct.EVENT['BLOBS_CONTENT_UPDATE_BY_BUFFER'],
            {
                'group_key': group_name,
                'content_key': content_path,
                'buffer_key': buffer_key,
                'last_modified': last_modified_sec,
                **metadata
            },
        )
    else:
        await duct.call(
            duct.EVENT['BLOBS_CONTENT_ADD_BY_BUFFER'],
            {
                'group_key': group_name,
                'content_key': content_path,
                'buffer_key': buffer_key,
                'last_modified': last_modified_sec,
                **metadata
            },
        )


async def delete_resources(duct, resource_id: str) -> None:
    from os import path
    resource_dir = path.normpath(path.join(RESOURCES_ROOT_DIR_NAME, resource_id))
    await duct.call(
        duct.EVENT['BLOBS_DIR_RM_FILES'],
        {
            'group_key': VIDEO_GROUP_NAME,
            'content_key': RESOURCES_ROOT_DIR_NAME,
            'files': [ path.basename(resource_dir) ],
        },
    )


async def upload_resources(
    duct,
    resource_id: str,
    video_data: bytes,
    scatt_data: str,
    waveform_digest_data: bytes,
    overwrite: bool,
) -> None:
    from os import path
    from datetime import datetime, timezone
    for group_name in [ VIDEO_GROUP_NAME, WAV_DIGEST_GROUP_NAME, ANNOTATION_GROUP_NAME ]:
        group_exists = await duct.call(duct.EVENT['BLOBS_GROUP_EXISTS'], { 'group_key': group_name })
        if not group_exists:
            await duct.call(duct.EVENT['BLOBS_GROUP_ADD'], { 'group_key': group_name })
            group_exists = await duct.call(duct.EVENT['BLOBS_GROUP_EXISTS'], { 'group_key': group_name })
            if not group_exists:
                raise Exception('Failed to add group({0:s}).'.format(group_name))

    last_modified_sec = int(datetime.now(timezone.utc).timestamp())
    metadata = {}

    if waveform_digest_data is not None:
        wav_digest_file_path = path.normpath(path.join(RESOURCES_ROOT_DIR_NAME, resource_id, 'video.wav.digest'))
        if await _content_exists(duct, WAV_DIGEST_GROUP_NAME, wav_digest_file_path) and not overwrite:
            raise ScattFileStorageError('{0:s} already exists'.format(wav_digest_file_path))
        await _upload_data(
            duct,
            waveform_digest_data, 
            WAV_DIGEST_GROUP_NAME,
            wav_digest_file_path,
            overwrite,
            last_modified_sec,
        )
        wav_digest_revision_id = await _get_latest_content_revision_id(
            duct,
            WAV_DIGEST_GROUP_NAME,
            wav_digest_file_path,
        )
        metadata['wav_digest_group_key'] = WAV_DIGEST_GROUP_NAME
        metadata['wav_digest_content_key'] = wav_digest_file_path
        metadata['wav_digest_revision_id'] = wav_digest_revision_id

    if scatt_data is not None:
        scatt_data_file_path = path.normpath(path.join(RESOURCES_ROOT_DIR_NAME, resource_id, 'scatt_data.json'))
        if await _content_exists(duct, ANNOTATION_GROUP_NAME, scatt_data_file_path) and not overwrite:
            raise ScattFileStorageError('{0:s} already exists'.format(scatt_data_file_path))
        await _upload_data(
            duct,
            scatt_data.encode('utf-8'), 
            ANNOTATION_GROUP_NAME,
            scatt_data_file_path,
            overwrite,
            last_modified_sec,
        )
        scatt_data_revision_id = await _get_latest_content_revision_id(
            duct,
            ANNOTATION_GROUP_NAME,
            scatt_data_file_path,
        )
        metadata = {
            'scatt_data_group_key': ANNOTATION_GROUP_NAME,
            'scatt_data_content_key': scatt_data_file_path,
            'scatt_data_revision_id': scatt_data_revision_id,
        }

    if not await _content_exists(duct, VIDEO_GROUP_NAME, ROOT_DIR):
        await _create_directory(duct, VIDEO_GROUP_NAME, ROOT_DIR)

    if not await _content_exists(duct, VIDEO_GROUP_NAME, RESOURCES_ROOT_DIR_NAME):
        await _create_directory(duct, VIDEO_GROUP_NAME, RESOURCES_ROOT_DIR_NAME)

    if not await _has_file(duct, VIDEO_GROUP_NAME, ROOT_DIR, RESOURCES_ROOT_DIR_NAME):
        await _add_file_to_directory(duct, VIDEO_GROUP_NAME, ROOT_DIR, RESOURCES_ROOT_DIR_NAME)

    resource_dir = path.normpath(path.join(RESOURCES_ROOT_DIR_NAME, resource_id))
    if not await _content_exists(duct, VIDEO_GROUP_NAME, resource_dir):
        await _create_directory(duct, VIDEO_GROUP_NAME, resource_dir)

    if not await _has_file(duct, VIDEO_GROUP_NAME, RESOURCES_ROOT_DIR_NAME, resource_dir):
        await _add_file_to_directory(duct, VIDEO_GROUP_NAME, RESOURCES_ROOT_DIR_NAME, resource_dir)

    video_file_path = path.normpath(path.join(resource_dir, 'video'))
    if await _content_exists(duct, VIDEO_GROUP_NAME, video_file_path) and not overwrite:
        raise ScattFileStorageError('{0:s} already exists'.format(video_file_path))
    await _upload_data(
        duct,
        video_data, 
        VIDEO_GROUP_NAME,
        video_file_path,
        overwrite,
        last_modified_sec,
        **metadata,
    )

    if not await _has_file(duct, VIDEO_GROUP_NAME, resource_dir, video_file_path):
        await _add_file_to_directory(duct, VIDEO_GROUP_NAME, resource_dir, video_file_path)

async def _get_children_resource_info(duct, group_key, parent_dir_content_name):
    resource_info = dict()
    file_info_list = await duct.call(duct.EVENT['BLOBS_DIR_LIST_FILES'], {
        'group_key': group_key,
        'content_key': parent_dir_content_name,
    })
    if '.' in file_info_list.keys():
        del file_info_list['.']
    if '..' in file_info_list.keys():
        del file_info_list['..']
    for file_name in sorted(file_info_list.keys()):
        file_info = file_info_list[file_name]
        if file_info['is_dir'] == '1':
            children_resource_info = await _get_children_resource_info(duct, file_info['group_key'], file_info['content_key'])
            if len(children_resource_info) > 0:
                resource_info[file_name] = children_resource_info
        else:
            resource_info[file_name] = dict()
            revision_id, content_metadata = await _get_latest_content_metadata(duct, file_info['group_key'], file_info['content_key'])
            resource_info[file_name] = ScattFileResourceInfo(
                content_metadata['last_modified'],
                content_metadata['content_length'],
                content_metadata,
            )

    return resource_info


async def get_user_data_resource_info(duct):
    return await _get_children_resource_info(duct, ANNOTATION_GROUP_NAME, ROOT_DIR)


async def get_uploaded_resource_info(duct):
    return await _get_children_resource_info(duct, VIDEO_GROUP_NAME, ROOT_DIR)


def show_resource_tree(resource_info):
    def show_child_resource_tree(resource_info, indent):
        file_names = resource_info.keys()
        for idx, file_name in enumerate(file_names):
            resource_info_body = resource_info[file_name]
            is_last_file = (idx == (len(file_names) - 1))
            first_bar = ('`' if is_last_file else '|') + '-- '
            print(indent + first_bar + file_name)
            if isinstance(resource_info_body, dict):
                next_indent = indent + (' ' if is_last_file else '|').ljust(4, ' ')
                show_child_resource_tree(resource_info_body, next_indent)

    show_child_resource_tree(resource_info, '')
