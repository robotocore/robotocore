"""Native Rekognition provider.

Implements collection CRUD, face operations, image analysis, video analysis,
projects, stream processors, face liveness, and tagging.
Forwards other operations to Moto.
"""

import json
import time
import uuid

from starlette.requests import Request
from starlette.responses import Response

from robotocore.providers.moto_bridge import forward_to_moto

# In-memory stores keyed by (account_id, region)
# collections: {(acct, region): {collection_id: {...metadata...}}}
_collections: dict[tuple[str, str], dict[str, dict]] = {}
# tags: {arn: {key: value}}
_tags: dict[str, dict[str, str]] = {}
# faces: {(acct, region): {collection_id: {face_id: {...face record...}}}}
_faces: dict[tuple[str, str], dict[str, dict[str, dict]]] = {}
# video jobs: {job_id: {job_type, status, results}}
_video_jobs: dict[str, dict] = {}
# projects: {(acct, region): {project_name: {...}}}
_projects: dict[tuple[str, str], dict[str, dict]] = {}
# stream processors: {(acct, region): {name: {...}}}
_stream_processors: dict[tuple[str, str], dict[str, dict]] = {}
# face liveness sessions: {session_id: {...}}
_liveness_sessions: dict[str, dict] = {}

_JSON_TYPE = "application/x-amz-json-1.1"


def _get_collections(account_id: str, region: str) -> dict[str, dict]:
    key = (account_id, region)
    if key not in _collections:
        _collections[key] = {}
    return _collections[key]


def _get_faces(account_id: str, region: str) -> dict[str, dict[str, dict]]:
    key = (account_id, region)
    if key not in _faces:
        _faces[key] = {}
    return _faces[key]


def _get_projects(account_id: str, region: str) -> dict[str, dict]:
    key = (account_id, region)
    if key not in _projects:
        _projects[key] = {}
    return _projects[key]


def _get_stream_processors(account_id: str, region: str) -> dict[str, dict]:
    key = (account_id, region)
    if key not in _stream_processors:
        _stream_processors[key] = {}
    return _stream_processors[key]


def _collection_arn(account_id: str, region: str, collection_id: str) -> str:
    return f"arn:aws:rekognition:{region}:{account_id}:collection/{collection_id}"


def _project_arn(account_id: str, region: str, project_name: str) -> str:
    return f"arn:aws:rekognition:{region}:{account_id}:project/{project_name}/{int(time.time())}"


def _stream_processor_arn(account_id: str, region: str, name: str) -> str:
    return f"arn:aws:rekognition:{region}:{account_id}:streamprocessor/{name}"


def _not_found(message: str) -> tuple[int, dict]:
    return (400, {"__type": "ResourceNotFoundException", "Message": message})


async def handle_rekognition_request(request: Request, region: str, account_id: str) -> Response:
    """Handle Rekognition requests, intercepting unimplemented operations."""
    target = request.headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    handler = _ACTION_MAP.get(action)
    if handler:
        body = await request.body()
        params = json.loads(body) if body else {}
        result = handler(params, region, account_id)
        if isinstance(result, tuple):
            # (status_code, body_dict)
            return Response(
                content=json.dumps(result[1]),
                status_code=result[0],
                media_type=_JSON_TYPE,
            )
        return Response(
            content=json.dumps(result),
            status_code=200,
            media_type=_JSON_TYPE,
        )

    return await forward_to_moto(request, "rekognition", account_id=account_id)


# ---------------------------------------------------------------------------
# Collection CRUD
# ---------------------------------------------------------------------------


def _create_collection(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id in store:
        return (
            400,
            {
                "__type": "ResourceAlreadyExistsException",
                "Message": "A collection with the specified ID already exists.",
            },
        )

    arn = _collection_arn(account_id, region, collection_id)
    now = time.time()
    store[collection_id] = {
        "CollectionId": collection_id,
        "CollectionArn": arn,
        "CreationTimestamp": now,
        "FaceCount": 0,
        "FaceModelVersion": "6.0",
    }
    _tags[arn] = dict(params.get("Tags", {}))

    return {
        "StatusCode": 200,
        "CollectionArn": arn,
        "FaceModelVersion": "6.0",
    }


def _describe_collection(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    col = store[collection_id]
    return {
        "FaceCount": col["FaceCount"],
        "FaceModelVersion": col["FaceModelVersion"],
        "CollectionARN": col["CollectionArn"],
        "CreationTimestamp": col["CreationTimestamp"],
    }


def _list_collections(params: dict, region: str, account_id: str) -> dict:
    store = _get_collections(account_id, region)
    max_results = params.get("MaxResults", 1000)
    next_token = params.get("NextToken")

    all_ids = sorted(store.keys())

    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0

    end = start + max_results
    result_ids = all_ids[start:end]

    resp: dict = {
        "CollectionIds": result_ids,
        "FaceModelVersions": [store[cid]["FaceModelVersion"] for cid in result_ids],
    }
    if end < len(all_ids):
        resp["NextToken"] = str(end)
    return resp


def _delete_collection(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    arn = store[collection_id]["CollectionArn"]
    del store[collection_id]
    _tags.pop(arn, None)
    # Clean up faces for this collection
    face_store = _get_faces(account_id, region)
    face_store.pop(collection_id, None)

    return {"StatusCode": 200}


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------


def _tag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceArn", "")
    tags = params.get("Tags", {})

    if not _resource_exists(arn, region, account_id):
        return _not_found("The resource with the specified ARN was not found.")

    if arn not in _tags:
        _tags[arn] = {}
    _tags[arn].update(tags)
    return {}


def _list_tags_for_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceArn", "")

    if not _resource_exists(arn, region, account_id):
        return _not_found("The resource with the specified ARN was not found.")

    return {"Tags": _tags.get(arn, {})}


def _untag_resource(params: dict, region: str, account_id: str) -> dict:
    arn = params.get("ResourceArn", "")
    tag_keys = params.get("TagKeys", [])

    if not _resource_exists(arn, region, account_id):
        return _not_found("The resource with the specified ARN was not found.")

    if arn in _tags:
        for key in tag_keys:
            _tags[arn].pop(key, None)
    return {}


def _resource_exists(arn: str, region: str, account_id: str) -> bool:
    """Check if a resource ARN corresponds to a known resource."""
    store = _get_collections(account_id, region)
    for col in store.values():
        if col["CollectionArn"] == arn:
            return True
    proj_store = _get_projects(account_id, region)
    for proj in proj_store.values():
        if proj["ProjectArn"] == arn:
            return True
    sp_store = _get_stream_processors(account_id, region)
    for sp in sp_store.values():
        if sp["StreamProcessorArn"] == arn:
            return True
    return False


# ---------------------------------------------------------------------------
# Face operations
# ---------------------------------------------------------------------------


def _index_faces(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    face_store = _get_faces(account_id, region)
    if collection_id not in face_store:
        face_store[collection_id] = {}

    # Generate a mock face record
    face_id = str(uuid.uuid4())
    image_id = str(uuid.uuid4())
    face_record = {
        "FaceId": face_id,
        "ImageId": image_id,
        "ExternalImageId": params.get("ExternalImageId", ""),
        "Confidence": 99.99,
        "BoundingBox": {"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1},
    }
    face_store[collection_id][face_id] = face_record
    store[collection_id]["FaceCount"] = len(face_store[collection_id])

    return {
        "FaceRecords": [
            {
                "Face": face_record,
                "FaceDetail": {
                    "BoundingBox": face_record["BoundingBox"],
                    "Confidence": 99.99,
                    "Landmarks": [
                        {"Type": "eyeLeft", "X": 0.3, "Y": 0.3},
                        {"Type": "eyeRight", "X": 0.5, "Y": 0.3},
                        {"Type": "nose", "X": 0.4, "Y": 0.5},
                    ],
                    "Quality": {"Brightness": 80.0, "Sharpness": 90.0},
                },
            }
        ],
        "FaceModelVersion": "6.0",
        "UnindexedFaces": [],
    }


def _list_faces(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    face_store = _get_faces(account_id, region)
    faces = face_store.get(collection_id, {})
    max_results = params.get("MaxResults", 1000)

    face_list = list(faces.values())[:max_results]
    return {
        "Faces": face_list,
        "FaceModelVersion": "6.0",
    }


def _search_faces(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    face_id = params.get("FaceId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    face_store = _get_faces(account_id, region)
    faces = face_store.get(collection_id, {})

    # Return other faces as matches (excluding the query face)
    matches = []
    for fid, face in faces.items():
        if fid != face_id:
            matches.append({"Similarity": 95.0, "Face": face})

    return {
        "SearchedFaceId": face_id,
        "FaceMatches": matches,
        "FaceModelVersion": "6.0",
    }


def _search_faces_by_image(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    face_store = _get_faces(account_id, region)
    faces = face_store.get(collection_id, {})

    matches = [{"Similarity": 95.0, "Face": face} for face in faces.values()]

    return {
        "SearchedFaceBoundingBox": {"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1},
        "SearchedFaceConfidence": 99.99,
        "FaceMatches": matches,
        "FaceModelVersion": "6.0",
    }


def _delete_faces(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    face_ids = params.get("FaceIds", [])
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    face_store = _get_faces(account_id, region)
    faces = face_store.get(collection_id, {})
    deleted = []
    for fid in face_ids:
        if fid in faces:
            del faces[fid]
            deleted.append(fid)
    store[collection_id]["FaceCount"] = len(faces)

    return {"DeletedFaces": deleted}


# ---------------------------------------------------------------------------
# Image analysis
# ---------------------------------------------------------------------------


def _detect_faces(params: dict, region: str, account_id: str) -> dict:
    return {
        "FaceDetails": [
            {
                "BoundingBox": {"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1},
                "Confidence": 99.99,
                "Landmarks": [
                    {"Type": "eyeLeft", "X": 0.3, "Y": 0.3},
                    {"Type": "eyeRight", "X": 0.5, "Y": 0.3},
                    {"Type": "nose", "X": 0.4, "Y": 0.5},
                    {"Type": "mouthLeft", "X": 0.3, "Y": 0.7},
                    {"Type": "mouthRight", "X": 0.5, "Y": 0.7},
                ],
                "Quality": {"Brightness": 80.0, "Sharpness": 90.0},
                "Pose": {"Roll": 0.0, "Yaw": 0.0, "Pitch": 0.0},
            }
        ]
    }


def _detect_moderation_labels(params: dict, region: str, account_id: str) -> dict:
    return {
        "ModerationLabels": [],
        "ModerationModelVersion": "6.0",
    }


def _detect_protective_equipment(params: dict, region: str, account_id: str) -> dict:
    return {
        "Persons": [
            {
                "Id": 0,
                "BoundingBox": {"Width": 0.5, "Height": 0.8, "Left": 0.2, "Top": 0.1},
                "Confidence": 99.0,
                "BodyParts": [
                    {
                        "Name": "FACE",
                        "Confidence": 99.0,
                        "EquipmentDetections": [],
                    }
                ],
            }
        ],
        "ProtectiveEquipmentModelVersion": "1.0",
    }


def _recognize_celebrities(params: dict, region: str, account_id: str) -> dict:
    return {
        "CelebrityFaces": [],
        "UnrecognizedFaces": [
            {
                "BoundingBox": {"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1},
                "Confidence": 99.0,
                "Landmarks": [
                    {"Type": "eyeLeft", "X": 0.3, "Y": 0.3},
                    {"Type": "eyeRight", "X": 0.5, "Y": 0.3},
                ],
            }
        ],
    }


def _get_celebrity_info(params: dict, region: str, account_id: str) -> dict:
    celebrity_id = params.get("Id", "")
    return {
        "Name": f"Celebrity-{celebrity_id}",
        "Urls": [],
    }


# ---------------------------------------------------------------------------
# Video analysis (Start/Get pattern)
# ---------------------------------------------------------------------------


def _make_job_id() -> str:
    return str(uuid.uuid4())


def _video_metadata() -> dict:
    return {
        "Codec": "h264",
        "DurationMillis": 5000,
        "Format": "QuickTime / MOV",
        "FrameRate": 29.97,
        "FrameHeight": 720,
        "FrameWidth": 1280,
    }


def _start_face_detection(params: dict, region: str, account_id: str) -> dict:
    job_id = _make_job_id()
    _video_jobs[job_id] = {
        "JobType": "FaceDetection",
        "JobStatus": "SUCCEEDED",
        "VideoMetadata": _video_metadata(),
        "Faces": [
            {
                "Timestamp": 0,
                "Face": {
                    "BoundingBox": {"Width": 0.5, "Height": 0.6, "Left": 0.2, "Top": 0.1},
                    "Confidence": 99.0,
                    "Landmarks": [
                        {"Type": "eyeLeft", "X": 0.3, "Y": 0.3},
                        {"Type": "eyeRight", "X": 0.5, "Y": 0.3},
                    ],
                },
            }
        ],
    }
    return {"JobId": job_id}


def _get_face_detection(params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("JobId", "")
    job = _video_jobs.get(job_id, {})
    return {
        "JobStatus": job.get("JobStatus", "SUCCEEDED"),
        "VideoMetadata": job.get("VideoMetadata", _video_metadata()),
        "Faces": job.get("Faces", []),
    }


def _start_label_detection(params: dict, region: str, account_id: str) -> dict:
    job_id = _make_job_id()
    _video_jobs[job_id] = {
        "JobType": "LabelDetection",
        "JobStatus": "SUCCEEDED",
        "VideoMetadata": _video_metadata(),
        "Labels": [
            {
                "Timestamp": 0,
                "Label": {
                    "Name": "Person",
                    "Confidence": 98.0,
                    "Instances": [],
                    "Parents": [],
                },
            }
        ],
    }
    return {"JobId": job_id}


def _get_label_detection(params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("JobId", "")
    job = _video_jobs.get(job_id, {})
    return {
        "JobStatus": job.get("JobStatus", "SUCCEEDED"),
        "VideoMetadata": job.get("VideoMetadata", _video_metadata()),
        "Labels": job.get("Labels", []),
        "LabelModelVersion": "3.0",
    }


def _start_celebrity_recognition(params: dict, region: str, account_id: str) -> dict:
    job_id = _make_job_id()
    _video_jobs[job_id] = {
        "JobType": "CelebrityRecognition",
        "JobStatus": "SUCCEEDED",
        "VideoMetadata": _video_metadata(),
        "Celebrities": [],
    }
    return {"JobId": job_id}


def _get_celebrity_recognition(params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("JobId", "")
    job = _video_jobs.get(job_id, {})
    return {
        "JobStatus": job.get("JobStatus", "SUCCEEDED"),
        "VideoMetadata": job.get("VideoMetadata", _video_metadata()),
        "Celebrities": job.get("Celebrities", []),
    }


def _start_content_moderation(params: dict, region: str, account_id: str) -> dict:
    job_id = _make_job_id()
    _video_jobs[job_id] = {
        "JobType": "ContentModeration",
        "JobStatus": "SUCCEEDED",
        "VideoMetadata": _video_metadata(),
        "ModerationLabels": [],
    }
    return {"JobId": job_id}


def _get_content_moderation(params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("JobId", "")
    job = _video_jobs.get(job_id, {})
    return {
        "JobStatus": job.get("JobStatus", "SUCCEEDED"),
        "VideoMetadata": job.get("VideoMetadata", _video_metadata()),
        "ModerationLabels": job.get("ModerationLabels", []),
        "ModerationModelVersion": "6.0",
    }


def _start_person_tracking(params: dict, region: str, account_id: str) -> dict:
    job_id = _make_job_id()
    _video_jobs[job_id] = {
        "JobType": "PersonTracking",
        "JobStatus": "SUCCEEDED",
        "VideoMetadata": _video_metadata(),
        "Persons": [
            {
                "Timestamp": 0,
                "Person": {
                    "Index": 0,
                    "BoundingBox": {"Width": 0.5, "Height": 0.8, "Left": 0.2, "Top": 0.1},
                },
            }
        ],
    }
    return {"JobId": job_id}


def _get_person_tracking(params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("JobId", "")
    job = _video_jobs.get(job_id, {})
    return {
        "JobStatus": job.get("JobStatus", "SUCCEEDED"),
        "VideoMetadata": job.get("VideoMetadata", _video_metadata()),
        "Persons": job.get("Persons", []),
    }


def _start_segment_detection(params: dict, region: str, account_id: str) -> dict:
    job_id = _make_job_id()
    _video_jobs[job_id] = {
        "JobType": "SegmentDetection",
        "JobStatus": "SUCCEEDED",
        "VideoMetadata": [_video_metadata()],
        "Segments": [],
        "SelectedSegmentTypes": [
            {"Type": "TECHNICAL_CUE", "ModelVersion": "2.0"},
            {"Type": "SHOT", "ModelVersion": "2.0"},
        ],
    }
    return {"JobId": job_id}


def _get_segment_detection(params: dict, region: str, account_id: str) -> dict:
    job_id = params.get("JobId", "")
    job = _video_jobs.get(job_id, {})
    return {
        "JobStatus": job.get("JobStatus", "SUCCEEDED"),
        "VideoMetadata": job.get("VideoMetadata", [_video_metadata()]),
        "Segments": job.get("Segments", []),
        "SelectedSegmentTypes": job.get(
            "SelectedSegmentTypes",
            [
                {"Type": "TECHNICAL_CUE", "ModelVersion": "2.0"},
                {"Type": "SHOT", "ModelVersion": "2.0"},
            ],
        ),
    }


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------


def _create_project(params: dict, region: str, account_id: str) -> dict:
    project_name = params.get("ProjectName", "")
    store = _get_projects(account_id, region)

    if project_name in store:
        return (
            400,
            {
                "__type": "ResourceInUseException",
                "Message": f"The project {project_name} already exists.",
            },
        )

    arn = _project_arn(account_id, region, project_name)
    store[project_name] = {
        "ProjectName": project_name,
        "ProjectArn": arn,
        "CreationTimestamp": time.time(),
        "Status": "CREATED",
    }
    _tags[arn] = {}

    return {"ProjectArn": arn}


def _describe_projects(params: dict, region: str, account_id: str) -> dict:
    store = _get_projects(account_id, region)
    max_results = params.get("MaxResults", 100)

    projects = []
    for proj in list(store.values())[:max_results]:
        projects.append(
            {
                "ProjectArn": proj["ProjectArn"],
                "CreationTimestamp": proj["CreationTimestamp"],
                "Status": proj["Status"],
            }
        )

    return {"ProjectDescriptions": projects}


def _delete_project(params: dict, region: str, account_id: str) -> dict:
    project_arn = params.get("ProjectArn", "")
    store = _get_projects(account_id, region)

    # Find project by ARN
    found_name = None
    for name, proj in store.items():
        if proj["ProjectArn"] == project_arn:
            found_name = name
            break

    if found_name is None:
        return _not_found(f"The project with ARN {project_arn} was not found.")

    del store[found_name]
    _tags.pop(project_arn, None)

    return {"Status": "DELETING"}


# ---------------------------------------------------------------------------
# Stream Processors
# ---------------------------------------------------------------------------


def _create_stream_processor(params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    store = _get_stream_processors(account_id, region)

    if name in store:
        return (
            400,
            {
                "__type": "ResourceInUseException",
                "Message": f"The stream processor {name} already exists.",
            },
        )

    arn = _stream_processor_arn(account_id, region, name)
    store[name] = {
        "Name": name,
        "StreamProcessorArn": arn,
        "Status": "STOPPED",
        "CreationTimestamp": time.time(),
        "Input": params.get("Input", {}),
        "Output": params.get("Output", {}),
        "RoleArn": params.get("RoleArn", ""),
        "Settings": params.get("Settings", {}),
    }
    _tags[arn] = {}

    return {"StreamProcessorArn": arn}


def _describe_stream_processor(params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    store = _get_stream_processors(account_id, region)

    if name not in store:
        return _not_found(f"The stream processor {name} was not found.")

    sp = store[name]
    return {
        "Name": sp["Name"],
        "StreamProcessorArn": sp["StreamProcessorArn"],
        "Status": sp["Status"],
        "CreationTimestamp": sp["CreationTimestamp"],
        "Input": sp["Input"],
        "Output": sp["Output"],
        "RoleArn": sp["RoleArn"],
        "Settings": sp["Settings"],
    }


def _list_stream_processors(params: dict, region: str, account_id: str) -> dict:
    store = _get_stream_processors(account_id, region)
    max_results = params.get("MaxResults", 100)

    processors = [
        {"Name": sp["Name"], "Status": sp["Status"]} for sp in list(store.values())[:max_results]
    ]

    return {"StreamProcessors": processors}


def _delete_stream_processor(params: dict, region: str, account_id: str) -> dict:
    name = params.get("Name", "")
    store = _get_stream_processors(account_id, region)

    if name not in store:
        return _not_found(f"The stream processor {name} was not found.")

    arn = store[name]["StreamProcessorArn"]
    del store[name]
    _tags.pop(arn, None)

    return {}


# ---------------------------------------------------------------------------
# Face Liveness
# ---------------------------------------------------------------------------


def _create_face_liveness_session(params: dict, region: str, account_id: str) -> dict:
    session_id = str(uuid.uuid4())
    _liveness_sessions[session_id] = {
        "SessionId": session_id,
        "Status": "CREATED",
        "Confidence": 99.5,
        "CreatedTimestamp": time.time(),
    }
    return {"SessionId": session_id}


def _get_face_liveness_session_results(params: dict, region: str, account_id: str) -> dict:
    session_id = params.get("SessionId", "")
    session = _liveness_sessions.get(session_id)

    if session is None:
        return (
            400,
            {
                "__type": "SessionNotFoundException",
                "Message": "The session was not found.",
            },
        )

    return {
        "SessionId": session_id,
        "Status": "SUCCEEDED",
        "Confidence": session.get("Confidence", 99.5),
    }


def _list_users(params: dict, region: str, account_id: str) -> dict:
    collection_id = params.get("CollectionId", "")
    store = _get_collections(account_id, region)

    if collection_id not in store:
        return _not_found(f"The collection id: {collection_id} does not exist")

    return {"Users": [], "NextToken": None}


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    # Collection CRUD
    "CreateCollection": _create_collection,
    "DescribeCollection": _describe_collection,
    "ListCollections": _list_collections,
    "DeleteCollection": _delete_collection,
    # Tagging
    "TagResource": _tag_resource,
    "ListTagsForResource": _list_tags_for_resource,
    "UntagResource": _untag_resource,
    # Face operations
    "IndexFaces": _index_faces,
    "ListFaces": _list_faces,
    "SearchFaces": _search_faces,
    "SearchFacesByImage": _search_faces_by_image,
    "DeleteFaces": _delete_faces,
    # Image analysis
    "DetectFaces": _detect_faces,
    "DetectModerationLabels": _detect_moderation_labels,
    "DetectProtectiveEquipment": _detect_protective_equipment,
    "RecognizeCelebrities": _recognize_celebrities,
    "GetCelebrityInfo": _get_celebrity_info,
    # Video analysis
    "StartFaceDetection": _start_face_detection,
    "GetFaceDetection": _get_face_detection,
    "StartLabelDetection": _start_label_detection,
    "GetLabelDetection": _get_label_detection,
    "StartCelebrityRecognition": _start_celebrity_recognition,
    "GetCelebrityRecognition": _get_celebrity_recognition,
    "StartContentModeration": _start_content_moderation,
    "GetContentModeration": _get_content_moderation,
    "StartPersonTracking": _start_person_tracking,
    "GetPersonTracking": _get_person_tracking,
    "StartSegmentDetection": _start_segment_detection,
    "GetSegmentDetection": _get_segment_detection,
    # Projects
    "CreateProject": _create_project,
    "DescribeProjects": _describe_projects,
    "DeleteProject": _delete_project,
    # Stream Processors
    "CreateStreamProcessor": _create_stream_processor,
    "DescribeStreamProcessor": _describe_stream_processor,
    "ListStreamProcessors": _list_stream_processors,
    "DeleteStreamProcessor": _delete_stream_processor,
    # Face Liveness
    "CreateFaceLivenessSession": _create_face_liveness_session,
    "GetFaceLivenessSessionResults": _get_face_liveness_session_results,
    # Users
    "ListUsers": _list_users,
}
