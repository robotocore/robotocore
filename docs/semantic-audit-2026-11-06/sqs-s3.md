# Semantic audit — SQS and S3 vs moto 5.2.5

**Date:** 2026-11-06

**Method:** branch-by-branch source comparison of the Rust implementation against moto 5.2.5 (installed in `.venv`) — the reference this Rust port claims to preserve. Both sides were read implementation-by-implementation; no files were modified and nothing was executed during the audit. Each finding lists the operation affected, moto's behavior with file:line references, the Rust behavior with file:line references, the real-AWS verdict where determinable, and a confidence grade.

Findings marked **FIXED (2026-11-06)** were re-verified against the current branch source before marking; the original audit text is otherwise verbatim.

---

Audit of robotocore (Rust) vs moto 5.2.5 (installed in .venv) — SQS and S3 semantic divergences. All paths absolute; moto paths prefixed M= /home/jdanger/termag/projects/robotocore/repos/robotocore/.venv/lib/python3.13/site-packages/moto, R= /home/jdanger/termag/projects/robotocore/repos/robotocore/crates. Verified by reading both implementations branch-by-branch; no files modified, nothing run.

Overall observation: the Rust SQS crate deliberately mirrors moto's crash/error ordering (extensively annotated, live-verified comments) and is close on wire shapes, but it skips moto's *validation layer* almost entirely (message size, delay bounds, attribute-name validation, redrive-policy validation). The S3 crate is a lighter reimplementation with larger gaps in conditional requests, versioned deletes, and multipart part validation.

SQS

1. SendMessage: no message-size or delay validation at all
   (a) SendMessage / SendMessageBatch.
   (b) moto M/sqs/models.py:835-837 — body longer than queue.maximum_message_size raises InvalidParameterValue "One or more parameters are invalid. Reason: Message must be shorter than {size} bytes."; models.py:894-899 — DelaySeconds > 900 raises InvalidParameterValue "DelaySeconds must be >= 0 and <= 900"; models.py:829-833 — per-message DelaySeconds > 0 on a FIFO queue raises InvalidParameterValue "The request include parameter that is not valid for this queue type."
   (c) R/sqs/src/handler.rs:396-565 (`prepare_send`): no body-size check anywhere; DelaySeconds is silently `.min(900)`-clamped (handler.rs:408-412) and forced to 0 for FIFO (handler.rs:405-407). Batch path (handler.rs:2141-2252) inherits the same gaps, so moto's per-entry `DelaySeconds is invalid` → BatchResultErrorEntry path (models.py:956-965) can never fire either.
   (d) Real AWS: rejects all three (size vs the queue's MaximumMessageSize, delay ≤ 900, no per-message delay on FIFO). Rust diverges from both.
   (e) High confidence.

   **FIXED (2026-11-06, verified in source):** `prepare_send` (crates/sqs/src/handler.rs:405-435) now rejects an over-long body against the queue's MaximumMessageSize with moto's "Message must be shorter than {size} bytes." message, rejects DelaySeconds > 900 with moto's "DelaySeconds must be >= 0 and <= 900", and rejects a nonzero per-message DelaySeconds on a FIFO queue with moto's "The request include parameter that is not valid for this queue type."

2. CreateQueue: QueueAlreadyExists, MaximumMessageSize bounds, and FIFO-name rule all missing
   **FIXED (2026-11-08, verified live)**: CreateQueue now enforces the FIFO name rule
  (InvalidParameterValue "Queue name must end in .fifo for FIFO queues"), the
  MaximumMessageSize [1024, 1048576] bounds (InvalidAttributeValue "Invalid value
  for the parameter MaximumMessageSize."), and the QueueAlreadyExists attribute
  comparison — moto's sent-keys-only re-create semantics
  (models.py:288-299, 705-725). SetQueueAttributes runs the same checks.

   (a) CreateQueue.
   (b) moto M/sqs/models.py:711-722 — re-creating with differing attributes (for the keys sent) raises QueueAlreadyExists; models.py:294-298 — MaximumMessageSize < 1024 or > 1048576 raises InvalidAttributeValue; models.py:292-293 + 342-348 — FifoQueue=true with a name not ending ".fifo" raises InvalidParameterValue "Queue name must end in .fifo for FIFO queues".
   (c) R/sqs/src/handler.rs:304-375 — always resolves-or-creates idempotently and re-applies attributes (200); `QueueAttributes::apply` (R/sqs/src/models.rs:112-155) parses any u32 with no bounds; FIFO-ness is inferred from the name suffix (handler.rs:342-344) but an explicit `FifoQueue=true` attribute on a non-`.fifo` name is silently accepted (and does make the queue FIFO).
   (d) Real AWS: raises QueueAlreadyExists on attribute mismatch and InvalidAttributeValue on out-of-bounds size; FIFO queues require the .fifo suffix. Rust diverges from both.
   (e) High confidence.

3. RedrivePolicy is stored but never validated (DLQ silently no-ops)
   **FIXED (2026-11-08, verified live; wording corrected 2026-11-08 by
   adversarial wire review)**: _setup_dlq's validation chain
   (models.py:372-430) — with moto's exact shapes: non-JSON policies
   CRASH moto above its dict checks (an unhandled JSONDecodeError
   escaping the emptiness probe at models.py:373-377 = the werkzeug
   plain-500 page, which the twin mirrors crash-faithfully — the ledger's
   sqs_validation.create_rp_badjson row records the same); an empty
   decoded policy ({} or []) books None silently; a non-empty non-object
   policy answers "Redrive policy does not contain deadLetterTargetArn"
   (its list-membership false); missing arn / maxReceiveCount each carry
   moto's InvalidParameterValue sentence; the DLQ arn resolves against the
   account's own queues ("Could not find DLQ for {arn}") and the
   FIFO/ non-FIFO DLQ mismatch is InvalidParameterCombination.
   SetQueueAttributes runs the same redrive setup (the only validation
   moto's set path runs — its constructor checks live in __init__).

   (a) SetQueueAttributes/CreateQueue (RedrivePolicy) and redrive behavior.
   (b) moto M/sqs/models.py:372-424 — `_setup_dlq` raises InvalidParameterValue for non-JSON, missing deadLetterTargetArn, or missing maxReceiveCount; "AWS.SimpleQueueService.NonExistentQueue / Could not find DLQ for ..." when the target queue doesn't exist; InvalidParameterCombination "Fifo queues cannot use non fifo dead letter queues".
   (c) R/sqs/src/handler.rs:1585-1589 — the raw string is stored via `QueueAttributes::apply` (models.rs:145) with no checks; `redrive_target_arn`/`redrive_max_receive_count` (models.rs:234-257) silently return None on missing fields, so a policy pointing at a nonexistent queue or missing maxReceiveCount results in a queue with no dead-letter delivery and a 200 response. The receive-time redrive itself (handler.rs:1062-1155) matches moto's semantics closely, but only when the policy happens to be well-formed.
   (d) Real AWS validates the policy at set time (InvalidParameterValue / AWS.SimpleQueueService.NonExistentQueue equivalents). Rust diverges.
   (e) Medium-high confidence (validation gaps certain; exact real-AWS error codes not checked in botocore models).

4. GetQueueAttributes ignores AttributeNames entirely
   (a) GetQueueAttributes.
   (b) moto M/sqs/models.py:763-792 + responses.py:122-137 — filters the returned map to the requested names ("All" or specific), returns no Attributes (EmptyResult) when none requested, and raises InvalidAttributeName "Unknown Attribute {name}." for unknown names (including "").
   (c) R/sqs/src/handler.rs:1402-1506 — never reads `AttributeNames`; always returns the full attribute set (plus FIFO-only members only for FIFO queues), and never raises InvalidAttributeName. It also omits moto's FIFO attributes DeduplicationScope/FifoThroughputLimit and never includes KMS attributes from `extra` under the moto names (they do land via the free-form `extra` map, handler.rs:1478-1480).
   (d) Real AWS: filters by the requested names and rejects unknown ones. Rust diverges.
   (e) High confidence.

   **FIXED (2026-11-06, verified on main during the ledger-freshness pass; wording corrected 2026-11-07 by adversarial wire review):** GetQueueAttributes projects the requested subset and raises InvalidAttributeName with moto's "Unknown Attribute {n}." for unknown names (crates/sqs/src/handler.rs:1554-1586); with AttributeNames omitted or empty it returns the full default map (SQS's own documented default), unlike moto which drops the member entirely.

5. ReceiveMessage does not filter message attributes by MessageAttributeNames
   (a) ReceiveMessage.
   (b) moto M/sqs/models.py:669-691 (`_filter_message_attributes`: exact names, "All", and `<prefix>.*` patterns) applied to a deepcopy at responses.py:1036-1038, with MD5OfMessageAttributes computed over the *filtered* set (responses.py:314-316); non-matching attributes are omitted.
   (c) R/sqs/src/handler.rs:1196-1213 — if any requested name is "All" or matches *any* of the message's attribute keys (`name_pattern_matches`, handler.rs:1539-1547 — `*` wildcard instead of moto's `.*`-prefix rule), it returns the FULL unfiltered attribute map and computes the MD5 over the full set; a specific-name request returns every attribute instead of only the matching ones.
   (d) Real AWS returns only the matching attributes and an MD5 over exactly what it returns. Rust diverges on both payload and checksum.
   (e) High confidence.

   **FIXED (2026-11-06, verified on main during the ledger-freshness pass):** ReceiveMessage filters system and message attributes to the requested names (exact + prefix patterns per moto's rule) and computes MD5OfMessageAttributes over the filtered set (crates/sqs/src/handler.rs:181, 633-644, 1591-1600).

6. DeleteMessage does not accept old receipt handles or repeat deletes; DeleteMessageBatch Failed rows are shaped differently
   **FIXED (2026-11-08, verified live)**: DeleteMessage accepts already-deleted
   handles as moto's graceful no-op (moto's deleted_messages set is mirrored as
   a FIFO-evicted set per queue — the twin never rotates handles, so "any
   historical handle" is already the single stored handle), and the batch rows
   carry SenderFault: true + moto's per-handle wording
   'The input receipt handle "{h}" is not a valid receipt handle.'
   (moto responses.py:1069-1087).

   (a) DeleteMessage / DeleteMessageBatch.
   (b) moto M/sqs/models.py:613-631 — a delete matches ANY historical handle (`had_receipt_handle` includes `_old_receipt_handles`, models.py:203-210) and `deleted_messages` makes re-deleting the same handle a graceful no-op success; models.py:1069-1087 — a batch row failure emits `SenderFault: true`, Code `ReceiptHandleIsInvalid`, Message `'The input receipt handle "{handle}" is not a valid receipt handle.'`.
   (c) R/sqs/src/models.rs:359-370 — `delete_message` matches only the CURRENT `receipt_handle`; an old handle or a repeat delete returns Err → 400 ReceiptHandleIsInvalid (handler.rs:1263-1267). Batch failed rows (handler.rs:883-888) omit `SenderFault` entirely and use the message "The specified receipt handle is not valid.".
   (d) Real AWS accepts old (but retained) receipt handles and tolerates repeat deletes; the botocore model requires `SenderFault` on BatchResultErrorEntry. Rust diverges from both.
   (e) High confidence.

S3

7. DeleteObjects (POST /{bucket}?delete) ignores versioning and per-object VersionId
   (a) DeleteObjects.
   (b) moto M/s3/models.py:3016-3046 (`delete_objects` → `delete_object` per key): on a versioned bucket a version-less delete creates a delete marker and returns DeleteMarker/DeleteMarkerVersionId in the Deleted list (responses.py:1717-1783), a `VersionId` delete removes only that version, and an empty object list raises MalformedXML (responses.py:1730-1731).
   (c) R/s3/src/handler.rs:3295-3341 — parses only `<Key>` elements (ignores `<VersionId>`), clears ALL versions and markers of every key regardless of bucket versioning (never creates delete markers), never emits `<DeleteMarker>`/`<VersionId>` in the response, and never raises MalformedXML. Also no object-lock AccessDenied errors.
   (d) Real AWS creates delete markers on versioned buckets and honors per-object VersionIds. Rust diverges from both — this silently destroys all object versions.
   (e) High confidence.

   **FIXED in part (2026-11-07, verified in source):** versioned batch deletes now mirror single DeleteObject — a versioned bucket books a delete marker per key (a never-written key books one too, per real AWS; moto does not — `s3.delete_objects_unknown_key` is ledgered twin-preferred), an explicit VersionId removes only that version, the Deleted rows carry VersionId / DeleteMarker / DeleteMarkerVersionId, an empty object list is MalformedXML, and unknown keys stay tolerant. The remaining open half: object-lock AccessDenied errors (the twin does not enforce object locks).
8. CompleteMultipartUpload ignores the request body and skips all part validation
   (a) CompleteMultipartUpload.
   (b) moto M/s3/models.py:473-511 (`FakeMultipart.complete`): validates every listed part exists and its ETag matches (InvalidPart), that part numbers ascend (InvalidPartOrder, responses.py:3109-3116), that no non-final part is < S3_UPLOAD_PART_MIN_SIZE 5 MiB (EntityTooSmall, models.py:494-495), and that at least one part is listed (MalformedXML, models.py:502-503); only the listed parts, in listed order, compose the object.
   (c) R/s3/src/handler.rs:3753-3845 — the request body is never parsed; it concatenates ALL uploaded parts sorted by part number, verifies no ETags, and answers 400 InvalidPart "At least one part must be uploaded" when none exist (moto: MalformedXML). ETag composite math itself matches (models.rs:311-318).
   (d) Real AWS validates parts exactly like moto here (InvalidPart/InvalidPartOrder/EntityTooSmall). Rust diverges.
   (e) High confidence.

   **FIXED (2026-11-07, verified in source):** the request body is parsed for the listed `<Part>` entries; every listed part must exist and match its ETag (InvalidPart, moto's message), part numbers must ascend with no duplicates (InvalidPartOrder), every listed part except the last must be >= 5 MiB (EntityTooSmall), and only the listed parts in listed order compose the object. An empty list now answers MalformedXML (the old twin answered InvalidPart "At least one part must be uploaded"). Verified live against moto via the differential corpus; `s3.complete_multipart` is ledgered moto-only for the `<VersionId>null</VersionId>` member moto emits on unversioned buckets (real AWS omits VersionId).

9. Conditional requests (If-Match / If-None-Match / If-Modified-Since / If-Unmodified-Since) are unimplemented on GET/HEAD/PUT

   **FIXED (2026-11-07, verified in source):** If-Match / If-None-Match /
   If-Modified-Since / If-Unmodified-Since on GET and HEAD drive the
   moto-ordered evaluation (hard 412s first, then the 304 outcome); PUT
   honors If-Match (MissingKey when absent) and If-None-Match=*, DELETE
   honors If-Match — all with moto's "At least one of the pre-conditions
   you specified did not hold" text. A garbage date header behaves as
   absent (moto 500s; the malformed-date contract is deferred). Differen-
   tial steps get_if_match_wrong / get_if_modified_since_future /
   get_if_none_match_current / put_if_none_match_star /
   delete_if_match_wrong; the If-Modified-Since subsecond caveat stays
   as moto has it.
   (a) GetObject, HeadObject, PutObject, DeleteObject.
   (b) moto M/s3/responses.py:2188-2219 (`_get_key` — 304 on If-Modified-Since/If-None-Match, 412 PreconditionFailed on If-Match/If-Unmodified-Since), head_object at 2675-2687, put_object If-Match / If-None-Match=* preconditions at 2286-2296, delete_object If-Match at 3083-3090.
   (c) R/s3/src/handler.rs:2515-2637 (get), 2639-2719 (head), 1545-1606 (put), 2721-2775 (delete) — no If-* header is read anywhere (grep confirms zero occurrences in the crate).
   (d) Real AWS honors all of these with 304/412. Rust diverges from both.
   (e) High confidence.

10. CopyObject: no in-place guard, no copy-source-version-id header, wrong error for a missing source version

   **FIXED (2026-11-07, verified in source):** copying an object onto itself
   with no changing parameter answers moto's CopyObjectMustChangeSomething
   contract (400 InvalidRequest with real AWS's full legality message);
   missing source with an explicit versionId is 404 NoSuchVersion
   ("The specified version does not exist.") while a version-less missing
   source stays NoSuchKey; the response carries x-amz-copy-source-version-id
   for any non-null source version. Pins: copy_in_place / copy_in_place_replace
   / copy_missing_version corpus steps + five compat tests.
    (a) CopyObject.
    (b) moto M/s3/models.py:3066-3083 — copying onto itself with no changing parameter raises CopyObjectMustChangeSomething (unless versioned with a provided version id); responses.py:2429-2431 — a missing source with versionId raises MissingVersion (code NoSuchVersion) vs MissingKey without one; responses.py:2446-2447 — response carries `x-amz-copy-source-version-id` whenever the source version != "null".
    (c) R/s3/src/handler.rs:2344-2451 (`do_copy_object`) — no in-place check (silently succeeds), always 404 NoSuchKey for any missing source (bucket or version), and never sends x-amz-copy-source-version-id. Metadata/tagging directive handling (COPY/REPLACE) matches moto.
    (d) Real AWS: InvalidRequest "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes"; NoSuchVersion for a bad source version; always returns x-amz-copy-source-version-id for versioned sources. Rust diverges.
    (e) Medium-high confidence.

11. Multipart edges: partNumber unvalidated, UploadPartCopy ignores the copy range, AbortMultipartUpload never 404s
    (a) UploadPart / UploadPartCopy / AbortMultipartUpload / ListParts.
    (b) moto M/s3/responses.py:2563-2577 — partNumber > 10000 raises InvalidMaxPartNumberArgument; M/s3/models.py:513-515 — partNumber < 1 raises NoSuchUpload; responses.py:2592-2597 + models.py:2774-2793 — UploadPartCopy slices the source by `x-amz-copy-source-range` (bytes=start-end); models.py:2660-2665 — aborting an unknown uploadId raises NoSuchUpload.
    (c) R/s3/src/handler.rs:3613-3662 — no partNumber bounds at all (a missing partNumber stores part 0); 3664-3751 — `do_upload_part_copy` copies the ENTIRE source object, ignoring x-amz-copy-source-range; 3847-3860 — abort removes silently and answers 204 even for an unknown uploadId. ListParts (3899-3948) also ignores max-parts/part-number-marker and always answers IsTruncated=false (moto paginates, models.py:2667-2685).
    (d) Real AWS enforces partNumber 1..10000, applies the copy range, and answers NoSuchUpload (404) for unknown upload ids. Rust diverges.
    (e) High confidence.

    **FIXED (2026-11-07, verified in source):** partNumber > 10000 answers 400 InvalidArgument ("Part number must be an integer between 1 and 10000, inclusive" — moto's InvalidMaxPartNumberArgument text) and partNumber < 1 answers 404 NoSuchUpload (moto's set_part branch) on UploadPart and UploadPartCopy; UploadPartCopy applies `x-amz-copy-source-range` as the python-clamped slice (missing/unparsable -> whole object; start > end -> python-empty slice, moto's semantics); AbortMultipartUpload answers 404 NoSuchUpload for an unknown uploadId; ListParts honors max-parts and part-number-marker with IsTruncated/NextPartNumberMarker.

12. Range handling: no 416 InvalidRange, and multi-range specs produce a wrong 206
    (a) GetObject/HeadObject with a Range header.
    (b) moto M/s3/responses.py:1785-1835 — begin ≥ size (or `bytes=-0`) raises InvalidRange (416 with actual size in the message); a multi-range spec (`bytes=0-1,3-4`) or unparsable spec falls back to 200 with the full body.
    (c) R/s3/src/handler.rs:288-308 (`parse_range`) returns Option — every failure mode (start ≥ size, suffix 0, unparsable) becomes None → 200 full body; a multi-range like `bytes=0-1,3-4` parses end as `1,3-4` → parse fail → silently treated as `end=size-1`, yielding a WRONG 206 partial body instead of 200.
    (d) Real AWS answers 416 InvalidRange for an unsatisfiable range. Rust diverges (and the multi-range case is actively wrong data).
    (e) Medium-high confidence.

    **FIXED (2026-11-07, verified in source):** `parse_range` returns a three-way outcome mirroring moto's `_handle_range_header`: multi-range/unparsable specs answer 200 with the full body; `bytes=-0` and begin >= size answer 416 InvalidRange ("The requested range is not satisfiable"); and begin > end with begin in bounds answers 200 full-body (moto's `begin > min(end, last)` branch). GET and HEAD both drive the parser now (HEAD answers the bare 416 + sliced Content-Length shape moto's HEAD branch produces). Verified against moto via the differential corpus (`s3.range_*`) and HEAD-range compat tests.

13. Object tagging: versionId ignored, no validation, wrong tag-count header name, no 404 on GET of a missing key

   **FIXED in part (2026-11-07, verified in source; wording corrected
   2026-11-08 by adversarial wire review):** Get/Put/DeleteObjectTagging are
   versionId-aware. Wire-verified per op: a marker versionId is 405
   MethodNotAllowed on PUT/DELETE-tagging but 404 NoSuchVersion on GET-tagging
   (moto's GET resolves without return_delete_marker); a marked key without a
   versionId is 405 on PUT-tagging (moto resolves the marker) and 404
   NoSuchKey on GET-tagging; moto's DELETE-tagging dereferences a None key and
   500s on every missing-target shape — the twin's typed 404/405 answers are
   the recorded twin-preferred divergence. PUT validates the moto limits (>10
   tags → BadRequest "Object tags cannot be greater than 10"; an
   "aws"-prefixed key → InvalidTag "Your TagKey cannot be prefixed with
   aws:", matching moto's startswith("aws") check). Still open: moto's shared
   tagger.validate_tags also rejects 129-char keys, `k(x)`-shaped keys,
   257-char values and `v(x)` values (InvalidTag) — the twin accepted
   those (probes recorded in review 438).

   **FIXED (2026-11-09, live-probed against moto 5.2.5):** the shared
   validator (utilities/tagging_service.py:109-169) is mirrored —
   per-member key/value length caps (128/256) and character classes
   (`^(?!aws:)[{a-zA-Z0-9 }_.://=+-@%]*$` for keys; no lookahead in the
   Rust regex crate, so the prefix + class check is by hand; moto's
   message text preserved verbatim), collected into ONE compound
   InvalidTag ("N validation errors detected: ...", indexed by TagSet
   position). moto's custom checks (>10; aws: prefix) raise before the
   compound. The x-amz-tagging HEADER path validates on PutObject too
   (live-probed: moto PutObject Tagging="k(x)=v" answers the same
   compound). The wire arrival is a DICT — duplicates collapse
   first-position/last-value before the count and compound validators.
   Order-of-operations mirrors moto (review-probed live on both
   engines): PutObject stores first, the header validates after — a
   failure leaves the object present and UNTAGGED; CopyObject's
   x-amz-tagging-directive REPLACE runs the header list through the
   same validator after the copy stores — the failed copy keeps the
   COPIED source tags and a tag-only self-copy still answers
   InvalidRequest (moto's must-change condition does not count the
   tagging directive). put_object_tagging parses an ordered vec before
   folding into the stored map.

   Recorded edges around the same surface (raw SigV4-probed 2026-11-09 —
   probed raw because the empty-body members are unreachable through
   boto3, which enforces TagKey minLength 1; duplicate keys DO pass
   through boto3): (a) a duplicate-Key TagSet stores LAST-value into
   the collapsed map on both engines on the XML path — but the HEADER
   path (parse_qs → values[0]) keeps the FIRST value ("k=1&k=2"
   stores 1, live-probed); the twin's initial HashMap collapse always
   kept last-value → CLOSED 2026-11-09 with a per-path collapse
   (first-value header, last-value XML) after the stored set became
   an ordered pairs vec (insertion order) and GET TagSet began
   rendering member-for-member like moto's tagger (live cross-check);
   snapshots restore both shapes; (b) an
   empty-TagSet body (`<Tagging></Tagging>`) and a `<Tag>` with an
   empty `<Key>` make moto HTTP 500 (a response-layer XML unpack and a
   backend None-subscript respectively — moto tagging-path crashes)

   OPEN tape (same header surface, pre-existing, live-probed by the
   #474 review): (c) `tags_list_from_header` does not decode '+' to a
   space (moto "a=b+c" stores 'b c'; the twin stores 'b+c'); (d) it
   decodes BEFORE splitting on '&' (moto parse_qs splits first, so
   "a=1%26k=2" parses as ONE pair with Value "1&k=2" → InvalidTag; the
   twin splits the decoded string into two clean pairs and accepts);
   (e) CreateMultipartUpload parses x-amz-tagging (moto responses.py
   :3137) and validates+applies it at CompleteMultipartUpload (moto
   models.py:2744, stores values[0] per the header rule) — the twin
   ignores the header on both MP surfaces. Any future MPU-tagging lane
   must use keep_last=false.

   MPU lane CLOSED (2026-11-09): CreateMultipartUpload captures the
   collapsed first-value header tags without validating (moto
   responses.py:3137 stores raw) and CompleteMultipartUpload routes
   them through put_object_tagging AFTER the object stores (moto
   models.py:2744) — a bad set fails the complete with the compound
   InvalidTag and leaves the stored object UNTAGGED (live-verified
   both engines: object persists, TagSet empty); a good set applies
   with first-value collapse ("m=1&m=2" stores m=1). Pin:
   TestS3TaggingVersioning.test_mpu_tag_lane.

   Event parity (added after the #475 review's objection #1): moto's
   put_object fires ObjectCreated:Put BEFORE the tagging raise — the
   failed complete now delivers the Put event with the object present
   and untagged (live-verified via a subscribed SQS queue both
   engines), and the success path delivers Put +
   CompleteMultipartUpload (moto's two events; the twin previously
   sent only Complete). Delivered in moto's ORDER after the sqs
   bridge's drain_pending was reversed-walked (it pushed matches
   back-to-front, so two same-tick events reached the queue
   Complete-first; crates/sqs/src/bridge.rs now drains the pending
   Vec forward and removes right-to-left — live-verified Put-then-
   Complete). Recorded lead (out-of-lane): completed multipart
   objects do not reach maybe_replicate — replication on the
   complete path is a pre-existing gap — CLOSED 2026-11-09:
   CompleteMultipartUpload now calls maybe_replicate like the direct
   put paths (both copy the stored version's tags with the object) —
   including the INVALID-pending lane (replicated untagged, exactly
   like put_object's error path; botocore rejects Tagging on
   CompleteMultipartUpload client-side, so invalid sets ride from
   CREATE). Pin: TestS3TaggingVersioning.test_mpu_complete_replicates.

   Differential-corpus note (2026-11-09): extending the s3 corpus with
   replication-config steps surfaces the pre-existing checksum slot
   gaps (moto's put response carries ChecksumCRC32, put/list Contents
   carry ChecksumAlgorithm [CRC32] — recorded as s3.list_v2_prefix).
   The replication ENGINE itself is not single-bucket-corpus
   exercisable (nothing crosses an engine boundary; the
   twin-preferred engine ships to a dest bucket named in the rule, so
   a two-bucket corpus is required after the models-side checksum
   slot is fixed). Two more single-bucket divergences also surface
   and stay open: moto REBUILDS the config on GET (injects
   DeleteMarkerReplication {"Status": "Disabled"}, forces
   Filter: {"Prefix": ""}) where the twin echoes the stored XML; and
   moto accepts a replication config whose dest bucket doesn't exist
   while the twin answers 400 InvalidRequest (the dest bucket must be
   created before the config PUT on the twin).
   where the twin answers 200 with the member dropped — twin-preferred,
   recorded as the oracle misbehaving. 6 pins in TestS3TaggingVersioning
   (129-char key; k(x) key regex; 257-char value; compound two-error
   message; header-path validation; clean header still stores).
    (a) PutObjectTagging / GetObjectTagging / DeleteObjectTagging (+ GetObject tag-count header).
    (b) moto M/s3/responses.py:2496-2515 — honors `versionId`, raises MethodNotAllowed (405) on a delete marker, MissingKey/MissingVersion otherwise; M/s3/models.py:2517-2545 — rejects > 10 tags ("Object tags cannot be greater than 10") and `aws:`-prefixed keys; models.py:357 — GET/HEAD emits header `x-amz-tagging-count`.
    (c) R/s3/src/handler.rs:3377-3404 — always tags the LATEST version, ignores versionId, no tag-count/system-tag validation; 3406-3427 — GetObjectTagging on a nonexistent key returns 200 with an empty TagSet (no existence check); the GET/HEAD tag header is emitted as `x-amz-tag-count` (handler.rs:2627-2629, 2706-2708) — a name no SDK recognizes.
    (d) Real AWS: honors versionId, enforces the 10-tag limit and the aws: prefix ban, answers NoSuchKey for a missing key, and uses `x-amz-tagging-count`. Rust diverges on every branch.
    (e) High confidence (header name is a certain bug).

    **FIXED in part (2026-11-06, verified in source):** the tag-count header is fixed — GET/HEAD now emits `x-amz-tagging-count` (crates/s3/src/handler.rs:2636, 2714). The rest of this finding remains open: versionId is still ignored, tag-count/`aws:`-prefix validation is still absent, and GetObjectTagging on a missing key still returns 200 with an empty TagSet.

14. Storage class: accepted unvalidated, and Glacier objects are served without InvalidObjectState

   **FIXED (2026-11-07, verified in source; wording corrected 2026-11-08 by
   adversarial wire review):** x-amz-storage-class is validated against moto's
   nine-class whitelist at PUT, COPY and CreateMultipartUpload
   (InvalidStorageClass, 400 — moto validates at create too); GET of a
   GLACIER/DEEP_ARCHIVE object without a completed restore answers 403
   InvalidObjectState exactly like moto; **HEAD does not gate in moto (its
   head_object has no storage-class check → 200) — the twin's HEAD answers
   real AWS's 403 there**, ledgered twin-preferred behind `s3.head_glacier`
   in the differential. CopyObject of an unrestored archived source answers
   403 ObjectNotInActiveTierError; a copy of a RESTORED archived source keeps
   the GLACIER class per real AWS (moto drops it to STANDARD — ledgered
   twin-preferred); RestoreObject books the per-version marker that unblocks
   reads (re-restore answers 200 like moto's had_expiry_date path) and GET
   now carries the x-amz-restore + x-amz-storage-class headers.
    (a) PutObject (x-amz-storage-class) and GetObject/HeadObject/CopyObject on archive-class objects.
    (b) moto M/s3/models.py:2310-2311 — an unknown storage class raises InvalidStorageClass "The storage class you specified is not valid"; M/s3/utils.py:38-50 + responses.py:2201-2205 — GET/HEAD of a GLACIER/DEEP_ARCHIVE object not restored raises InvalidObjectState (403); copy of one raises ObjectNotInActiveTierError (responses.py:2398-2404).
    (c) R/s3/src/handler.rs:1567-1571 — any storage-class string is stored verbatim; GetObject (2515+) and CopyObject (2344+) serve archive-class data with no state check; RestoreObject (3983-4008) implements 202/403 but nothing gates regular GETs behind a restore.
    (d) Real AWS rejects unknown storage classes and gates archived objects behind restore. Rust diverges.
    (e) Medium-high confidence.

15. Bucket-level PUT validation: LocationConstraint ignored, duplicate-create semantics, versioning status parsing
    (a) CreateBucket and PutBucketVersioning.
    (b) moto M/s3/responses.py:1104-1119 — a non-us-east-1 request requires a matching LocationConstraint (IllegalLocationConstraintException / IncompatibleLocationConstraintException) and us-east-1 rejects an explicit us-east-1 constraint; 1131-1148 — re-creating an owned bucket outside us-east-1 raises BucketAlreadyOwnedByYou (409); 1003-1011 — PutBucketVersioning with no `<Status>` regex match returns 404, and any `[A-Za-z]+` status (e.g. "enabled") is stored and echoed back by GetBucketVersioning (models.py:2062-2066).
    (c) R/s3/src/handler.rs:1391-1462 — the body is read only to detect a Directory bucket type; no location-constraint validation, and a duplicate create always answers 200 idempotently in every region; 3538-3553 — only exact "Enabled"/"Suspended" set state, anything else silently becomes "never enabled" with a 200 (moto: 404; and moto would echo arbitrary casings).
    (d) Real AWS: enforces the location constraint, answers BucketAlreadyOwnedByYou (409) in non-us-east-1, and rejects an invalid versioning status (MalformedXML-ish 400) — moto's 404 and case-sensitivity are its own quirks. Rust diverges from both.
    (e) Medium confidence (real-AWS exact codes not re-checked in botocore models).

    **FIXED (2026-11-08, verified live)**: CreateBucket now runs moto's
    region gate before any bucket resolution — us-east-1 answers
    InvalidLocationConstraint ("The specified location-constraint is not
    valid") for its own explicit constraint while allowing other regions'
    constraints (bucket_region stamped from the LC, responses.py:1104-1119);
    every other region requires the constraint present and matching
    (IllegalLocationConstraintException with moto's two sentences). Owned
    recreation: idempotent ONLY for the us-east-1↔us-east-1 pair;
    everything else answers BucketAlreadyOwnedByYou 409 ("Your previous
    request to create the named bucket succeeded and you already own it.")
    — one caveated row: the twin's state is per region-keyed store, so
    moto's GLOBAL cross-region collision rows (bucket created under
    region A's own client, recreated from region B with no constraint)
    are unreachable here; documented twin-preferred architecture gap in
    the ledger. PutBucketVersioning mirrors the `<Status>[A-Za-z]+</>`
    regex: no match → moto's bare 404 (empty body, Code '404'); any
    matched charset stores VERBATIM into a versioning_status slot that
    GetBucketVersioning echoes ("Wibble"/"enabled"/"Disabled" all
    round-trip at 200) while the internal Option<bool> feature flag stays
    Enabled/Suspended-only, exactly like moto's is_versioned comparison.
    Verified on the live moto matrix (LC 7 rows, versioning 6 rows).

Follow-ups opened by the 2026-11-07 adversarial review of the multipart/DeleteObjects tranche (pre-existing, unchanged by it):

- Bulk `delete_objects` fires no `s3:ObjectRemoved:Delete*` notifications (single `delete_object` does fire; moto fires per-entry at models.py:2956-2964/2994-3000).
- Completing an already-completed uploadId answers 404 NoSuchUpload; moto answers 200 idempotently (responses.py:3183-3190).
- Garbage-in divergence ledger candidates (twin answers the real-AWS shape where moto 500s): a `<Part>` with a missing/non-numeric PartNumber or missing ETag makes moto IndexError/AttributeError (responses.py:3113-3116) where the twin answers 400 InvalidPartOrder/InvalidPart; a `Range` header without the `bytes=` prefix makes moto's rsplit unpack raise an uncaught 500 (responses.py:1793) where the twin answers 200 with the full body.

Non-findings worth recording (checked, match): SQS visibility/dedup/redrive ordering and the moto-crash mirrors are faithful; S3 multipart composite ETag math (models.rs:311-318) matches moto's md5-of-md5s; ListObjects v1/v2 delimiter grouping, KeyCount, and continuation-token semantics match moto (Rust's v1 NextMarker-only-with-delimiter actually matches real AWS better than moto); the PurgeQueue 60s throttle is on by default in moto 5.2.5 too (settings.py:87-91). Rust's FIFO duplicate-send response (original MessageId/SequenceNumber) diverges from moto's new-MessageId behavior but matches real AWS — not counted as a finding.
