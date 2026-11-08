# Semantic audit — DynamoDB and IAM vs moto 5.2.5

**Date:** 2026-11-06

**Method:** branch-by-branch source comparison of the Rust implementation against moto 5.2.5 (installed in `.venv`) — the reference this Rust port claims to preserve. Both sides were read implementation-by-implementation; no files were modified and nothing was executed during the audit. Each finding lists the operation affected, moto's behavior with file:line references, the Rust behavior with file:line references, the real-AWS verdict where determinable, and a confidence grade (A = both sides read, behavior certain; B = high; C = medium).

Findings marked **FIXED (2026-11-06)** were re-verified against the current branch source before marking; the original audit text is otherwise verbatim.

---

Audit of robotocore (Rust) vs moto 5.2.5 — DynamoDB and IAM semantic divergences. All moto paths are under `.venv/lib/python3.13/site-packages/moto/`, Rust paths under `crates/`. Confidence: A = both sides read, behavior certain; B = high; C = medium (real-AWS verdict uncertain).

DYNAMODB

1. PutItem key validation: wrong error text and no key-type check.
   (a) PutItem (and the transact Put row).
   (b) moto `Table.put_item` raises `Missing the key {hash_attr} in the item` when the hash key is absent and the per-key text for a missing range key (models/table.py:627-638), and raises `InvalidAttributeTypeError` — "One or more parameter values were invalid: Type mismatch for key {name} expected: {S|N|B} actual: {type}" (models/table.py:643-654, exceptions.py:299-303) — when the key value's type differs from the schema's AttributeDefinition.
   (c) robotocore put_item answers 400 "The provided key element does not match the schema" for any unresolvable key (handler.rs:1364-1373) and has no key-type check at all: a `{"S":"5"}` value for an N hash key is accepted and stored (models.rs compute_key_from_attrs:354-376 does no type comparison).
   (d) Real AWS sends the "Missing the key …" text and rejects type-mismatched keys with the "Type mismatch for key" ValidationException — moto matches AWS; robotocore diverges from both.
   (e) A.

   **FIXED (2026-11-07, verified in source):** put-shaped writes carry moto's exact
   texts — `<Missing the key {name} in the item>` for absent keys and the
   `Type mismatch for key {name} expected: {E} actual: {A}` InvalidAttributeTypeError
   wording for a key value outside the schema type (put_shape_error in
   crates/dynamodb/src/handler.rs, wired into PutItem). Differential step
   `dynamodb.put_key_type_mismatch` + `put_missing_key` pin it.

2. Query KeyConditionExpression is not schema-validated.
   (a) Query.
   (b) moto parses the KCE against the table/index schema (responses.py:934-951 → parsing/key_condition_expression.py:215-316): a missing hash-key equality → MockValidationException "Query condition missed key schema element: {k}" (key_condition_expression.py:250-254); a hash key used with a non-equality operator or a non-key attribute → "Query key condition not supported" (261-257, 280); a Query with neither KeyConditionExpression nor KeyConditions → "Either KeyConditions or QueryFilter should be present" (responses.py:967-970).
   (c) robotocore's query (handler.rs:1616-1648) feeds the KCE to the generic condition evaluator per item (handler.rs:1795-1805): a KCE on a non-key attribute silently acts as a filter, a KCE missing the hash key silently returns an empty page, and a Query with no KCE succeeds as a full-table scan. Only the key-value type check was added (handler.rs:1728-1761).
   (d) Real AWS rejects all three shapes with ValidationException — robotocore diverges from both moto and AWS.
   (e) A.

   **FIXED in part (2026-11-06, verified in source):** the key-value type check is in place — the Query path calls `expr_engine::key_condition_type_mismatch` against the table/index schema types and answers 400 ValidationException "Condition parameter type does not match schema type" (crates/dynamodb/src/handler.rs:1732-1761, expr.rs:1025). The rest of this finding is now FIXED (2026-11-08, verified in source): the KCE runs moto's validate_schema (key_condition_expression.py:215-316) — a missing hash key answers "Query condition missed key schema element: {k}"; a hash key with a non-equality operator answers "Query key condition not supported"; an unknown KCE attribute answers missed-element when a schema range key is missing and the generic shape otherwise; an empty-string hash key carries moto's KeyIsEmptyStringException text; and a Query with neither KeyConditionExpression nor legacy KeyConditions answers "Either KeyConditions or QueryFilter should be present" (query-only — Scan keeps scan semantics; the first cut leaked the check into Scan and broke the scan suite, caught in-process). Verified live via the differential corpus steps query_missing_hash_key / query_hash_non_equality / query_without_kce; 7 compat tests.

3. Query/Scan pagination: the 1 MB result-size limit is missing.
   (a) Query, Scan.
   (b) moto stops a page when `result_size + item.size() > RESULT_SIZE_LIMIT` (1,000,000 bytes) and returns LastEvaluatedKey (models/table.py:27, 930-935 for query, 1110-1116 for scan).
   (c) robotocore's run_scan_or_query paginates only on the Limit member (handler.rs:1870-1944); there is no size accounting, so a Query over large items returns everything in one response where moto (and AWS) return a truncated 1 MB page with LastEvaluatedKey.
   (d) Real AWS enforces the 1 MB per-response limit — robotocore diverges.
   (e) A.

   **FIXED (2026-11-08, verified live)**: run_scan_or_query accrues
   `result_size` over the raw pre-filter items (crate::models::item_size)
   and breaks when the next candidate would pass RESULT_SIZE_LIMIT = 1_000_000
   (crates/dynamodb/src/models.rs) — the oversize candidate is neither
   counted (ScannedCount stops before it) nor appended, and the
   LastEvaluatedKey stays on the last included result, matching moto's
   table.py:930-935/1111 byte-for-byte on the 150 kB corpus (6-item cut,
   ESK resume, filtered-out accrual, COUNT scans). One deliberate
   divergence inherited from the item-size work stands: Number values are
   sized with AWS's ceil(digits/2)+1 formula (moto #10176) instead of
   moto's len(str()) — only a page boundary straddling a
   number-sided-overrun item can cut one position differently.

4. Empty-string keys, empty sets and NULL-typed GSI keys are accepted on writes.
   (a) PutItem, BatchWriteItem, TransactWriteItems Put/Update.
   (b) moto rejects: empty S/B key values (validate_put_has_empty_keys, responses.py:93-131, called at 680-682 and in batch 745-749 / transact 1499-1513), empty SS/NS sets anywhere in the item (validate_put_has_empty_attrs, responses.py:134-146), and a GSI key attribute set to `{"NULL": true}` (responses.py:154-161) — all MockValidationException.
   (c) robotocore's write-path validation (handler.rs check_item_size:1135-1155 and validate_attribute_values:1196-1246) checks number/NULL shape only; an empty-string partition key, an `{"SS": []}` and a NULL GSI key are all stored with 200.
   (d) Real AWS rejects empty-string key values and empty sets (empty sets are unrepresentable in DynamoDB) — robotocore diverges from both.
   (e) A.

   **FIXED (2026-11-07, verified in source):** empty string/binary key values, empty
   NS/SS sets outside the keys, and NULL-typed index keys are rejected with moto's
   exact texts (responses.py:93-161 mirrored, including moto's hardcoded
   "Expected: S" in the NULL-index-key message). Differential steps
   `put_empty_string_key`, `put_empty_set`.

5. Unresolved `#name` placeholders: wrong error, and writes an attribute literally named `#x`.
   (a) ConditionExpression and UpdateExpression on PutItem/UpdateItem/DeleteItem (the Query/Scan pre-pass already handles its own case).
   (b) moto's condition parser resolves `#names` by strict dict access (comparisons.py:474-475) — an unresolved name crashes the request (unhandled KeyError); in update expressions the validator raises ExpressionAttributeNameNotDefined "An expression attribute name used in the document path is not defined; attribute name: #x" (parsing/validators.py:108-112), wire type ValidationException.
   (c) robotocore's resolve_path silently falls back to the raw token (expr.rs:146-151: `names.get(...).unwrap_or_else(|| head.to_string())`). In a ConditionExpression this usually evaluates false and the caller gets ConditionalCheckFailedException (handler.rs:1288-1349) instead of a ValidationException; in an UpdateExpression `SET #nope = :v` SUCCEEDS and writes an attribute literally named `#nope`. The `unbound_name_token` guard (handler.rs:1037-1067) is applied only in query/scan, not on the item-op paths.
   (d) Real AWS returns ValidationException for both shapes — robotocore diverges.
   (e) A.

   **FIXED (2026-11-07, verified in source):** unresolved `#name`s on the item ops
   answer 400 ValidationException with moto's ExpressionAttributeNameNotDefined
   text (the unbound_name_token pre-pass applied to UpdateExpression and to the
   condition paths via eval_condition_on). moto 500s on the update shape
   (uncaught KeyError) — `dynamodb.update_unresolved_name` ledgered twin-preferred.

6. ADD to an existing attribute of the wrong type silently overwrites it.
   (a) UpdateItem ADD (and the transact Update row).
   (b) moto AddExecutor raises IncorrectDataType "An operand in the update expression has an incorrect data type" when the target is neither a number nor a same-kind set (parsing/executors.py:269-270, 244-246).
   (c) robotocore's add_to_attr falls through to `_ => val.clone()` (expr.rs:1628-1656): `ADD string_attr :number` replaces the attribute with the new value and returns 200.
   (d) Real AWS returns a ValidationException — robotocore diverges from both.
   (e) A.

   **FIXED (2026-11-07, verified in source):** ADD enforces moto's operand typing
   (executors.py:220-272): a non-set/non-number operand — lists included — raises
   IncorrectDataType's "An operand in the update expression has an incorrect data
   type"; the old twin overwrote the attribute (and extended lists). Differential
   step `add_onto_string_attr`.

7. DELETE that empties a set leaves an empty set behind; ADD of an empty set is accepted.
   (a) UpdateItem DELETE / ADD on SS/NS/BS.
   (b) moto removes the attribute entirely when the last element is deleted ("DynamoDB does not support empty sets", executors.py:177-192) and raises "An string set may not be empty" for an empty ADD operand (executors.py:228-231).
   (c) robotocore keeps `{"SS": []}` in the stored item (delete_from_attr expr.rs:1661-1684 always returns Some of the trimmed array; nothing deletes the attribute), and merge_sets/add_to_attr accept empty operands (no empty check anywhere in apply_update).
   (d) Real AWS removes the emptied attribute and rejects empty sets — robotocore diverges from both; the leftover empty set then leaks into GetItem/Scan responses.
   (e) A.

   **FIXED (2026-11-07, verified in source):** DELETE that empties a set removes the
   attribute entirely (moto executors.py:177-181) and an empty ADD operand is
   rejected with moto's wording; the emptied attribute is consumed from the
   stored item. Differential step `delete_set_to_empty_removes_attr` pins it.

8. Set (SS/NS/BS) equality is order- and format-sensitive.
   (a) All condition/filter evaluation, ExclusiveStartKey matching, ADD/DELETE element dedup.
   (b) moto compares DynamoType via `cast_value`, which converts sets to python sets of Decimal/str (models/dynamo_type.py:81-82, 183-188): `{"SS":["a","b"]}` == `{"SS":["b","a"]}` and `{"NS":["1"]}` == `{"NS":["1.0"]}` are both true.
   (c) robotocore's attr_eq for (SS,SS)/(NS,NS)/(BS,BS) falls to the generic `a == b` JSON comparison (expr.rs:467-482) — order- and string-format-sensitive; the same raw compare governs merge_sets dedup (expr.rs:1687-1707), so ADD of "1" onto a set holding "1.0" duplicates the element, and a filter `ss = :v` with reordered elements misses.
   (d) Real AWS treats sets as unordered with numeric-equal elements — robotocore diverges from both.
   (e) A.

   **FIXED in part (2026-11-07, verified in source):** whole-value set equality in
   conditions/filters is now unordered and numeric-equal (set_eq); ADD/DELETE
   element dedup prefers real AWS's numeric semantics for NS (moto's list
   membership is string-sensitive on storage — a live moto probe stores
   ['1','1.0','2'] and its DELETE exposes the 1.0 member — while its read path
   canonicalizes NS through cast_value Decimal sets, so wire reads show
   ['1','2']; the audit's (b)/(d) overstated moto's dedup. Ledgered
   twin-preferred when a differential step covers the op).

9. ConsumedCapacity is never returned on single-item/read operations.
   (a) PutItem, GetItem, DeleteItem, UpdateItem, Query, Scan, BatchGetItem.
   (b) moto returns ConsumedCapacity when ReturnConsumedCapacity is TOTAL/INDEXES for PutItem/GetItem/DeleteItem/Query/Scan (the include_consumed_capacity decorator, responses.py:47-90), always for UpdateItem (`{"TableName", "CapacityUnits": 0.5}`, responses.py:1316) and DeleteItem ("ConsumedCapacityUnits": 0.5, responses.py:1216), and always for BatchGetItem with per-table `CapacityUnits: len(keys)` (responses.py:829-833, 893-895).
   (c) robotocore renders none of these — PutItem/GetItem/Query/Scan/DeleteItem/UpdateItem bodies never contain ConsumedCapacity (e.g. get_item handler.rs:1415-1441), and batch_get_item returns only `{"Responses", "UnprocessedKeys"}` (handler.rs:2202-2205). Only TransactGet/TransactWrite implement the member.
   (d) Real AWS returns ConsumedCapacity when TOTAL/INDEXES is requested — robotocore diverges (moto's UpdateItem/DeleteItem always-on rendering is moto-specific).
   (e) A.

   **FIXED in part (2026-11-08, verified in source):** the BatchGetItem
   leg of the ConsumedCapacity family is rendered always-on per moto
   (CapacityUnits = requested key count). Still open: the single-item
   / Query / Scan members (moto gates them on ReturnConsumedCapacity
   --- with moto's always-on quirks on UpdateItem/DeleteItem).

   **FIXED (2026-11-08, verified live)**: the single-item and read legs are
   done — a decorate_consumed_capacity helper mirrors the
   include_consumed_capacity decorator (responses.py:47-90): PutItem and
   DeleteItem at 1.0, GetItem at 0.5 (misses included), Query/Scan at 1.0,
   gated on ReturnConsumedCapacity TOTAL/INDEXES; INDEXES adds a Table
   member plus moto's quirk of parking any IndexName (GSI or LSI) under
   LocalSecondaryIndexes (verified live 2026-11-08 on query and scan).
   The decorator's enum validation now also runs before the handler body
   on the raw wire (botocore pre-validates client-side, so only raw
   requests reach moto's check).
   UpdateItem carries moto's always-0.5 quirk verbatim (responses.py:1316,
   ReturnConsumedCapacity notwithstanding); moto's internal
   "ConsumedCapacityUnits": 0.5 DeleteItem artifact (responses.py:1216)
   stays hidden from the wire, matching live moto. BatchGetItem (see the
   2026-11-07 note above) was already rendered.

   **FIXED in part (2026-11-06, verified in source):** one related table-description member is fixed — `describe_table_response` now renders `NumberOfDecreasesToday` in the provisioned-throughput block (crates/dynamodb/src/handler.rs:627). The missing ConsumedCapacity rendering described above remains open.

10. GSI membership requires only the index hash key.
    (a) Query, Scan with IndexName.
    (b) moto includes an item in an index's result set only when ALL of the index's schema attributes are present (query: models/table.py:877-882; scan: has_idx_items `issubset`, models/table.py:1019-1033).
    (c) robotocore retains candidates when the index HASH attribute is present (handler.rs:1789-1791: `candidates.retain(|(_, m)| m.contains_key(&hash_attr))`), so items missing the GSI range key flow into Query/Scan results (their sort/compare segments treat the missing range key as equal, handler.rs:1811-1847).
    (d) Real AWS indexes an item only if every key attribute of the index is present — robotocore diverges from both.
    (e) A.

   **FIXED (2026-11-07, verified in source):** GSI membership requires every
   index key attribute (candidates.retain over the full index key schema,
   mirroring models/table.py:877-882/1019-1033).

11. BatchGetItem: missing limits, duplicate-key check and response ConsumedCapacity.
    (a) BatchGetItem.
    (b) moto validates ≤100 keys per table and ≤100 across all tables (responses.py:835-851), rejects duplicate keys (856-859), moves >16 MB responses into UnprocessedKeys (882-891), and always returns ConsumedCapacity (893-895).
    (c) robotocore implements none of these (handler.rs:2128-2206): no 100-key limits, no duplicate check (numeric-equal or not), UnprocessedKeys always `{}`, no ConsumedCapacity; it does keep moto's "Validation Exception" text for schema-mismatched keys.
    (d) Real AWS enforces the 100-key caps and duplicate rejection and returns ConsumedCapacity when requested — robotocore diverges.
    (e) A.

   **FIXED (2026-11-08, verified in source):** BatchGetItem validates like
   moto (responses.py:835-861) — >100 keys per table with moto's exact
   path-constraint text, the 100-key across-all-tables cap ('Too many
   items requested for the BatchGetItem call'), duplicate-key rejection
   ('Provided list of item keys contains duplicates') — and renders the
   always-on per-table ConsumedCapacity over requested keys
   (responses.py:893-895). Corpus steps batch_get_consumed /
   batch_get_per_table_over_100 / batch_get_duplicates. Still open: the
   16 MB UnprocessedKeys overflow (needs >16 MB items to pin — the twin
   computes item sizes via the same item_size fn moto walks).

12. BatchWriteItem row containing both Put and Delete executes both (delete wins).
    (a) BatchWriteItem.
    (b) moto executes only the FIRST member of a request row (`list(table_request.keys())[0]`, responses.py:741-742) — a Put+Delete row performs just the put.
    (c) robotocore executes both members of a row, deletes last (handler.rs:2247-2347; the code comments acknowledge this as a recorded divergence from moto).
    (d) Real AWS rejects a row carrying both members client-side (boto3) / server-side ValidationException; the divergence is only observable against moto's quirk, and the twin already documents it.
    (e) A (divergence), C (practical impact).

13. GetItem silently ignores an invalid ProjectionExpression.
    (a) GetItem.
    (b) moto parses the projection via ProjectionExpressionParser and 400s on duplicate paths ("Two document paths overlap"), reserved keywords, names starting with a digit or containing whitespace (responses.py:245-281, 230-242).
    (c) robotocore's project_from_req swallows parse errors and returns the FULL item (handler.rs:2088-2100: `Err(_) => item.clone()`), so e.g. a duplicate-path projection silently over-returns data instead of failing.
    (d) Real AWS returns a ValidationException — robotocore diverges from both. (Query/Scan/TransactGet/BatchGet return the error correctly; only GetItem swallows it.)
    (e) A.

   **FIXED (2026-11-07, verified in source):** GetItem's invalid projection now
   propagates the ValidationException (overlapping paths get moto's own
   exceptions.py:316 contract text — moto itself never ran the validator on
   GetItem, ledgered `dynamodb.get_item_duplicate_projection` twin-preferred; the
   other half of the audit's reserved-keyword/digit/whitespace list remains for
   a later tranche).

14. UpdateTimeToLive: wrong error code and wrong validation trigger; DescribeTimeToLive drops AttributeName after disable.
    (a) UpdateTimeToLive, DescribeTimeToLive.
    (b) moto raises `JsonRESTError("InvalidParameterValue", "TimeToLiveSpecification does not contain Enabled and AttributeName")` whenever either member is missing — including Enabled=false with no AttributeName (models/__init__.py:578-594); DescribeTimeToLive returns the stored dict, so AttributeName persists after a disable (models/__init__.py:596-602).
    (c) robotocore answers ValidationException (not InvalidParameterValue) and only when `enabled && attr.is_empty()` (handler.rs:3767-3801); DescribeTimeToLive omits AttributeName whenever disabled (handler.rs:3743-3765).
    (d) Botocore's model declares ResourceInUse/ResourceNotFound/LimitExceeded for UpdateTimeToLive (no message text to rule on the code); AWS does keep TimeToLiveStatus DISABLED with the AttributeName omitted — robotocore's describe behavior matches AWS, its update error code does not match moto's.
    (e) A on the divergence, C on the AWS verdict.

15. Query/Scan ignore ConsistentRead on GSIs; ListTables pagination ignored.
    (a) Query/Scan with ConsistentRead=true on a GSI; ListTables with Limit/ExclusiveStartTableName.
    (b) moto raises MockValidationException "Consistent reads are not supported on global secondary indexes" (models/table.py:785-787, 1053-1056), and ListTables paginates with default Limit 100 and LastEvaluatedTableName (responses.py:326-338, models/__init__.py:136-158).
    (c) robotocore never reads ConsistentRead (query/scan handlers 1616-1675) and returns all table names unpaginated, sorted (handler.rs:137-142).
    (d) Real AWS rejects consistent reads on GSIs (ValidationException) and paginates ListTables — robotocore diverges on both.
    (e) A.

   **FIXED in part (2026-11-08, verified in source):** ListTables paginates
   with moto's semantics (Limit default 100, ExclusiveStartTableName skips
   past the named table, unknown start name starts at the end,
   LastEvaluatedTableName = the page's last entry when more tables
   follow). The ConsistentRead-on-GSI half was fixed earlier in this
   branch's tranche.

   **FIXED in part (2026-11-07, verified in source):** ConsistentRead=true on a GSI
   (Query or Scan) answers 400 ValidationException with moto's exact text. Still
   open: ListTables pagination.

IAM

16. Policy document validation is entirely absent.
    (a) PutUserPolicy, PutRolePolicy, PutGroupPolicy, CreatePolicy, CreatePolicyVersion, UpdateAssumeRolePolicy.
    (b) moto runs IAMPolicyDocumentValidator (or the trust-policy variant) on every one of these and raises ValidationError on malformed documents — missing Version/Statement, bad Effect/Action/Resource/Condition syntax (models.py:2023-2027 create_policy, 2226-2234 put_role_policy, 2844-2851 put_user_policy, 2379-2382 create_policy_version, 2220-2224 update_assume_role_policy; validator in iam/policy_validation.py).
    (c) robotocore stores the raw string unvalidated on every path (e.g. put_role_policy handler.rs:1190-1208 — the comment acknowledges the deferral; create_policy 1699-1731; create_policy_version 1832-1895; update_assume_role_policy 1081-1096). Garbage documents are accepted with 200 and then silently contribute nothing in Simulate*.
    (d) Real AWS validates policy syntax server-side (MalformedPolicyDocument/ValidationError) — robotocore diverges from both.
    (e) A.

    **FIXED (2026-11-08, verified live)**: crates/iam/src/policy_validation.rs
    ports the validator — moto's layer order (syntax → version → legacy
    parsing → Sid uniqueness → action existence → accumulated resource
    errors → action prefixes → flavor extras) with moto's verbatim
    messages; mounted at CreatePolicy/CreatePolicyVersion (validate
    BEFORE the duplicate-name and policy lookups, models.py:2031-2033,
    2382-2387) and PutUserPolicy/PutRolePolicy/PutGroupPolicy after the
    entity resolution (2849-2851, 2231, 2600-2602), UpdateAssumeRolePolicy
    after get_role with the trust flavor (2222-2225). Verified against a
    28-shape live moto matrix; the validator quirks port verbatim (an
    unknown Condition operator is legal on an EMPTY value; R+NR and
    Action+NotAction exclusivity; reverse-sorted resource lists; the
    legacy `resource[2]` third-character check for list members). Two
    recorded divergences from moto itself (kept twin-preferred): moto
    crashes HTTP 500 on a null Action list element (None.split), and the
    legacy Date-condition layer on raw JSON numbers: python `int()`
    REFUSES float values (1.5 → legacy parsing fail) while the port
    truncates f64s, and python's `int()` accepts underscore-style strings
    ("2_0") the port rejects — both unreachable through botocore's own
    client-side Date validators on the common paths.

17. DeleteRolePolicy is a silent 200 for a missing role or missing policy.
   **FIXED (2026-11-08, verified live)**: DeleteRolePolicy resolves the role
   ("Role {n} not found" — moto's role-family wording, models.py:2191) and the
   policy ("The role policy with name {p} cannot be found.", models.py:914-920)
   — NoSuchEntity 404 on both absences.

    (a) DeleteRolePolicy.
    (b) moto resolves the role (`Role {name} not found`) and then `Role.delete_policy` raises NotFound "The role policy with name {p} cannot be found." (models.py:2235-2238 → 914-922), wire code NoSuchEntity.
    (c) robotocore answers 200 unconditionally — a missing role and a missing policy name both no-op (handler.rs:1230-1239). The DeleteUserPolicy/DeleteGroupPolicy siblings do check; this is the one blind arm.
    (d) Real AWS raises NoSuchEntity 404 — robotocore diverges from both.
    (e) A.

18. DeleteRole succeeds while the role is still in an instance profile.
   **FIXED (2026-11-08, verified live)**: the instance-profile membership
   conflict now precedes the policy checks (moto models.py:2198-2212's
   three-raise shape — profile conflict first (409 DeleteConflict), then
   managed policies ("must detach all policies first."), then inline
   ("must delete policies first.")). The stored strip loop still exists and
   is now a no-op race: it runs only after the new conflict guard.

    (a) DeleteRole.
    (b) moto raises DeleteConflictException "Cannot delete entity, must remove roles from instance profile first." when any instance profile references the role (models.py:2199-2206), before the policy checks.
    (c) robotocore performs no instance-profile check and instead silently strips the role from every profile, then deletes (handler.rs:965-994).
    (d) Real AWS raises DeleteConflict (409) — robotocore diverges from both and additionally mutates profiles the caller never asked to change.
    (e) A.

19. DeleteInstanceProfile succeeds while roles are attached.
   **FIXED (2026-11-08, verified live)**: DeleteInstanceProfile with roles
   attached raises moto's DeleteConflictException before its own removal
   (models.py:2456-2464).

    (a) DeleteInstanceProfile.
    (b) moto raises DeleteConflictException "Cannot delete entity, must remove roles from instance profile first." when `instance_profile.roles` is non-empty (models.py:2456-2464).
    (c) robotocore's IamState::delete_instance_profile removes the profile and all side maps unconditionally (models.rs:577-584; handler.rs:3394-3405) — an attached role is silently orphaned.
    (d) Real AWS raises DeleteConflict — robotocore diverges from both.
    (e) A.

20. Permissions-boundary operations are silent no-ops on unknown entities and skip ARN validation.
   **FIXED (2026-11-08, verified live; deltas noted 2026-11-08 review)**:
   put/delete boundary operations resolve the entity ("Role {n} not found" /
   the user sibling) and validate the boundary arn with moto's EXACT
   policy_arn_regex partition shape (models.py:1894-1896 via 1959/2157 —
   partitions aws/aws-cn/aws-eusc/aws-iso/aws-iso-b/aws-us-gov, account
   (aws|[0-9]*), path :policy/.*); moto's FALSY-arn arm skips the regex and
   clears an existing entity's boundary (models.py:1959-1967), matching the
   twin now; create_role validates the boundary BEFORE the duplicate check
   (2157-2163) and stores it (finding 20's create half).
   PARTIALLY ALIGNED: moto has NO PutUserPermissionsBoundary at all (its raw
   wire 500s; the twin answers real-AWS — unverifiable against moto, ledgered
   below); the render Type member is now PermissionsBoundaryPolicy per moto
   models.py:736-742.

    (a) PutUserPermissionsBoundary, PutRolePermissionsBoundary, DeleteUser/RolePermissionsBoundary.
    (b) moto validates the boundary ARN shape up front (RESTError InvalidParameterValue "Value ({arn}) for parameter PermissionsBoundary is invalid.", models.py:1956-1963, and the same check in create_role:2159-2162) and resolves the user/role (NoSuchEntity otherwise, 1956-1971).
    (c) robotocore's put_boundary/delete_boundary answer 200 without checking that the user/role exists and without any ARN validation (handler.rs:4547-4578); create_role likewise accepts any PermissionsBoundary string... (create_role handler.rs:913-950 has no boundary handling at all — the CreateRole PermissionsBoundary parameter is dropped, so a boundary requested at role creation is lost).
    (d) Real AWS validates the ARN and the entity — robotocore diverges.
    (e) A on put/delete divergence; B on the CreateRole boundary drop (moto's create_role accepts a `permissions_boundary` parameter — robotocore's create_role never reads it).

21. List*Tags / ListUserPolicies / ListAttached{User,Role,Group}Policies return empty 200 for a nonexistent entity.

   **FIXED (2026-11-08, verified live)**: every List* in the family resolves
   the entity first with moto's sentences — users ("The user with name {n}
   cannot be found.", models.py:2840-2842), roles ("Role {n} not found",
   models.py:2191), groups get_group's short sentence ("Group {n} not
   found", models.py:2581 — delete_group/update_group keep their long
   form), and policy arns ("Policy {arn} not found") — ListUserTags/
   ListUserPolicies/ListAttachedUserPolicies/ListRoleTags/
   ListRolePolicies/ListAttachedRolePolicies/ListGroupPolicies/
   ListAttachedGroupPolicies/ListPolicyTags. (Excepted: ListAccessKeys
   already raised; botocore has no ListGroupTags client method.) The same
   pass closed the family's remaining silent-200 rows — ListSSHPublicKeys
   and ListSigningCertificates resolve the user (2938, 2769); the per-id
   ssh/cert ops raise ("The SSH Public Key with id {id} cannot be found",
   1464-1472; "The Certificate with id {id} cannot be found.", 2762-2767,
   2782-2786); AddUserToGroup resolves the user before the group
   (2818-2822) and RemoveUserFromGroup raises "User {n} not in group {n}"
   (2825-2829); DeleteGroupPolicy resolves the group and the policy.
   GetGroup's raise arm switched to get_group's short sentence (a
   pre-existing long-form drift the review caught).
    (a) ListUserTags, ListRoleTags, ListGroupTags, ListUserPolicies, ListAttachedUserPolicies (+ role/group flavors), ListPolicyTags.
    (b) moto resolves the entity first and raises NoSuchEntity ("The user with name … cannot be found", "Role {name} not found", "Policy {arn} not found") (models.py:2840-2842, 2304-2306, 2341+, 2836, 2056+, 2066+).
    (c) robotocore uses `.map(...).unwrap_or_default()` and renders an empty list with 200 (handler.rs:714-722 list_user_tags, 863-871 list_user_policies, 779-792 list_attached_user_policies, 1122-1130 list_role_tags, 1175-1188, 2081-2089). (ListAccessKeys, by contrast, does raise.)
    (d) Real AWS raises NoSuchEntity 404 — robotocore diverges from both.
    (e) A.

22. Token/ID semantics: CreatePolicyVersion's SetAsDefault also rewrites the stored policy document but DeletePolicyVersion id-format validation is absent.
    (a) CreatePolicyVersion / SetDefaultPolicyVersion / DeletePolicyVersion.
    (b) moto SetDefaultPolicyVersion validates the version id against `v[1-9][0-9]*(\.[A-Za-z0-9-]*)?` and raises ValidationError before lookup (models.py:2106-2110); version ids come from a monotonic per-policy counter and the 5-version cap raises LimitExceeded (2379-2398).
    (c) robotocore mirrors the cap and the monotonic counter (handler.rs:1848-1894 — including moto's quirk of syncing policy.policy_document only on default changes) but performs no version-id regex validation on SetDefaultPolicyVersion (handler.rs:1970-2017) — a malformed id falls through to NoSuchEntity instead of ValidationError.
    (d) Real AWS validates the id pattern client-side in boto3; server-side verdict not determinable from the model.
    (e) A on the divergence, C on the AWS verdict.

    **FIXED (2026-11-08, verified live)**: set_default_policy_version runs
    the start-anchored `v[1-9][0-9]*(\.[A-Za-z0-9-]*)?` gate BEFORE the
    policy lookup with moto's exact ValidationError sentence — note
    re.match is NOT fullmatch, so "v1junk" passes the gate and falls
    through to the not-found arm — and the not-found message is moto's
    "Policy {arn} version {version_id} does not exist or is not
    attachable." (models.py:2119-2121, live-probed). DeletePolicyVersion's
    absent id-format gate stays open in the ledger (finding 23's queue).

23. Residual IAM wording/order drift flagged by the finding-21 review (2026-11-08), queued for the policy-validation tranche.

    **FIXED in part (2026-11-08, verified live)**: the POLICY row is done —
    all eight policy-arn ghost raises (get/delete policy,
    get/list/delete policy version, create_policy_version,
    set_default_policy_version, list_entities_for_policy) now raise
    moto's short "Policy {arn} not found" (live-probed on all seven
    client-visible arms; the wording pins pin it). Also recorded live:
    DeletePolicyVersion has NO version-id regex gate in moto
    (models.py:2415-2429 strikes finding 22's "absent validation" — a
    ghost version answers the generic "Policy not found", JunkDb ids
    included, and the default version raises DeleteConflict 409) — the
    twin already matched every row; pins added, finding 22 is now
    FULLY fixed (modulo the DeleteConflict status note).

    **Still open**: instance profiles ("Instance profile {n} not
    found"), server-certificate trailing period, login-profile user
    resolution first, list_entities_for_policy's strictness (opposite
    direction: moto 200 empty), and the inline-policy get_-family
    (c2) rows.
    (a) Policy entities, instance profiles, server certificates, login profiles, entity updates.
    (b) moto: get/delete_policy + get/create/set_default/list/delete policy version raise "Policy {arn} not found" (vs the twin's "The policy with ARN {arn} cannot be found." — list_policy_tags already carries moto's text); get/tag_instance_profile raise "Instance profile {n} not found" (vs "… cannot be found"); get/update/tag_server_certificate append a trailing period ("The Server Certificate with name {n} cannot be found.", models.py:2543); get/delete_login_profile resolve the user FIRST ("The user with name {n} cannot be found.") before the profile check; list_entities_for_policy returns 200 empty for a nonexistent policy where the twin is stricter (404 NoSuchEntity).
    (c) UpdateGroup semantics: moto errors when the rename target already exists BEFORE resolving the group (EntityAlreadyExists wins over NoSuchEntity, models.py:2627-2647) and silently ignores a NewPath-only update when NewGroupName is absent; the twin resolves the group first and applies the path change.
    (c2) The inline inline-policy ghost family raises the SHORT "Policy {name} not found" on both user and group flavors (Group/User.get_policy / delete_policy, models.py:1306-1309, 1325-1327, 1390-1393) where the twin's get_user_policy/get_group_policy still carry "The {user,group} policy with name {n} cannot be found." — DeleteGroupPolicy was switched to the short sentence in the #443 review pass; the get_-family rows stay open queue for this tranche.
    (d) Real AWS unknown for each (no moto-oracle conflict documented yet beyond the above).
    (e) A (wording) / C (AWS verdict where unprobed); fix with the finding-16 policy-document validator.

Skipped per instructions: moto's Simulate*/credential-report implementations are stubs or absent (moto has no Simulate handlers in iam/responses.py), so robotocore's full simulator has no moto branch to compare; moto's TransactWriteItems per-row error capture for non-MockValidationException exceptions (models/__init__.py:708-710 turning e.g. ItemSizeTooLarge into cancellation reasons) is unreachable in practice because those exception types are MockValidationException subclasses (exceptions.py:162-175) that abort — robotocore's immediate-return matches. Numeric precision: robotocore's exact decimal arithmetic (expr.rs dec_add/dec_cmp/attr_eq) is observably identical to moto's Decimal-based evaluation except in one internal inconsistency — the Query/Scan SORT comparator uses f64 (handler.rs dyn_cmp:1956-1988), so sort order can differ from both moto (Decimal, table.py:901) and from robotocore's own equality for N keys beyond 2^53; folded into finding 10's neighborhood but worth its own fix.

Notably NOT divergent (verified equal): moto's BatchWriteItem duplicate-detection quirk (table-blind whole-item dumps) vs robotocore's table-scoped keys — robotocore matches real AWS, divergence documented in source; transact unicity, cancellation-reason wire shape, TooManyTransactions/TransactGet caps with exact message text, warm-throughput defaults, delete-table DELETING description, UpdateTable member ordering, and item-size formulas (AWS's ceil(digits/2)+1 per moto #10176) all match.
