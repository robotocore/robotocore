# Moto Operations Pilot — Progress Tracker

Run this to see where things stand at any point:
```bash
make start
uv run pytest tests/moto_impl/test_connect_lifecycle.py tests/moto_impl/test_iot_lifecycle.py tests/moto_impl/test_glue_lifecycle.py -q --tb=no
```

## Baseline (2026-03-17)
- Connect: 48 passed, 10 failed
- IoT:     54 passed, 12 failed
- Glue:    52 passed, 26 failed
- **Total: 154 passed, 48 failed**

## Goal
All 202 tests passing. Then run the same tool on more services.

## Failure Categories

Fixes fall into three buckets:

### A) Test fix — ParamValidationError (client-side, never reaches server)
These tests have wrong params; boto3 rejects them before sending. Fix: correct the params in the test file.

### B) Server fix — 500 crash (KeyError in Moto)
These hit the server but crash with an unhandled exception. Fix: implement the missing op in `vendor/moto/`.

### C) Server fix — 501 NotImplemented
The operation isn't implemented in Moto at all. Fix: add to `vendor/moto/moto/{service}/models.py` and `responses.py`.

## Remaining Failures (updated as we go)

### Connect (10 failing)
| Test | Category | Fix |
|------|----------|-----|
| test_contact_flow_module_lifecycle | B (500 crash) | KeyError in Moto |
| test_contact_flow_module_alias_lifecycle | B (500 crash) | KeyError 'Name' |
| test_data_table_lifecycle | B (500 crash) | KeyError in Moto |
| test_data_table_attribute_lifecycle | B (500 crash) | KeyError 'AttributeName' |
| test_evaluation_form_lifecycle | B (500 crash) | KeyError in Moto |
| test_predefined_attribute_lifecycle | B (500 crash) | KeyError 'Values' |
| test_instance_not_found | D (DID NOT RAISE) | Missing validation |
| test_task_template_lifecycle | A (ParamValidationError) | Fix test params |
| test_traffic_distribution_group_lifecycle | A (ParamValidationError) | Fix test params |
| test_view_lifecycle | A (ParamValidationError) | Fix test params |

### IoT (12 failing)
| Test | Category | Fix |
|------|----------|-----|
| test_audit_mitigation_actions_task_lifecycle | C (501) | Not implemented |
| test_audit_suppression_lifecycle | A (ParamValidationError) | Fix test params |
| test_audit_suppression_not_found | A (ParamValidationError) | Fix test params |
| test_ca_certificate_not_found | A (ParamValidationError) | Fix test params |
| test_certificate_not_found | A (ParamValidationError) | Fix test params |
| test_detect_mitigation_actions_task_lifecycle | C (501) | Not implemented |
| test_package_version_lifecycle | C (501) | Not implemented |
| test_policy_version_lifecycle | C (501) | Not implemented |
| test_provisioning_template_version_lifecycle | C (501) | Not implemented |
| test_thing_registration_task_lifecycle | C (501) | Not implemented |
| test_topic_rule_lifecycle | A (ParamValidationError) | Fix test params |
| test_topic_rule_destination_lifecycle | A (ParamValidationError) | Fix test params |

### Glue (26 failing)
| Test | Category | Fix |
|------|----------|-----|
| test_blueprint_run_lifecycle | C (501) | Not implemented |
| test_catalog_lifecycle | A (ParamValidationError) | Fix test params |
| test_classifier_lifecycle | A (ParamValidationError) | Fix test params |
| test_column_statistics_task_run_lifecycle | C (501) | Not implemented |
| test_column_statistics_task_settings_lifecycle | C (501) | Not implemented |
| test_data_catalog_encryption_settings_not_found | D (DID NOT RAISE) | Missing validation |
| test_data_quality_rule_recommendation_run_lifecycle | C (501) | Not implemented |
| test_data_quality_ruleset_evaluation_run_lifecycle | C (501) | Not implemented |
| test_job_run_lifecycle | A (ParamValidationError) | Fix test params |
| test_materialized_view_refresh_task_run_lifecycle | C (501) | Not implemented |
| test_partition_lifecycle | A (ParamValidationError) | Fix test params |
| test_partition_not_found | A (ParamValidationError) | Fix test params |
| test_registry_lifecycle | C (501) | Not implemented |
| test_registry_not_found | C (501) | Not implemented |
| test_resource_policy_lifecycle | D (DID NOT RAISE) | Missing validation |
| test_resource_policy_not_found | D (DID NOT RAISE) | Wrong error code |
| test_schema_lifecycle | C (501) | Not implemented |
| test_schema_not_found | C (501) | Not implemented |
| test_schema_version_lifecycle | C (501) | Not implemented |
| test_schema_version_not_found | C (501) | Not implemented |
| test_session_lifecycle | C (501) | Not implemented |
| test_statement_lifecycle | C (501) | Not implemented |
| test_table_lifecycle | A (ParamValidationError) | Fix test params |
| test_user_defined_function_lifecycle | A (ParamValidationError) | Fix test params |
| test_workflow_run_lifecycle | C (501) | Not implemented |
| test_workflow_run_properties_lifecycle | C (501) | Not implemented |

## Moto Workflow
```bash
cd vendor/moto
git checkout -b fix/<service>-<noun>   # or add to existing fix branch
# edit moto/<service>/models.py + responses.py
git add -A && git commit -m "feat(<service>): implement <noun> ops"
git push jackdanger master
cd ../..
uv lock  # pin new commit
```
