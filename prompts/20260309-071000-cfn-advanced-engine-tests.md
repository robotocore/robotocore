---
session: "7092342e"
timestamp: "2026-03-09T07:10:00Z"
model: claude-opus-4-6
---

## Human

Add CFN advanced engine tests: stack updates, parameters, cross-stack refs, rollback, conditions, intrinsic functions, nested stacks.

## Assistant

### What was done
Created/rewrote 7 test files in `tests/iac/cloudformation/advanced/` exercising the native CFN engine. All 19 tests pass against the running server on port 4566.

- **test_stack_updates.py** (3 tests): add resource via update, remove resource via update, update with invalid resource triggers rollback
- **test_parameters.py** (3 tests): default params, custom params, AllowedValues validation rejects invalid values
- **test_cross_stack_refs.py** (2 tests): Export/ImportValue resolution across stacks, ListExports includes stack exports
- **test_rollback.py** (2 tests): invalid resource causes ROLLBACK_COMPLETE with cleanup, entirely invalid template fails
- **test_conditions.py** (2 tests): Fn::Equals condition true creates resource, condition false skips resource and output
- **test_intrinsic_functions.py** (5 tests): Fn::Sub, Fn::Join, Fn::Select, Ref, Fn::GetAtt all resolve correctly
- **test_nested_stacks.py** (2 tests): child resources created from S3 template URL, parent delete cascades to child

### Bugs found
None - all CFN engine features tested (updates, parameters, conditions, intrinsics, cross-stack refs, rollback, nested stacks) work correctly.

### Files created
- `tests/iac/cloudformation/advanced/__init__.py`
- `tests/iac/cloudformation/advanced/test_stack_updates.py`
- `tests/iac/cloudformation/advanced/test_parameters.py`
- `tests/iac/cloudformation/advanced/test_cross_stack_refs.py`
- `tests/iac/cloudformation/advanced/test_rollback.py`
- `tests/iac/cloudformation/advanced/test_conditions.py`
- `tests/iac/cloudformation/advanced/test_intrinsic_functions.py`
- `tests/iac/cloudformation/advanced/test_nested_stacks.py`
