# Skill: Explore Vendor Code (Moto / LocalStack)

Use this skill when you need to understand how Moto or LocalStack implements a specific AWS service or feature.

## Moto Code Navigation

Moto's code is in `vendor/moto/moto/`. Each service follows a consistent pattern:

- `moto/{service}/__init__.py` — Exports `mock_{service}` decorator
- `moto/{service}/models.py` — **Start here.** Contains the backend class with all business logic
- `moto/{service}/responses.py` — HTTP request parsing, dispatches to models, formats responses
- `moto/{service}/urls.py` — URL patterns that route to this service
- `moto/{service}/exceptions.py` — Service-specific error responses

### Key Moto patterns:
- Backend classes inherit from `BaseBackend`
- `responses.py` classes inherit from `BaseResponse`
- Request dispatch uses method names matching the AWS Action parameter
- State is stored in Python dicts/lists on the backend instance

### Finding how an operation works:
```
1. grep for the operation name in responses.py (e.g., "create_bucket")
2. Follow the call into models.py
3. Check what state is created/modified
```

## LocalStack Code Navigation

LocalStack's code is in ``. Structure:

- `localstack/services/{service}/provider.py` — **Start here.** The main service provider
- `localstack/services/{service}/models.py` — Data models (if separate from provider)
- `localstack/services/{service}/stores.py` — State stores (per-account, per-region)
- `localstack/aws/api/{service}/` — Auto-generated API stubs from Smithy specs
- `localstack/aws/protocol/` — Protocol parsers and serializers
- `localstack/aws/handlers/` — Gateway handler chain

### Key LocalStack patterns:
- Providers inherit from the generated API class
- Methods are decorated with `@handler("OperationName")`
- `call_moto(context)` forwards to Moto's implementation
- State is stored in `AccountRegionBundle` stores
- Many providers mix native implementation with Moto fallbacks

### Finding how an operation works:
```
1. Open provider.py, search for the operation name
2. If it calls call_moto(), the logic is in Moto
3. If it has custom logic, that's the LocalStack extension
4. Check stores.py for state management
```

## Comparing implementations:
When implementing a service in robotocore, always check BOTH:
1. What does Moto do? (this is our foundation)
2. What does LocalStack add on top? (this is what we need to match)
