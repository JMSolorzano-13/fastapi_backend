# FastAPI Backend — Architecture Analysis

**Date**: 2026-03-27  
**Analyzed By**: AI Senior Developer  
**Project**: Siigo Fiscal (solucioncp-backend)

---

## Executive Summary 

The `fastapi_backend` is the **API Gateway & Business Logic Core** for Siigo Fiscal, a Mexican fiscal compliance SaaS platform. It orchestrates:

- CFDI (digital invoices) synchronization from Mexico's SAT
- Multi-tenant company data management
- IVA/ISR tax calculations
- Asynchronous scraping via SQS + Lambda
- Stripe billing and Odoo CRM integration

**Critical Migration Context**: Recently migrated from AWS Chalice to FastAPI while maintaining 100% backward compatibility with the React frontend.

---

## Tech Stack

| Layer | Technology | Version | Purpose |
|-------|-----------|---------|---------|
| **Framework** | FastAPI | Latest | ASGI HTTP API (replaced Chalice) |
| **Python** | Python | 3.12 | Strict version requirement |
| **ASGI Server** | Uvicorn | Latest (standard) | Production server |
| **ORM** | SQLAlchemy | 1.x | Database abstraction |
| **Database** | PostgreSQL (Aurora RDS) | - | Primary data store |
| **Validation** | Pydantic | v2 | Request/response validation |
| **Async Queue** | AWS SQS | - | Event-driven processing (25+ queues) |
| **Auth** | AWS Cognito + B2C | - | User authentication (Siigo SSO) |
| **Payments** | Stripe | 11.6 | Subscription billing |
| **CRM** | Odoo | - | Via OdooRPC 0.10.1 |
| **Testing** | pytest + moto | Latest | Unit/integration tests |
| **Linter** | Ruff | Latest | Code quality (line-length=100) |
| **Type Checker** | mypy | Latest | Static type analysis (Python 3.12) |

---

## Architecture Patterns

### 1. Hexagonal Architecture (Clean Architecture Light)

```
┌─────────────────────────────────────────────────────────────┐
│                         Routers                             │
│  (HTTP Entry Points — FastAPI route handlers)               │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                      Dependencies                            │
│  (Dependency Injection — Session/User/Company resolution)    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                      Controllers                             │
│  (Business Logic — CRUD, search, resume, export)             │
│  ├── CommonController (base class with ORM patterns)         │
│  ├── CFDIController (core business domain)                   │
│  ├── CompanyController (certificate management)              │
│  └── UserController (auth, permissions)                      │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Domain Layer                              │
│  (Pure Business Logic — no framework dependencies)           │
│  ├── chalicelib/new/query/domain/ (SAT queries)              │
│  ├── chalicelib/new/cfdi_processor/domain/ (CFDI export)     │
│  ├── chalicelib/new/isr.py (tax calculations)                │
│  └── chalicelib/new/iva.py (VAT calculations)                │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   Infrastructure                             │
│  (External Services — DB, SQS, S3, Stripe, Odoo)             │
│  ├── chalicelib/schema/ (SQLAlchemy models)                  │
│  ├── chalicelib/new/shared/infra/ (SQS, S3 clients)          │
│  ├── chalicelib/new/stripe/infra/ (Stripe API)               │
│  └── chalicelib/new/ws_sat/infra/ (SAT web service)          │
└─────────────────────────────────────────────────────────────┘
```

**Key Principles**:
- **Ports & Adapters**: Domain layer defines interfaces (e.g., `CFDIRepository`), infrastructure implements them (`CFDIRepositorySA`)
- **DI at Boundaries**: FastAPI's `Depends()` resolves sessions, users, companies
- **Business Logic Isolation**: Controllers orchestrate, domain layer has zero framework imports

---

### 2. Multi-Tenant Architecture

**Three-Tier Data Isolation**:

```
┌───────────────────────────────────────────────────────────┐
│              Control Database (public schema)              │
│  ┌──────────┬─────────────┬────────────┬───────────────┐  │
│  │  users   │  companies  │ workspaces │  permissions  │  │
│  └──────────┴─────────────┴────────────┴───────────────┘  │
└───────────────────────────────────────────────────────────┘
                              │
                              ├─ company_id: 1 → tenant_abc123
                              ├─ company_id: 2 → tenant_def456
                              └─ company_id: 3 → tenant_ghi789
                              
┌───────────────────────────────────────────────────────────┐
│         Tenant Database (per-company schemas)              │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ tenant_abc123:                                      │  │
│  │  - cfdis                                            │  │
│  │  - payments                                         │  │
│  │  - attachments                                      │  │
│  │  - sat_queries                                      │  │
│  └─────────────────────────────────────────────────────┘  │
│  ┌─────────────────────────────────────────────────────┐  │
│  │ tenant_def456: (isolated schema for company 2)     │  │
│  └─────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────┘
```

**Session Types**:
1. **Control Session** (`get_db_session`): Global data (users, companies, workspaces)
2. **Tenant Session** (`get_company_session`): Company-specific data (CFDIs, payments)

**Tenant Resolution (Priority Order)**:
1. Request body: `json_body["company_identifier"]`
2. Domain filter: `json_body["domain"][0][2]` (Odoo-style)
3. HTTP Header: `company_identifier`
4. Path parameter: `{company_identifier}` or `{cid}`

---

### 3. Event-Driven Architecture (EventBus + SQS)

**Pattern**: Publish/Subscribe with SQS-backed persistence

```
┌──────────────────────────────────────────────────────────┐
│                    EventBus (In-Memory)                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  EventType → List[EventHandler]                    │  │
│  │  ├── SAT_METADATA_REQUESTED → SQSHandler          │  │
│  │  ├── SAT_CFDIS_DOWNLOADED → SQSHandler            │  │
│  │  ├── USER_EXPORT_CREATED → SQSHandler             │  │
│  │  └── COMPANY_CREATED → OnCompanyCreateAutoSync    │  │
│  └────────────────────────────────────────────────────┘  │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│              SQS Queues (Async Workers)                   │
│  ┌────────────────────────────────────────────────────┐  │
│  │  SQS_SEND_QUERY_METADATA                          │  │
│  │  SQS_PROCESS_PACKAGE_METADATA                     │  │
│  │  SQS_PROCESS_PACKAGE_XML                          │  │
│  │  SQS_EXPORT                                        │  │
│  │  SQS_SCRAP_ORCHESTRATOR                           │  │
│  │  ... (25+ queues)                                  │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

**Example Flow (CFDI Export)**:

```
User clicks "Export" in frontend
    ↓
POST /api/CFDI/massive_export
    ↓
CFDIExporter.export_event() publishes:
  EventType.MASSIVE_EXPORT_CREATED
    ↓
SQSHandler sends message to SQS_MASSIVE_EXPORT
    ↓
Lambda worker polls queue, processes export
    ↓
Uploads XLSX to S3, updates CfdiExport record
    ↓
User polls GET /api/CFDI/get_exports
```

**Critical**: `suscribe_all_handlers()` MUST be called in `main.py:startup_event()` or `bus.publish()` will log `NO_HANDLERS`.

---

### 4. Dependency Injection (FastAPI Native)

**Two Parallel Chains (Read-Only vs Read-Write)**:

```python
# Read-Only Chain (GET/search/resume)
get_db_session (control DB, RO replica)
    ↓
get_current_user (decode access_token)
    ↓
get_company_identifier (extract from body/domain/header/path)
    ↓
get_company (fetch Company record)
    ↓
get_company_session (tenant DB, RO replica)

# Read-Write Chain (POST/PUT/DELETE)
get_db_session_rw (control DB, primary)
    ↓
get_current_user_rw
    ↓
get_company_identifier_rw
    ↓
get_company_rw
    ↓
get_company_session_rw (tenant DB, primary)
```

**Caching**: FastAPI caches dependencies **per-callable per-request**, so all deps in the same chain share a single DB session (matches Chalice's SuperBlueprint behavior).

**Rules**:
- Use `_rw` variants for ANY data modification
- Never mix RO and RW in same endpoint (breaks session caching)
- Choose chain at route definition, not runtime

---

## Data Flow Examples

### 1. CFDI Search

```
┌──────────────────────────────────────────────────────────┐
│ Frontend: axios.post("/api/CFDI/search", {               │
│   domain: [["company_identifier", "=", "uuid-123"]],     │
│   fuzzy_search: "ABC CORP",                              │
│   limit: 50,                                             │
│   offset: 0                                              │
│ })                                                        │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Router: routers/cfdi.py:search()                          │
│  ├── Inject json_body (from request)                     │
│  └── Inject company_session (tenant DB)                  │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Dependency: get_company_session                           │
│  ├── Extract company_identifier from domain[0][2]        │
│  ├── Validate user has Permission for Company            │
│  └── Create session for tenant schema                    │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Controller: CFDIController.search()                       │
│  ├── Build SQLAlchemy query with domain filters          │
│  ├── Apply fuzzy search (unaccent + ilike)               │
│  ├── Apply pagination (limit=50, offset=0)               │
│  └── Execute query, return (records, next_page, total)   │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Response: {                                               │
│   data: [...],                                           │
│   next_page: true,                                       │
│   total_records: 1234                                    │
│ }                                                         │
└──────────────────────────────────────────────────────────┘
```

### 2. IVA Export (Async)

```
┌──────────────────────────────────────────────────────────┐
│ Frontend: POST /api/CFDI/export_iva {                     │
│   period: "2024-01-01",                                  │
│   yearly: false,                                         │
│   iva: "Acreditable",                                    │
│   issued: true,                                          │
│   company_identifier: "uuid-123",                        │
│   export_data: {...}                                     │
│ }                                                         │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Router: routers/cfdi.py:export_iva()                      │
│  ├── Parse period, iva type, issued flag                 │
│  └── Create CFDIExporter instance                        │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Domain: CFDIExporter.publish_export()                     │
│  ├── Calculate IVA filter (IVAGetter.get_full_filter)    │
│  ├── Create CfdiExport record (state=PENDING)            │
│  ├── Publish event: EventType.USER_EXPORT_CREATED        │
│  └── Return export_identifier                            │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ EventBus: get_global_bus().publish()                      │
│  ├── Find handlers for USER_EXPORT_CREATED               │
│  └── Call SQSHandler.handle(event)                       │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Infra: SQSHandler sends message to SQS_EXPORT            │
│  ├── Serialize event to JSON                             │
│  └── sqs_client.send_message(queue_url, body)            │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Lambda Worker (separate codebase) polls SQS_EXPORT       │
│  ├── Fetch CFDI records matching IVA filter              │
│  ├── Generate XLSX with XLSXExporter                     │
│  ├── Upload to S3 bucket (envars.S3_EXPORT)              │
│  ├── Update CfdiExport record (state=COMPLETED, url=...) │
│  └── Delete SQS message                                  │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│ Frontend: Poll GET /api/CFDI/get_export_cfdi             │
│  ├── Check CfdiExport.state                              │
│  └── Download from S3 presigned URL when COMPLETED       │
└──────────────────────────────────────────────────────────┘
```

---

## Communication Patterns

### 1. Frontend ↔ Backend (REST API)

**Base URL**: Configured via `VITE_REACT_APP_BASE_URL` (env-specific)
- Local: `http://localhost:8000/api`
- Dev: `https://api-dev.siigocp.com/api`
- Prod: `https://api.siigocp.com/api`

**Authentication**:
- Header: `access_token: <cognito_jwt>`
- **NOT** `Authorization: Bearer <token>` (custom Chalice convention)

**Error Format (CRITICAL — Frontend Dependency)**:
```json
{
  "Code": "UnauthorizedError",
  "Message": "No company found"
}
```

Frontend always reads `error.response.data.Message`.

### 2. Backend ↔ Database (SQLAlchemy)

**Two Connection Pools**:
- **Primary (RW)**: `engine` → Aurora cluster writer endpoint
- **Replica (RO)**: `engine_ro` → Aurora cluster reader endpoint

**Connection Strategy**:
- `poolclass=NullPool` (Lambda optimized — no persistent connections)
- `pool_pre_ping=False` (not needed with NullPool)
- Statement timeout: 25s (endpoints), 5min (background tasks)

**Session Pattern**:
```python
with new_session(comment="api_call", read_only=True) as session:
    # Auto-rollback on exception, auto-commit on success
    session.query(...).all()
```

### 3. Backend ↔ SQS (Async Processing)

**25+ Queues** (see `chalicelib/new/config/infra/envars/sqs.py`):
- `SQS_SEND_QUERY_METADATA`: SAT metadata requests
- `SQS_PROCESS_PACKAGE_METADATA`: Metadata CSV parsing
- `SQS_PROCESS_PACKAGE_XML`: CFDI XML parsing
- `SQS_EXPORT`: User-triggered exports
- `SQS_MASSIVE_EXPORT`: Large batch exports
- `SQS_SCRAP_ORCHESTRATOR`: PDF/XML scraper coordination
- `SQS_ADD_DATA_SYNC`: Odoo/ADD integration
- `SQS_PASTO_CONFIG_WORKER`: Pasto worker setup
- etc.

**Pattern**:
```python
# 1. Define Event
@dataclass
class QueryReadyToDownload(DomainEvent):
    query_identifier: Identifier
    request_type: RequestType

# 2. Subscribe Handler
bus.subscribe(
    event_type=EventType.SAT_WS_QUERY_DOWNLOAD_READY,
    handler=SQSHandler(queue_url=envars.SQS_DOWNLOAD_QUERY),
)

# 3. Publish
bus.publish(EventType.SAT_WS_QUERY_DOWNLOAD_READY, event)
```

### 4. Backend ↔ External Services

**SAT Web Service (SOAP)**:
- Client: `chalicelib/new/ws_sat/infra/ws.py`
- Operations: Query send, verify, download
- Auth: FIEL certificates (stored in S3)

**Stripe (REST)**:
- Client: `stripe` Python SDK (v11.6)
- Operations: Subscription create/update, coupon apply, webhook handlers
- Config: `chalicelib/new/stripe/infra/stripe_config.py`

**Odoo (XML-RPC)**:
- Client: `OdooRPC` (v0.10.1)
- Operations: Company sync, metadata sync, license reset
- Connector: PastoCorp/ADD integration

---

## Domain Logic

### 1. CFDI Processing

**CFDI Types (`TipoDeComprobante`)**:
- `I` (Ingreso): Income invoice
- `E` (Egreso): Credit note / refund
- `P` (Pago): Payment complement
- `N` (Nomina): Payroll
- `T` (Traslado): Transfer

**Status (`Estatus`)**:
- `True`: Valid (timbrado by SAT)
- `False`: Cancelled (cancelado)

**Payment Method (`MetodoPago`)**:
- `PUE`: Pago en una sola exhibición (single payment)
- `PPD`: Pago en parcialidades o diferido (installments)

### 2. IVA Calculations

**Tax Rates**:
- Standard: 16%
- Border region: 8%
- Exempt: 0%

**Calculation Logic** (`chalicelib/new/iva.py`):
```python
class IVAGetter:
    def get_iva(self, period: date) -> dict:
        # Returns:
        # - BaseIVA16, BaseIVA8, BaseIVA0
        # - IVATrasladado16, IVATrasladado8
        # - IVARetenido
        # - TotalIVA (trasladado - retenido)
```

### 3. ISR Calculations

**Configurable Rates** (per company):
- 1.0%
- 1.25%
- 1.875%

**Storage**: `company.data["isr_percentage"]`

**Calculation Logic** (`chalicelib/new/isr.py`):
```python
class ISRGetter:
    def get_isr(self, period: date, company: Company) -> dict:
        # Returns:
        # - IngresosBrutos (gross income)
        # - Deducciones (deductions)
        # - BaseGravable (taxable base)
        # - ISRRetenido (withheld ISR)
        # - ISRPorPagar (ISR to pay)
```

### 4. EFOS Monitoring

**EFOS** = Empresas que Facturan Operaciones Simuladas (blacklisted taxpayers)

**Flow**:
1. SAT publishes EFOS list (monthly updates)
2. Backend scrapes SAT website for PDF
3. Parses RFC list, stores in `efos` table
4. Checks company's CFDIs for EFOS counterparties
5. Sends email alerts if matches found

---

## Security Model

### 1. Authentication

**Cognito User Pool + External B2C**:
- Primary: AWS Cognito User Pool (email/password)
- Secondary: Siigo SSO via OIDC (external B2C)

**Token Validation**:
```python
# dependencies/__init__.py:get_current_user
def validate_token(access_token: str, session: Session) -> User:
    # 1. Decode JWT (joserfc library)
    # 2. Verify signature against Cognito JWKS
    # 3. Check expiration
    # 4. Lookup user by cognito_sub
    # 5. Return User object
    return UserController.get_by_token(access_token, session=session)
```

### 2. Authorization

**Role-Based Access Control**:
```python
class Permission(Model):
    user_id = ForeignKey(User)
    company_id = ForeignKey(Company)
    role = Enum("OPERATOR", "PAYROLL")  # RoleEnum
```

**Permission Check**:
```python
# dependencies/__init__.py:_assert_user_can_access_company
def check_permission(user, company, session, role=Role.OPERATOR):
    count = session.query(Permission).filter(
        Permission.user_id == user.id,
        Permission.company_id == company.id,
        Permission.role == role.name,
    ).count()
    if not count:
        raise UnauthorizedError("No company found")
```

**Admin-Only Operations**:
```python
# chalicelib/new/config/infra/envars/control.py
ADMIN_EMAILS = ["admin1@example.com", "admin2@example.com"]

# dependencies/__init__.py:get_admin_user
def check_admin(user: User) -> User:
    if user.email not in ADMIN_EMAILS:
        raise ForbiddenError("Only admin users can perform this action")
    return user
```

### 3. Data Isolation

**Tenant Boundaries**:
- Company A's session CANNOT query Company B's tenant schema
- Permission checks on EVERY request with `company_identifier`
- Workspace-level isolation (user can only access companies in their workspace)

**Validation Flow**:
```
1. Extract company_identifier from request
2. Verify User has Permission for Company
3. Create tenant session for company's schema
4. Execute business logic
```

---

## Performance Optimizations

### 1. Database

**Read Replicas**:
- All GET/search/resume endpoints use `engine_ro` (Aurora reader)
- Reduces load on primary writer

**Connection Pooling**:
- `NullPool` for Lambda (no persistent connections)
- Avoids "too many connections" errors in serverless

**Statement Timeouts**:
- Endpoint: 25s (prevents frontend timeout)
- Background: 5min (exports, imports)
- Auto-cancellation via `statement_timeout` Postgres parameter

**Query Optimization**:
- Fuzzy search: `unaccent()` + `ilike` with `🍔` separator
- Auto-JOIN resolution from domain filters
- Pagination: `limit` + `offset` (default 50 records)
- Distinct queries for M2M relationships

### 2. Async Processing

**EventBus + SQS**:
- Offload expensive operations (exports, scraping) to Lambda workers
- FastAPI endpoint returns immediately with `export_identifier`
- User polls for completion

**Concurrent Execution**:
```python
# Example: Fetch IVA for all 12 months in parallel
@router.post("/get_iva_all")
async def get_iva_all(...):
    tasks = [
        asyncio.create_task(asyncio.to_thread(get_monthly_iva, month))
        for month in range(1, 13)
    ]
    results = await asyncio.gather(*tasks)
    return results
```

### 3. Caching

**FastAPI Dependency Caching**:
- All deps in same chain share a single session per request
- Reduces DB connection overhead

**S3 Presigned URLs**:
- Exports uploaded to S3 with 7-day expiration
- Offloads file serving from API

---

## Error Handling

### 1. Custom Exceptions

**Hierarchy** (`exceptions.py`):
```
HTTPException (FastAPI)
├── BadRequestError (400)
├── UnauthorizedError (401)
├── ForbiddenError (403)
├── NotFoundError (404)
├── MethodNotAllowedError (405)
└── ChaliceViewError (500)
```

### 2. Global Exception Handlers

**Pydantic Validation Errors → 400**:
```python
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    # Special case: missing access_token → 401
    for err in exc.errors():
        if err["loc"] == ("header", "access_token"):
            return JSONResponse(status_code=401, content={"Code": "UnauthorizedError", "Message": "Unauthorized"})
    # All other validation errors → 400
    return JSONResponse(status_code=400, content={"Code": "BadRequestError", "Message": "..."})
```

**Unhandled Exceptions → 500**:
```python
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    return JSONResponse(status_code=500, content={"Code": "InternalServerError", "Message": str(exc)})
```

### 3. Logging

**Structured JSON Logs**:
```python
log(
    Modules.SEARCH,
    ERROR,
    "DATABASE_TIMEOUT",
    {
        "query": "CFDI.search",
        "timeout_ms": 25000,
        "company_identifier": str(company_id),
    },
)
```

Output:
```json
{
  "timestamp": "2024-03-27T10:30:45.123Z",
  "level": "ERROR",
  "module": "SEARCH",
  "log_code": "DATABASE_TIMEOUT",
  "context": {
    "query": "CFDI.search",
    "timeout_ms": 25000,
    "company_identifier": "uuid-123"
  }
}
```

---

## Testing Strategy

### 1. Test Pyramid

```
        ┌───────────────┐
        │  End-to-End   │  (Minimal — focus on critical flows)
        └───────────────┘
            ┌─────────────────┐
            │  Integration    │  (Routes + DB + SQS mocks)
            └─────────────────┘
                ┌─────────────────────┐
                │      Unit           │  (Controllers, domain logic)
                └─────────────────────┘
```

### 2. Fixtures

**Session Fixtures**:
```python
@pytest.fixture
def session():
    # Control DB session (users, companies, workspaces)
    engine = create_engine("postgresql://test:test@localhost/test_db")
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()

@pytest.fixture
def company_session(company):
    # Tenant DB session (CFDIs, payments)
    tenant_url = company.tenant_db_url_with_schema
    engine = create_engine(tenant_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()
```

### 3. Markers

**Custom Markers** (`pyproject.toml`):
```python
markers = [
    "infra: real infrastructure related tests",
    "slow: slow tests",
    "to_solve: Errors to be solved",
    "third_party: tests that depends on third party services",
]

# Usage
@pytest.mark.slow
@pytest.mark.third_party
def test_sat_query_integration():
    ...

# Run
pytest -m "not slow and not third_party"
```

### 4. Coverage

**Target**: 80%+ coverage on core controllers and domain logic

**Command**:
```bash
pytest --cov=chalicelib --cov-report=html --cov-report=term
```

---

## Constraints & Limitations

### 1. Backward Compatibility (CRITICAL)

**Frontend Dependency**:
- All route paths MUST match Chalice originals
- Error response format MUST be `{"Code": "...", "Message": "..."}`
- Header name MUST be `access_token` (not `Authorization`)

**Breaking Changes = Production Outage**

### 2. Database

**Aurora Limitations**:
- Max connections: ~500 (shared across all Lambdas)
- Solution: `NullPool` (no persistent connections)

**Tenant Schemas**:
- Each company has a separate schema (not database)
- Migration complexity: Alembic must run on all tenant schemas
- Backup complexity: Need schema-level backups

### 3. SQS

**Queue Limits**:
- Max message size: 256 KB
- Max retention: 14 days
- Visibility timeout: 30s (must process faster or extend)

**Dead Letter Queues (DLQ)**:
- Configured for all queues (3 retries → DLQ)
- Manual inspection required for failed messages

### 4. Lambda

**Execution Time**:
- API Gateway timeout: 29s (hard limit)
- FastAPI endpoint timeout: 25s (leaves 4s buffer)
- Long-running operations MUST use async (SQS)

**Cold Starts**:
- Python 3.12 + FastAPI: ~1-2s cold start
- Mitigation: Provisioned concurrency (expensive)

---

## Deployment & CI/CD

### 1. Infrastructure

**AWS Region**: `us-east-1` (all resources)

**Services**:
- Lambda (FastAPI app via Mangum adapter)
- API Gateway (HTTP API, stage: `/api`)
- Aurora PostgreSQL (primary + read replica)
- S3 (certs, exports, attachments, XMLs)
- SQS (25+ queues)
- CloudWatch (logs, metrics)
- Secrets Manager (DB credentials, API keys)
- SSM Parameter Store (config values)

### 2. CI/CD Pipeline

**CodePipeline + CodeBuild**:
1. Source: GitHub (CodeStar connection)
2. Build: `buildspec.yml` (poetry install, pytest, ruff, mypy)
3. Deploy: AWS SAM / CloudFormation (Lambda + API Gateway + SQS)

**Environments**:
- `sgdev`: Development (auto-deploy on push to `develop`)
- `sgstg`: Staging (manual approval)
- `sgprod`: Production (manual approval + PECP protocol)

### 3. Environment Variables

**SSM Parameter Store**:
- `/siigocp/sgdev/DB_HOST`
- `/siigocp/sgdev/DB_PASSWORD`
- `/siigocp/sgdev/STRIPE_SECRET_KEY`
- etc.

**Secrets Manager**:
- `siigocp/sgprod/db-credentials`
- `siigocp/sgprod/stripe-api-key`

---

## Monitoring & Observability

### 1. Logging

**CloudWatch Logs**:
- Log Group: `/aws/lambda/siigocp-backend-{env}`
- Retention: 30 days (dev), 90 days (prod)
- Format: Structured JSON (easy to query)

**Key Metrics**:
- `STATEMENT_TIMEOUT`: DB queries exceeding 25s
- `NO_HANDLERS`: EventBus missing subscribers
- `LOG_IN_LIMIT`: Domain filters with >100 values (performance risk)

### 2. Metrics

**CloudWatch Metrics**:
- Lambda duration, invocations, errors, throttles
- API Gateway 4xx, 5xx, latency
- SQS messages sent, received, deleted, in-flight
- Aurora connections, CPU, memory, disk

**Custom Metrics** (via `log()` structured data):
- CFDI search latency
- Export success/failure rates
- SAT query retry counts

### 3. Alarms

**Critical Alarms**:
- Lambda errors > 5% (5min window)
- API Gateway 5xx > 2% (5min window)
- Aurora CPU > 80% (10min window)
- SQS DLQ message count > 0

**Notifications**: SNS → Email/Slack

---

## Open Questions & Future Work

### 1. Rate Limiting

**Current State**: No rate limiting (relies on API Gateway throttling)

**Recommendation**: Implement per-user rate limiting (e.g., 100 req/min) via middleware or AWS WAF.

### 2. Caching

**Current State**: No application-level caching (DB queries on every request)

**Recommendation**: 
- Redis for session caching (user, company, permissions)
- S3 for CFDI XML caching (reduce SAT queries)

### 3. GraphQL Migration

**Current State**: REST API with 100+ endpoints

**Recommendation**: Consider GraphQL for:
- Reducing over-fetching (frontend often requests nested data)
- Real-time subscriptions (export status updates)
- Type-safe schema (aligns with Pydantic models)

### 4. Database Sharding

**Current State**: All tenant schemas in single Aurora cluster

**Recommendation**: When >1000 companies, consider:
- Horizontal sharding (companies 1-500 → DB1, 501-1000 → DB2)
- Geographic sharding (Mexico → DB1, USA → DB2)

---

## Conclusion

The FastAPI backend is a **production-grade, event-driven API Gateway** serving a Mexican fiscal compliance SaaS. Key strengths:

1. **Multi-Tenant Isolation**: Per-company schemas with strict permission checks
2. **Async Processing**: SQS-backed EventBus for scalable background tasks
3. **Backward Compatibility**: Seamless migration from Chalice (zero frontend changes)
4. **Type Safety**: Full type hints + Pydantic validation
5. **Performance**: Read replicas, async exports, fuzzy search optimization

**Critical Success Factor**: Maintain Chalice compatibility (error format, routes, headers) while modernizing to FastAPI patterns.

**Next Steps**: Add rate limiting, caching, and consider GraphQL for complex queries.

---

**Document Owner**: AI Senior Developer  
**Last Updated**: 2026-03-27  
**Review Cadence**: Quarterly (or after major architectural changes)
