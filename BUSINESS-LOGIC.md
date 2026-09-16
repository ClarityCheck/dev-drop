# DROP suppression — business logic

What the pipeline is obliged to do, and the rules that decide it. Written for
whoever changes this Worker or the lookup API next; the architecture (Workflow,
KV, Hyperdrive, ClickHouse) is in the diagram, and the code comments explain
mechanism. This file is only about the rules.

The regulation: California's Delete Request and Opt-out Platform. A consumer
registers with the state, the state publishes hashed identifiers, and a
registered data broker must not process or serve data about them.

Specification: https://privacy.ca.gov/drop-for-data-brokers/technical-specifications/working-with-data/

---

## 0. The obligation, and the clock

Two duties, and they are separate. Confusing them is how a pipeline ends up
suppressing correctly and still being non-compliant.

**Suppress.** Do not process or serve data about a registered consumer. This is
continuous, and it is what everything below Rule 1 is about.

**Report.** Tell the state what we did about every work item we were given. This
is periodic, and it is Cron B's job. A work item nobody has looked at is still
reported — as `5 Not found` — so silence is an answer, and it is an answer we
may be wrong about.

The list must be downloaded **at least every 45 days**. The cycle is the unit of
work: download, match, erase, report. Cron B has to run *after* Cron C for its
answers to be true, and nothing yet enforces that ordering.

### The download is a delta, and that has a consequence

Per the specification, "after initial download and completed upload, future
downloads will include only new identifiers since previous list download". The
first download is the whole list; every later one carries only what is new.

**The complete set therefore exists only on our side, and DROP cannot re-serve
it.** R2 holds every raw ZIP verbatim and is the only source it could ever be
rebuilt from. Backing up R2 and testing the restore is a compliance control, not
housekeeping.

It also means KV must be **cumulative**. Clearing it and loading only the newest
delta would stop suppressing everyone registered in an earlier cycle — silently,
and with no way to tell from the outside. `clearKv` defaults to `false` for this
reason; it exists for rebuilding from the archive, not for normal runs.

### Consumers can be removed

DROP publishes a removals file alongside the four lists — `Id,Hash,ListType` —
when a consumer withdraws their request or the state revokes an entry. Two
things then have to happen, and they are different obligations:

- **stop suppressing them.** The hash is deleted from KV, so the gate releases
  them on the next request.
- **stop answering for them.** `revoked_at` is stamped on the
  `ca_drop_work_item` row, so they are no longer a work item we owe California
  a status for.

The row is kept rather than deleted. It is the record that we were once asked to
suppress this consumer and then released, and Cron B needs to tell "never given
to us" apart from "given and then withdrawn".

**Supabase is stamped before KV is deleted from**, and the order is load-bearing:

| Failure | Result |
|---|---|
| stamped, KV delete fails | we keep suppressing someone who withdrew — over-suppression, and the next run retries |
| KV deleted, stamp fails | we release them *and* still report them as ours, and the KV repair puts the hash straight back |

Everything that reads work items reads **live ones only** — the KV rebuild, the
KV health sample, and the expected-count comparison Cron C makes. That is not
tidiness. A revoked item is absent from KV by design, so counting it would make
every revocation look like a key KV had lost, and `kv-repair` would resurrect
the hash and suppress a consumer who had asked to be released.

A removal naming a work item we never held marks nothing, which is normal: DROP
does not know which of its identifiers we were given.

### The four status codes

| Code | Meaning | When we say it |
|---|---|---|
| `2` | Exempted | by policy — a legal basis to keep the data |
| `3` | Deleted | we held data about them and erased it |
| `4` | Opted out | they are suppressed but the data is retained |
| `5` | Not found | we hold nothing about them |

Codes `0` and `1` are unused by the specification. `5` is the default for
anything untouched, which is why a Cron B that runs before Cron C would report a
clean sheet for a cycle whose matches were never looked for.

`3 Deleted` is the only one that rests on something having happened, and it is
the reason the erase is verified by re-reading rather than assumed from the
absence of an exception.

---

## 1. What DROP gives us, and what we derive

Four lists. Each entry is a work item ID and a Base64 SHA-256 hash.

| List | Hashed input |
|---|---|
| `email` | the address, whitespace removed, lowercased |
| `phone` | digits only, last ten |
| `ndz` | `sha256(first) + sha256(last) + sha256(dob) + sha256(zip)`, hashed again |
| `namevin` | `sha256(first) + sha256(last) + sha256(vin)`, hashed again |

Normalization lives in `worker/drop-normalize.ts` and `worker/drop-report.ts`,
and again in the ClickHouse view that Cron C matches against. **If those two
ever disagree, no error is raised anywhere** — the gate simply answers "not
listed" for someone who is. Two sets of conformance vectors exist for that
reason: `test/drop-report.test.ts` asserts the specification's own published
hashes, and both it and `test/drop-normalize.test.ts` carry vectors generated
from the view's SQL.

The per-field rules do agree. **The grouping does not** — see §2 and §7.

`ndz` and `namevin` are composites: we take every first name, last name, date of
birth and ZIP we hold for a person and hash every combination. One consumer can
therefore produce thousands of candidate keys.

## 2. The rule that governs everything else: what an array element *is*

Every report we check is an array. What its elements represent decides how a
match propagates, and getting this wrong is the difference between suppressing a
consumer and only appearing to.

**A people report's elements are different people.** Search "John Smith", get
back forty John Smiths. One of them registered with DROP; the other
thirty-nine did not, and their records are not his.

**A phone or email report's elements are providers.** Search a phone number, get
back one payload per provider — Veriphone, Pipl, PDL — and every one of them
describes the *same* person, the subject of the search. They are views, not
people.

Two consequences, and they run in opposite directions:

- **Keys may be combined across providers, and must not be combined across
  people.** If Veriphone returns the name and Pipl returns the date of birth and
  ZIP, those four factors belong to one person and must form one `ndz` key.
  Deriving them per provider produces *no* `ndz` key at all — each provider is
  missing a factor — and an NDZ-registered consumer is never matched. Doing the
  same across two people in a people report would invent a key for someone who
  does not exist.
- **A match condemns the whole phone/email report, and only one element of a
  people report.** Every provider row is about the listed consumer, so none of
  it may be served or stored. In a people report the other thirty-nine are
  strangers and their records stay.

`reportGroups()` in `worker/drop-report.ts` is where this is decided: one group
per element for people, one merged group for phone and email. The subject stays
its own group in both cases, so `subjectListed` still distinguishes the
statutory fact — *this identifier is on a DROP list* — from the wider finding
that the report is about someone who is.

### Cron C groups the same way, since v3 of the view

It did not until recently, and it was the pipeline's largest correctness gap.
`spec_version` v2 derived keys **per source row** and unioned them — right for
people, where a source row is one array element, and wrong for phone and email,
where a source row is one provider. It never combined a name from Veriphone with
a date of birth and ZIP from Pipl, so it derived *no* `ndz` key whenever the four
factors arrived from different providers, which is the ordinary case. Cron C
under-matched precisely the NDZ registrations it exists to catch, silently.

`sql/clickhouse-view-rebuild.sql` (v3) merges the providers for phone and email
and keeps per-element grouping for people, matching `reportGroups()`.

**The caps came with it, and had to.** Merging multiplies the factors, so v2's
rule — drop the composites when the width passes 20,000 — would have fired on
most email values and derived nothing, which is worse than the bug. The view now
caps exactly as `FIELD_CAPS` does: 10 first names, 10 last names, 5 dates of
birth, 24 ZIPs, 12 VINs, **sorted then sliced on the normalized values and
before hashing**, because the Worker sorts values and sorting hashes instead
would pick a different subset.

Order matters twice over:

| | |
|---|---|
| people | cap per element → keys per element → union |
| phone / email | union the raw fields across providers → **then** cap → **then** one cross product |

Capping before merging would take each provider's first ten names and merge
those, which is not the ten the Worker picks.

One group is then bounded at `10·10·5·24 + 10·10·12` = 13,200 keys, so for phone
and email the 20,000 cut is unreachable and the two sides agree exactly. A people
report sums its groups and can still cross it, and the composites are dropped —
as they are in the Worker.

The fix was verified against DEV with the two cases that distinguish the
behaviours: fields for one person split across two *providers* now produce one
`ndz` key, byte-identical to the Anna / Smith / 19800101 / 90210 conformance
vector; the same fields split across two *array elements* still produce none.

## 2a. What is checked, and what is not

| Lookup | Screened | Why |
|---|---|---|
| phone | yes | the subject is itself a DROP key |
| email | yes | the subject is itself a DROP key |
| people | yes | no subject key, but the records carry NDZ and NameVIN keys |
| VIN | **no** | — |
| username | **no** | — |
| entity | **no** | — |

The three unscreened types have weaker keys, not no keys: a VIN report can carry
a name and a VIN, which is a `namevin` key exactly. "Weaker" is not a reason, it
is an unfinished decision.

## 3. The three operating rules

### Rule 1 — a matched phone or email report is suppressed in full

Nothing from it is served, and nothing from it is stored.

- **Fresh report.** No provider row is written to ClickHouse. Provider responses
  are held (`pendingRows`) until the report is screened, and flushed only if it
  comes back clean; a match means they are dropped, not written and deleted.
- **Cached report.** Every `entity_search_results` row for the subject is
  erased, by `(type, normalized_value)` — all providers, not the one the match
  came from.
- **Response.** Empty, and it does not say why. The user sees a report with no
  results, indistinguishable from a search that found nothing. DROP is never
  named in an API response.

### Rule 2 — a matched people report loses only the matched elements

The report is still served and still stored, without those people.

- **Fresh report.** The filtered array is written.
- **Cached report.** The stored payload is edited in place: the matched array
  positions are removed and the row survives. It is pinned to the exact row
  version that was read, because positions belong to one array and applying
  them to another deletes the wrong people.

### Rule 3 — a check that cannot run is never a clean result

"Unknown" and "not listed" are indistinguishable to everything downstream, and
treating the first as the second serves data for someone who asked to be
deleted. So:

- The Worker answers **503**, never `listed: false`.
- The lookup API **fails closed**: the lookup raises, the report is marked
  **failed**, and no records are served. `DROP_CHECK_FAIL_OPEN=true` inverts
  this and should only ever be set deliberately.
- An unverified report is **never** written to ClickHouse, under either setting.
- The failure raises a **Better Stack alert**. A silent fail-closed is a
  suppression system that has stopped working and nobody knows.

A *reduced* check is the middle case. When a report's cross product is too large
to run in full the composite keys are capped (`FIELD_CAPS`) or dropped entirely
in favour of the exact email and phone keys. The answer carries `partial: true`.
A match under `partial` is as trustworthy as any; a **miss** is weaker evidence,
and the caller must not cache it — the reduction is deterministic, so every
later search would reduce the same way and miss the same way, making one weak
answer permanent.

Exact email and phone keys are never capped. The high-confidence half of the
check stays complete however noisy the payload is.

## 4. The endpoints, and who calls them

| Endpoint | Caller | Question it answers |
|---|---|---|
| `POST /api/drop/check` | website | is this one identifier suppressed? |
| `POST /api/drop/report-check` | lookup API | does this report touch any DROP key? |
| `POST /api/drop/match-found` | lookup API | record the match — **before** erasing |
| `POST /api/drop/erase-incident` | lookup API | the erase is done; verify and close it out |

`report-check` writes nothing and erases nothing. It answers, and the caller
decides.

### The gate answers two different questions at once

`/api/drop/check` is asked about one identifier and returns both:

| Field | Meaning | Who reads it |
|---|---|---|
| `listed` | do not search this, do not serve it | the funnel |
| `onDropList` | this identifier is on California's list | anything reporting to the state |
| `source` | `drop` or `suppressed-report` | logs and triage |

`listed` is true for either source, so a caller that reads nothing else gets the
cautious behaviour. `onDropList` is the statutory fact and has to be asked for
**by name** — that separation is deliberate, because a suppressed-report value
read as DROP membership would put a match into a compliance record California
never asked for.

The intended caller is the **website, before the lookup funnel starts**: one
question, and a listed subject costs no provider call at all. Nothing calls it
today (§7).

### The cached-report path is a fallback, and reaching it is a failure

This is the third line of defence, not a normal path:

1. **Before the write** — a fresh report is screened and the listed records are
   never stored (Rules 1 and 2).
2. **The sweep** — Cron C erases what is already stored, every cycle.
3. **The request path** — a cached report is re-checked when someone searches
   for it, and this is where the erase sequence below runs.

Step 3 catches only what steps 1 and 2 missed. So the erase is the small part.
The important part is what a step-3 event *means*: a report Cron C was told to
erase by a new DROP diff is still sitting in `entity_search_results`. Either the
sweep has not run since the diff arrived, or it ran and missed — a partial KV
sync, a failed chunk, a view that did not refresh, a normalization drift. All of
them look identical from here, and all of them mean the sweep is not working.

**That has to raise an incident, and today it does not.** The Worker sends a
`warn` match alert from `match-found`, in the same shape and at the same level
as the thousands Cron C sends on a healthy run — so the one event meaning "the
sweep is broken" is indistinguishable from the events meaning "the sweep is
working". It needs its own paging signal on that path alone, carrying the
`runId`, the list type and the count, and nothing identifying.

A fallback that works silently is the one thing a fallback must not be: it hides
a broken Cron C for as long as nobody reads the log.

### The erase sequence, and why it splits

Cron C finds a match, erases, and records. When the **lookup API** performs the
erase — it owns the write path for `entity_search_results`, so it owns the
delete — the sequence has to split at the same seam:

1. `match-found` — the match row and the alert. **Before** the erase.
2. the API erases.
3. `erase-incident` — verify it happened, then the R2 evidence file.

The order is not cosmetic. The erase destroys the only other evidence the
consumer was ever in our data. A match row written first and then a failed erase
is recoverable: Cron C finds the rows again next run. An erase with no match row
is not: the rows are gone, the view has nothing left to match, and the only
record that the consumer was ever there is the one that was never written.

`erase-incident` checks two things rather than believing them — that a match
really occurred (re-derived against KV) and that the rows are really gone (read
back from ClickHouse). A caller wrong about either gets a refusal and nothing is
written. A false "deleted" is worse than a missing one, because a missing one is
still discoverable.

### What the incident path deliberately does not do

It does not set `status = 'deleted'` on the work item. That status is what Cron B
reports to California as **code 3 Deleted**, and it is a claim about the
*consumer*, not about one report. This path erased the rows for one
`normalized_value`; an `ndz` work item stands for a person who may sit in dozens
of other cached emails and phone numbers it never touched. Cron C can make that
claim because it sweeps everything in one run. This path cannot, so it writes the
match row and leaves the status to the sweep.

The cost is that Cron B under-reports — code 5 Not found — until the next Cron C
run. That is wrong in the recoverable direction, and the match row is what makes
it recoverable.

## 5. The suppression cache

A phone number or email can be absent from every DROP list and still produce a
report that cannot be served, because the aggregated data carries a listed
person. The subject is not suppressed; the report is. Without a record of that,
every later search for the value calls the providers, spends the credit, builds
the report and has it suppressed again.

So the value is remembered — hash in KV under a `suppressed:` prefix, normalized
value in Supabase — and `/api/drop/check` answers from it.

Two things this is **not**:

- **Not a compliance record.** "Searching this yields data we must suppress" is
  ours and derived. "This identifier belongs to a consumer who asked to be
  deleted" is California's, and lives in `ca_drop_work_item`. Cron B reports from
  the second and must never report from the first, which is why the gate says
  which of the two it answered from (`source`).
- **Not on the answer's path.** It is a cost optimisation. Losing it costs one
  provider fan-out. It runs in `waitUntil`, after the response, and can never
  delay a request or change an answer.

The prefix matters: Cron C lists the KV namespace and copies what it finds into
ClickHouse to match against. A suppression key is not a DROP hash and must never
be mistaken for one.

Once written it stays. There is no expiry and no clearing path.

## 6. Where a mistake shows up as silence

Ranked by how hard it is to notice.

1. **Normalization drift** between `drop-normalize.ts` and the ClickHouse view.
   No error; a consumer is simply never matched.
2. **Wrong grouping** — combining keys across people, or failing to combine them
   across providers. No error; the second under-matches exactly the NDZ
   registrations it exists to catch.
3. **A partial KV sync.** Cron C matches against whatever made it into the
   mirror, and a missed match is indistinguishable from no match.
4. **A fail-closed nobody alerted on.** The system is refusing to serve, which is
   safe, but it has stopped suppressing anyone and looks healthy from outside.

Every one of these fails quietly and in the direction of serving data we should
not serve. That is why the checks that exist — the conformance vectors, the KV
health sample, the read-back verification in `erase-incident` — are not optional
extras.

## 7. Where the code does not yet meet this document

Rules 1 and 2 are implemented as described above. Everything below is a known
gap, ordered by consequence rather than by effort.

### It stops suppressing, or never starts

- **`DROP_WORKER_URL` is not set anywhere.** Unset means unconfigured, and
  fail-closed turns that into a 503 on every screened lookup. Deploying the API
  before setting it takes phone, email and people search down.
- **The Worker has no authentication.** `report-check` is an oracle for testing
  whether a value is on the DROP list, and `erase-incident` writes a compliance
  record and is trusted to have been called honestly. The API sends
  `DROP_WORKER_TOKEN`; the Worker ignores it.
- **VIN, username and entity lookups are not screened at all** (§2a).

### It fails quietly

- **No paging incident when the fallback fires** (§4). A broken Cron C is
  invisible for as long as nobody reads the log.
- **The view is not deployed until it is rebuilt.** v3 fixes the grouping, and
  nothing takes effect until `sql/clickhouse-view-rebuild.sql` is run and the
  view refreshed. Until then Cron C is still matching on v2's per-provider keys.
- **An oversized report is still handled differently on the two sides.** Both
  cap the factors identically. Past the 20,000 total the Worker falls back to
  the exact keys and flags `partial`; the view drops the composites. With the
  caps in place this is now reachable only for a very wide people report, and
  the direction is that the view checks *more* than the Worker rather than
  less.
- **`arraySort` orders by UTF-8 bytes; JavaScript `sort()` by UTF-16 code
  units.** They agree for everything in the Basic Multilingual Plane, which is
  all of `[a-z0-9]` and almost all CJK. A name containing a character above
  U+FFFF could be capped to a different subset on the two sides. Vanishingly
  rare, and it only matters for a report already over a cap.
- **Nothing gates Cron B on Cron C.** Absence of a status reads as `5 Not
  found`, so a Cron B that fires first reports a clean sheet for a cycle whose
  matches were never looked for.

### Rule 3 is implemented except for its alerting half

- **No dedicated Better Stack alert on a fail-closed.** The lookup API logs at
  `error` (`DropCheckService.unverified`, `CoordinatorProcessor.process`), which
  the Logtail transport ships to Better Stack — so the signal is there only if an
  alert rule is configured on those messages, and nothing in either repo defines
  one. There is no Sentry or Slack path either: the `AppExceptionFilter` that
  would provide it runs for HTTP requests, not BullMQ jobs, so the
  `DROP_CHECK_UNAVAILABLE` exception never reaches it.
- **The Worker logs nothing when the check fails.** The 503s in
  `/api/drop/report-check` and `/api/drop/check` return without calling
  `logRun`, so the side that actually knows the check could not run reports
  nothing. The cron paths already use `logRun`/`logMatches` against Better Stack;
  the gate routes should use the same.
- **`failed` does not say why.** A DROP outage and a provider timeout both
  surface as `status: "failed"`, so the client cannot distinguish "we could not
  verify" from "the lookup broke".

### It costs more than it needs to

- **Nothing calls `/api/drop/check`.** The suppression cache (§5) is written and
  maintained, and no caller reads it, so a permanently-suppressed value still
  pays a full provider fan-out on every search.
- **The second half of the erase sequence is fire-and-forget.**
  `reportErasure`'s return value is discarded at both call sites. After a
  successful `match-found` the unrecoverable case is closed, but a lost
  `erase-incident` still leaves a match row with no evidence file. BullMQ is
  already there.

### Outside this pipeline, but it decides what the pipeline can audit

- **The ClickHouse write buffer can lose a report.** `flush()` reads and deletes
  the Redis batch *before* an insert that can fail all three retries, so a
  dropped batch never reaches ClickHouse — and Cron C cannot audit what was
  never stored.
- **Report history is written at initiation, not on completion.** The website
  inserts the `search_history` row as soon as the API returns a `lookupId`,
  before any result exists, and nothing removes it if the job later fails. So
  failing a lookup does not prevent a history entry, and the row keeps the
  searched value.

---

## Keeping this file honest

It describes rules, so it goes stale the moment a rule changes rather than when
code moves. The places that pin each rule down:

| Rule | Pinned by |
|---|---|
| normalization (§1) | `test/drop-report.test.ts` — vectors from the published spec *and* from the view's own SQL |
| grouping (§2) | `reportGroups()` in `worker/drop-report.ts`, and the tests that assert no key crosses a people boundary |
| Rule 1 / Rule 2 | `screenAndPersistRecords` and `finalizePeopleReport` in the lookup API |
| Rule 3 | the 503 contract in `worker/index.ts`, `DropCheckService.unverified` |
| revocation (§0) | `markWorkItemsRevoked` in `worker/db.ts`, and the `revoked_at IS NULL` filter on all three readers |
| erase ordering (§4) | the test asserting the literal sequence `['match-found', 'erase']` |

If you change a rule, change this file in the same commit. A document that
describes an intention nobody implemented is worse than no document, because the
next person believes it.

- **No dedicated Better Stack alert on a fail-closed.** The lookup API logs at
  `error` (`DropCheckService.unverified`, `CoordinatorProcessor.process`), which
  the Logtail transport ships to Better Stack — so the signal is there only if an
  alert rule is configured on those messages, and nothing in either repo defines
  one. There is no Sentry or Slack path either: the `AppExceptionFilter` that
  would provide it runs for HTTP requests, not BullMQ jobs, so the
  `DROP_CHECK_UNAVAILABLE` exception never reaches it.
- **The Worker logs nothing when the check fails.** The 503s in
  `/api/drop/report-check` and `/api/drop/check` return without calling
  `logRun`, so the side that actually knows the check could not run reports
  nothing. The cron paths already use `logRun`/`logMatches` against Better Stack;
  the gate routes should use the same.
- **`failed` does not say why.** A DROP outage and a provider timeout both
  surface as `status: "failed"`, so the client cannot distinguish "we could not
  verify" from "the lookup broke".
