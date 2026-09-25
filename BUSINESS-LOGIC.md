# DROP suppression — business logic

The rules this pipeline is obliged to follow. The architecture (Workflow, KV,
Hyperdrive, ClickHouse) is in the diagram and the code comments explain
mechanism; this file is only about the rules.

The regulation: California's Delete Request and Opt-out Platform. A consumer
registers with the state, the state publishes hashed identifiers, and a
registered data broker must not process or serve data about them.

Specification: https://privacy.ca.gov/drop-for-data-brokers/technical-specifications/working-with-data/

---

## 0. The obligation, and the clock

Two separate duties. Meeting one and missing the other is still non-compliant.

**Suppress.** Do not process or serve data about a registered consumer.
Continuous; everything from Rule 1 onwards serves this.

**Report.** Tell the state what we did about every work item we were given.
Periodic, and Cron B's job. A work item nobody has looked at is still reported —
as `5 Not found` — so silence is an answer, and one we may be wrong about.

The list must be downloaded **at least every 45 days**. The cycle is the unit of
work: download, match, erase, report. Cron B has to run _after_ Cron C for its
answers to be true, and it refuses to run otherwise (§5a).

### The download is a delta

Per the specification, "after initial download and completed upload, future
downloads will include only new identifiers since previous list download". The
first download is the whole list; every later one carries only what is new. Two
consequences:

**The complete set exists only on our side, and DROP cannot re-serve it.** R2
holds every raw ZIP verbatim and is the only source it can be rebuilt from.
Backing up R2 and testing the restore is a compliance control, not housekeeping.

**KV must be cumulative.** Clearing it and loading only the newest delta stops
suppressing everyone registered in an earlier cycle — silently, and invisibly
from outside. `clearKv` defaults to `false`; it exists for rebuilding from the
archive, not for normal runs.

### Consumers can be removed

DROP publishes a removals file alongside the four lists — `Id,Hash,ListType` —
when a consumer withdraws their request or the state revokes an entry. Two
different obligations follow:

- **Stop suppressing them.** The hash is deleted from KV, so the gate releases
  them on the next request.
- **Stop answering for them.** `revoked_at` is stamped on the
  `ca_drop_work_item` row, so they are no longer a work item we owe California a
  status for.

The row is kept rather than deleted: it records that we were once asked to
suppress this consumer and then released, and Cron B needs "never given to us"
to differ from "given and then withdrawn".

**Supabase is stamped before KV is deleted from**, and the order is load-bearing:

| Failure                  | Result                                                                                         |
| ------------------------ | ---------------------------------------------------------------------------------------------- |
| stamped, KV delete fails | we keep suppressing someone who withdrew — over-suppression, and the next run retries          |
| KV deleted, stamp fails  | we release them _and_ still report them as ours, and the KV repair puts the hash straight back |

Everything that reads work items reads **live ones only** — the KV rebuild, the
KV health sample, and the expected-count comparison Cron C makes. A revoked item
is absent from KV by design, so counting it would make every revocation look
like a key KV had lost, and `kv-repair` would resurrect the hash and suppress a
consumer who had asked to be released.

A removal naming a work item we never held marks nothing, which is normal: DROP
does not know which of its identifiers we were given.

### The four status codes

| Code | Meaning   | When we say it                               |
| ---- | --------- | -------------------------------------------- |
| `2`  | Exempted  | by policy — a legal basis to keep the data   |
| `3`  | Deleted   | we held data about them and erased it        |
| `4`  | Opted out | they are suppressed but the data is retained |
| `5`  | Not found | we hold nothing about them                   |

Codes `0` and `1` are unused by the specification. `5` is the default for
anything untouched, which is why a Cron B that runs before Cron C reports a
clean sheet for a cycle whose matches were never looked for.

`3 Deleted` is the only code that rests on something having happened, and the
reason the erase is verified by re-reading rather than assumed from the absence
of an exception.

---

## 1. What DROP gives us, and what we derive

Four lists. Each entry is a work item ID and a Base64 SHA-256 hash.

| List      | Hashed input                                                             |
| --------- | ------------------------------------------------------------------------ |
| `email`   | the address, whitespace removed, lowercased                              |
| `phone`   | digits only, last ten                                                    |
| `ndz`     | `sha256(first) + sha256(last) + sha256(dob) + sha256(zip)`, hashed again |
| `namevin` | `sha256(first) + sha256(last) + sha256(vin)`, hashed again               |

`ndz` and `namevin` are composites: every first name, last name, date of birth
and ZIP held for a person is hashed in every combination, so one consumer can
produce thousands of candidate keys.

Normalization and key derivation exist twice — in `worker/drop-normalize.ts` and
`worker/drop-report.ts`, and again in the ClickHouse view Cron C matches
against. **If the two ever disagree, no error is raised anywhere**: the gate
simply answers "not listed" for someone who is. Two sets of conformance vectors
guard that seam. `test/drop-report.test.ts` asserts the specification's own
published hashes, and both it and `test/drop-normalize.test.ts` carry vectors
generated from the view's SQL.

## 2. The rule that governs everything else: what an array element _is_

Every report we check is an array. What its elements represent decides how a
match propagates, and getting it wrong is the difference between suppressing a
consumer and only appearing to.

**A people report's elements are different people.** Search "John Smith", get
back forty John Smiths. One registered with DROP; the other thirty-nine did
not, and their records are not his.

**A phone or email report's elements are providers.** Search a phone number, get
one payload per provider — Veriphone, Pipl, PDL — each describing the _same_
person, the subject of the search. They are views, not people.

Two consequences, running in opposite directions:

- **Keys may be combined across providers, and must not be combined across
  people.** If Veriphone returns the name and Pipl the date of birth and ZIP,
  those four factors belong to one person and form one `ndz` key. Deriving them
  per provider produces _no_ `ndz` key at all — each provider is missing a
  factor — and an NDZ-registered consumer is never matched. Combining across two
  people in a people report invents a key for someone who does not exist.
- **A match condemns the whole phone/email report, and only one element of a
  people report.** Every provider row is about the listed consumer, so none of
  it may be served or stored. In a people report the other thirty-nine are
  strangers and their records stay.

`reportGroups()` in `worker/drop-report.ts` decides this: one group per element
for people, one merged group for phone and email.

### What `report-check` answers

`POST /api/drop/report-check` returns `{ type, listed }`, and for a people
report also `records: [{ index, listed }]` — one verdict per array element,
because that is what lets the caller remove the matched person and keep the
strangers. A phone or email report has nothing to name, so it gets the two
fields alone.

Nothing else travels to the caller: no matched key families, no count of keys
checked, no `partial`. The lookup API acts on `listed`, places a people match
with `records`, and logs a match as an error carrying only the report type —
never a record count or any of the matched data.

`index` is a position in **the array the caller sent**, which is not the array
it holds: the Worker skips payloads it cannot read, so the caller maps the
indexes back itself (`readable` in `DropCheckService`). A people match whose
verdicts cannot be placed at all suppresses the whole report — the Worker
called it listed, and serving the element that matched is not an option.

### Cron C groups the same way

`sql/clickhouse.sql` builds view `ca_drop_combined_search_result`, `spec_version`
v4, whose **grain is the grouping rule**:

| Report shape  | One view row is                                                  |
| ------------- | ---------------------------------------------------------------- |
| people        | one array element of one stored row version, named by its digest |
| phone / email | one `(type, normalized_value)`, every provider merged into it    |

It is one `GROUP BY` whose key carries the element locator for people and a
constant for the other two, so both halves of the rule come out of one
derivation rather than two that can drift apart. `reportGroups()` makes the same
split in the Worker.

The grain is not only about deriving keys correctly. It is what makes the matched
**element** nameable, and without that Rule 2 cannot be kept at all: a match
that can name only the identifier leaves the whole row as the one thing there is
to erase.

A match carries `element_digest`, the SHA-256 of the element's raw JSON, and
**not** its array position. `entity_search_results` is a `ReplacingMergeTree`,
so several versions of one key coexist with different array lengths and
different people at the same index, and the view is refreshed long before the
erase runs. A position is meaningful only against the array it was read from; a
digest is meaningful against every stored copy of that element.

DEV still carries v3, which had no element identity at all. §7 has what that
costs until the view is rebuilt.

The view caps the factors exactly as `FIELD_CAPS` does — 10 first names, 10 last
names, 5 dates of birth, 24 ZIPs, 12 VINs — **sorted and sliced on the
normalized values, before hashing**, because the Worker sorts values and sorting
hashes instead would select a different subset. Without the caps, merging
providers multiplies the factors far enough that most email values would pass
the 20,000 cut and derive no composites at all.

Order matters twice over:

|               |                                                                                   |
| ------------- | --------------------------------------------------------------------------------- |
| people        | cap per element → keys per element, and never a union across elements             |
| phone / email | union the raw fields across providers → **then** cap → **then** one cross product |

Both fall out of capping _after_ the grouping and never before it. Capping first
would take each provider's own first ten names and merge those, which is not the
ten the Worker picks.

One group is bounded at `10·10·5·24 + 10·10·12` = 13,200 keys, so for phone and
email the 20,000 cut is unreachable and the two sides agree exactly. A people
report sums its elements and can still cross it; its composites are then
dropped, as they are in the Worker. The cut stays a decision about the whole
report even though a people report is now many rows — the view sums the report
with a window — because the Worker adds up its groups the same way.
On DEV no row has crossed it — `oversized_records` is 0 across all 2,033 values.

## 2a. What is checked, and what is not

| Lookup   | Screened | Why                                                        |
| -------- | -------- | ---------------------------------------------------------- |
| phone    | yes      | the subject is itself a DROP key                           |
| email    | yes      | the subject is itself a DROP key                           |
| people   | yes      | no subject key, but the records carry NDZ and NameVIN keys |
| VIN      | **no**   | —                                                          |
| username | **no**   | —                                                          |
| entity   | **no**   | —                                                          |

The three unscreened types have weaker keys, not no keys: a VIN report can carry
a name and a VIN, which is a `namevin` key exactly. "Weaker" is not a reason, it
is an unfinished decision.

## 3. The three operating rules

### Rule 1 — a matched phone or email report is suppressed in full

Nothing from it is served, and nothing from it is stored.

- **Fresh report.** No provider row is written to ClickHouse. Provider responses
  are held (`pendingRows`) until the report is screened and flushed only if it
  comes back clean; a match means they are dropped, not written and deleted.
- **Cached report.** Every `entity_search_results` row for the subject is
  erased, by `(type, normalized_value)` — all providers, not just the one the
  match came from.
- **Response.** Empty, and it does not say why. The user sees a report with no
  results, indistinguishable from a search that found nothing. DROP is never
  named in an API response.

### Rule 2 — a matched people report loses only the matched elements

The report is still served and still stored, without those people. This holds on
every path that can erase, including — especially — the sweep, which is the
destructive one and reaches everything.

- **Fresh report.** The filtered array is written.
- **Cached report, on the request path.** The stored payload is edited in place:
  the matched array positions are removed and the row survives. The edit is
  pinned to the exact row version that was read, because positions belong to one
  array and applying them to another deletes the wrong people.
- **Cached report, in the sweep.** The same array surgery, keyed on the
  element's digest instead of its position, so it needs no pinning and clears
  every stored version of that element in one mutation. A whole-row delete here
  would take the other thirty-nine John Smiths, irreversibly, and it is the
  path most likely to find the match first.

`ALTER TABLE … UPDATE`, not a rewrite sent from the Worker: the payload runs to
megabytes, and ClickHouse edits its own stored value. It is a mutation for the
same reason the delete is one — the part is rewritten, so the old payload does
not sit on disk waiting for a merge.

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
  suppression system that has stopped working with nobody aware.

**There is no middle case any more.** A reduced check used to answer with
`partial: true`, leaving the caller to treat a miss as weaker evidence. The
answer no longer carries that warning, so `report-check` runs the check in full
or fails:

| Situation                                     | Answer                                 |
| --------------------------------------------- | -------------------------------------- |
| cross product over `MAX_REPORT_KEYS` (20,000) | **503**, `detail` names the count      |
| no DROP key derivable from the report at all  | **503**, `detail` says so              |
| KV unreachable                                | **503**, `detail` carries the KV error |

All three reach the lookup API as a failed check: it logs the worker's reason,
raises, and the lookup is marked **failed**. A pathological report — the
measured worst case is an aggregated email row at 33M candidate keys — now
fails that subject's lookup instead of being capped down to something runnable.
`FIELD_CAPS` and `exactFieldsOnly()` still bound the **incident** paths, where a
subset of the hits is worth having because the erasure already happened.

## 4. The endpoints, and who calls them

### Starting the crons

Cron A, Cron B and Cron C are **started by hand, never on a schedule**, and
only by an operator. `POST /api/downloader/start`, `/api/workflow/start` and
`/api/status-report/start` require `Authorization: Bearer <DROP_OPERATOR_TOKEN>`
and answer `401` without it. With the secret unset they answer `503`: nobody
can start a run, rather than everybody. It is a separate secret from the one
the Lookup API sends, because the API has no reason to start a workflow.

Manual on purpose: each run is a step in a cycle someone is accountable for —
download, then sweep, then report — and Cron B refuses a sweep it cannot
trust, so an operator deciding when each happens is the control, not a gap.

### Who calls what

| Endpoint                        | Caller     | Question it answers                        |
| ------------------------------- | ---------- | ------------------------------------------ |
| `POST /api/drop/check`          | website    | is this one identifier suppressed?         |
| `POST /api/drop/report-check`   | lookup API | does this report touch any DROP key?       |
| `POST /api/drop/match-found`    | lookup API | record the match — **before** erasing      |
| `POST /api/drop/erase-incident` | lookup API | the erase is done; verify and close it out |

`report-check` writes nothing and erases nothing. It answers, and the caller
decides.

### The gate answers one question

`/api/drop/check` is asked about one identifier and returns `{ type, listed }`.
`listed` means: do not search this, do not serve it.

It reads the hash in **two KV namespaces** and a hit in either is `listed`:

| Namespace       | Holds                                          | Also read by        |
| --------------- | ---------------------------------------------- | ------------------- |
| `kv`            | California's DROP hash set                     | Cron C, report-check |
| `suppressed_kv` | hashes of values in `ca_drop_suppressed_value` | nothing else        |

Which namespace answered stays inside the Worker. The statutory fact — _this
identifier belongs to a consumer who asked to be deleted_ — is not something a
caller should take from a gate read: it lives in `ca_drop_work_item`, and Cron B
reports from there.

The intended caller is the **website, before the lookup funnel starts**: one
question, and a listed subject costs no provider call at all. Nothing calls it
today (§7).

### The cached-report path is a fallback, and reaching it is a failure

Three lines of defence, in order:

1. **Before the write** — a fresh report is screened and listed records are
   never stored (Rules 1 and 2).
2. **The sweep** — Cron C erases what is already stored, every cycle, by the
   same two rules: the whole row set for a phone or email identifier, the
   matched elements only for a people report.
3. **The request path** — a cached report is re-checked when someone searches
   for it, and this is where the erase sequence below runs.

Step 3 catches only what steps 1 and 2 missed, so the erase is the small part.
The important part is what a step-3 event _means_: a report Cron C was told to
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

### The erase sequence, and why it splits

Cron C finds a match, erases, and records. When the **lookup API** performs the
erase — it owns the write path for `entity_search_results`, so it owns the
delete — the sequence splits at the same seam:

1. `match-found` — the match row and the alert. **Before** the erase.
2. the API erases.
3. `erase-incident` — verify it happened, then the R2 evidence file.

The order is not cosmetic. The erase destroys the only other evidence that the
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
_consumer_, not about one report. This path erased the rows for one
`normalized_value`; an `ndz` work item stands for a person who may sit in dozens
of other cached emails and phone numbers it never touched. Cron C can make that
claim because it sweeps everything in one run. This path cannot, so it writes the
match row and leaves the status to the sweep.

Cron B does not report such an item as `5 Not found` in the meantime: a work
item with a match row and no status is **held back** until the next Cron C run
sets `deleted`, and is then reported as `3` (§5a). The match row is what makes
that possible.

## 5. Suppressed values

A phone number or email can be absent from every DROP list and still produce a
report that cannot be served, because the aggregated data carries a listed
person. The subject is not suppressed; the report is.

That value is recorded twice, by `report-check`, when a phone or email report
matches and its subject does not:

1. **`ca_drop_suppressed_value`** in Supabase — one row per
   `(search_type, value)`, DROP-normalized. The source of truth.
2. **`suppressed_kv`** — the value's DROP hash as the key. This is what the
   website's gate reads, so a later search for the value is answered `listed`
   without calling a provider.

Supabase is written first; `kv-repair` rebuilds `suppressed_kv` from it.

`suppressed_kv` is a **separate namespace** from the DROP `kv`, on purpose. Cron
C lists `kv` and copies it into ClickHouse as the set it matches and erases
against. A suppressed value is our own finding, not California's hash, and
keeping it in its own namespace means it can never join that set.

Two things this is **not**:

- **Not a compliance record.** "Searching this yields data we must suppress" is
  ours and derived. "This identifier belongs to a consumer who asked to be
  deleted" is California's, and lives in `ca_drop_work_item`. Cron B reports from
  the second and must never report from the first.
- **Not on the answer's path.** It runs in `waitUntil`, after the response, and
  can never delay a request or change an answer. A failed write is logged and
  dropped.

Once written it stays. There is no expiry and no clearing path.

## 5a. Reporting to DROP (Cron B)

`POST /api/status-report/start` with `{ cleanupInstanceId, upload? }` and the
operator token (§4). Workflow `drop-status-report`, in
`worker/workflow-status-report.ts`.

### It only runs on a real sweep

The caller names the Cron C run the report rests on. Cron B refuses unless that
run is **complete**, was **not a dry run**, **synced the DROP set from KV**,
**refreshed the view**, and **started after the newest live work item
arrived**. The last one is the point: absence of a status reads as `5`, so a
report over work items Cron C never looked at is a clean sheet nobody earned.
The refusal names which condition failed.

### What each work item gets

| Work item                       | Reported as                                   |
| ------------------------------- | --------------------------------------------- |
| `status = 'deleted'`            | `3` Deleted                                   |
| `status = 'exempted'`           | `2` Exempted                                  |
| `status = 'opted_out'`          | `4` Opted out                                 |
| no status, no match row         | `5` Not found                                 |
| no status, **has** a match row  | held back — found, not yet erased             |
| revoked                         | not reported; DROP asks for no response       |

Nothing sets `exempted` or `opted_out` today. They are policy decisions, not
something the pipeline infers.

### Upload, then amend

Each work item remembers what was last reported (`reported_status`,
`reported_at`). A run sends:

- **`POST /data/upload`** — every live item never reported.
- **`POST /data/amend`** — every item reported before whose code has changed,
  typically `5` → `3` after a later sweep erased a match. The specification
  requires an update within 45 days of the change.

`reported_status` is written only for files DROP answers as accepted, and with
the code that was in the file, so a status that changes between building and
accepting is amended on the next run rather than lost.

### The files

One `Id,Status` CSV per downloaded file and kind, named after the file it
answers: `20260910_0000_NDZ.csv` is answered by `20260910_0000_NDZ_U<run>.csv`
(upload) or `…_A<run>.csv` (amend). The suffix keeps names unique within the
cycle, as the specification requires. Every file is written to
`ca-drop/reports/<run>/` in R2 **before** anything is sent, so R2 holds exactly
what was reported; each upload also writes an audit file under `ca-drop/logs/`.

This is why `ca_drop_work_item.source_file` exists: Cron A records which
downloaded file each item came from. An item without one cannot be named, is
not reported, and fails the run.

### When a run fails

- DROP rejects a file — the others are still recorded; the run fails naming the
  rejected file and DROP's reason, and the next run sends it again.
- Items without `source_file` — the same.
- Without `upload: true`, or without the `DROP_API_KEY` secret, nothing is sent:
  the run builds and archives only. `upload: true` without the key fails.

A `202` means **queued, not accepted** — DROP validates rows afterwards and
answers by e-mail. A file DROP later refuses at row level is not visible to
Cron B.

## 6. Where a mistake shows up as silence

Ranked by how hard it is to notice.

1. **Normalization drift** between `drop-normalize.ts` and the ClickHouse view.
   No error; a consumer is simply never matched.
2. **Wrong grouping** — combining keys across people, or failing to combine them
   across providers. No error; the second under-matches exactly the NDZ
   registrations it exists to catch.

   Since v4 the grouping also decides what the sweep is allowed to erase, and
   that half does **not** fail quietly: a grain that cannot name the matched
   element makes every people erase a whole-row delete, and the strangers in
   that array are gone with an `ALTER TABLE … DELETE` that cannot be undone.
   Cron C refuses a people match with no element rather than widening it, which
   is why this is the one item on the list that stops the run.

3. **A partial KV sync.** Cron C matches against whatever made it into the
   mirror, and a missed match is indistinguishable from no match.
4. **A fail-closed nobody alerted on.** The system is refusing to serve, which is
   safe, but it has stopped suppressing anyone and looks healthy from outside.

Every one of these fails quietly and, but for the erase noted above, in the
direction of serving data we should not serve. That is why the checks that exist
— the conformance vectors, the KV health sample, the read-back verification in
`erase-incident` — are not optional extras.

## 7. Where the code does not yet meet this document

Rules 1 and 2 are implemented as described. Everything below is a known gap,
ordered by consequence rather than by effort.

### It stops suppressing, or never starts

- **The live DEV view is v3, and the sweep does not run until it is v4.** v4
  changes the view's grain, so it cannot be altered into place: `DROP VIEW`,
  re-run `sql/clickhouse.sql`'s section 2, `SYSTEM REFRESH VIEW` — in that
  order, and **before** the Worker is deployed. The grants survive, because
  ClickHouse records a privilege against the name. Cron C selects
  `element_digest`, so against a v3 view the match fails outright rather than
  matching less; a people match that cannot name its element is refused rather
  than widened into a whole-row delete. That is the right direction — a failed
  run alerts, and the records are still there for the next one — but for as long
  as the two disagree there is no sweep.
- **The request-path endpoints have no authentication.** Starting the crons
  needs the operator token (§4), but `report-check` is still an open oracle for
  testing whether a value is on the DROP list, and `erase-incident` writes a
  compliance record and is trusted to have been called honestly. The API sends
  `DROP_WORKER_TOKEN`; the Worker ignores it. The same is true of
  `/api/kv-repair/start`, which rewrites both KV namespaces.
- **VIN, username and entity lookups are not screened at all** (§2a).

### It fails quietly

- **No paging incident when the fallback fires** (§4). A broken Cron C is
  invisible for as long as nobody reads the log.
- **An oversized report now fails the lookup.** `report-check` caps nothing: a
  report over the 20,000 key total is a 503, so that subject's lookup fails
  every time until the payload gets narrower. Cron C's view still checks it, by
  dropping the composites, so the consumer is not missed — but the live search
  stays broken, and nothing measures how often this fires.
- **`arraySort` orders by UTF-8 bytes; JavaScript `sort()` by UTF-16 code
  units.** They agree for everything in the Basic Multilingual Plane, which is
  all of `[a-z0-9]` and almost all CJK. A name containing a character above
  U+FFFF could be capped to a different subset on the two sides. Rare, and it
  only matters for a report already over a cap.

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

### Cron B is built but not yet in service

- **It cannot upload yet.** There is no DROP account, so no `DROP_API_KEY`;
  runs build and archive only. The upload and amend calls follow the published
  API (`multipart/form-data`, field `files`, header `X-API-KEY`) and have only
  been exercised against a mocked response.
- **No alert as the deadline approaches.** It is started by hand, like Cron A
  and Cron C (§4), and the 45-day deadline is visible only as
  `oldestUnreportedDays` in the run summary.
- **Row-level rejections are invisible.** DROP validates after the `202` and
  answers by e-mail; nothing reads that mailbox or feeds it back.

### It costs more than it needs to

- **The second half of the erase sequence is fire-and-forget.**
  `reportErasure`'s return value is discarded at both call sites. After a
  successful `match-found` the unrecoverable case is closed, but a lost
  `erase-incident` still leaves a match row with no evidence file. BullMQ is
  already there.

### Outside this pipeline, but it decides what the pipeline can audit

- **The ClickHouse write buffer can lose a report.** `flush()` reads and deletes
  the Redis batch _before_ an insert that can fail all three retries, so a
  dropped batch never reaches ClickHouse — and Cron C cannot audit what was
  never stored.
- **Report history is written at initiation, not on completion.** The website
  inserts the `search_history` row as soon as the API returns a `lookupId`,
  before any result exists, and nothing removes it if the job later fails. So
  failing a lookup does not prevent a history entry, and the row keeps the
  searched value.

---
