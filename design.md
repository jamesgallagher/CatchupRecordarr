# Catchup Recordarr — Design Document

**Project:** A Dispatcharr plugin that detects IPTV channels supporting
catchup/timeshift, waits for a scheduled program — anything in the TV
guide, not a specific content type — to finish, and downloads it from the
provider's archive — surfacing the result as a normal, fully-functional
Dispatcharr recording (watchable, comskip-eligible) rather than a
bolted-on side feature.

**Reference implementation studied:** Sportarr
(github.com/Sportarr/Sportarr) — a .NET app with an equivalent
`CatchupDownloadService`. Sportarr happens to be sports-focused (its own
scheduling is built around league/team monitoring), but that's specific to
*Sportarr*, not to this project — this plugin has no content-type
restriction of its own; Sportarr was studied purely for its catchup/
timeshift mechanics. Concepts are ported; no code is shared (different
stack entirely — Dispatcharr is Django/Celery/Python).

**Target app:** Dispatcharr (github.com/Dispatcharr/Dispatcharr)

**Build strategy:** Plugin-first. No core Dispatcharr changes in v1. This
was a deliberate choice after establishing that Dispatcharr's existing
`Recording` model, playback endpoint, and comskip task are all decoupled
from *how* a recording's file was produced — a plugin can take over an
existing native `Recording` row (Section 4/5) and get native
playback/comskip for free just by updating it in place. See Section 7.
(Originally the plan was for the plugin to *create* the row itself;
superseded by Session 6's redesign — see Section 4.)

---

## How to use this document across sessions

This doc is the single source of truth for the project. Each section below
has a status tag:

- `[ ] NOT STARTED` — not discussed/decided yet
- `[~] DECIDED (design only)` — agreed on approach, no code written
- `[x] BUILT` — implemented, tested, working
- `[!] OPEN QUESTION` — needs a decision before building

**When resuming in a new session (any machine, any Claude instance):**
paste this whole file in, or point at it in the repo, and say "continue from
section N" or "here's what's built so far, what's next." Update the status
tags and the **Session Log** at the bottom as work progresses — that log is
the definitive "what actually happened" record, since this design section
may describe intent that shifts slightly once real code is written.

---

## Section 1 — Goals & Non-Goals `[~] DECIDED`

**Goal:** For IPTV channels whose provider supports catchup/timeshift
(Xtream `tv_archive=1`), automatically download a finished program from the
provider's archive instead of relying on live capture, and have the result
appear in Dispatcharr exactly like a normal recording — watchable from the
app, eligible for comskip commercial removal. "What to record" is never
decided by the plugin itself (Section 4, revised) — it's whatever the user
has already scheduled through Dispatcharr's own native recording features
(Series Rules, manual EPG scheduling, recurring rules); the plugin only
decides *how* to fulfill it once a catchup-capable channel is involved.

**Non-goals for v1:**
- No core Dispatcharr code changes (plugin-only).
- No automatic "is this worth recording" detection by category, name, or
  any other heuristic, and no plugin-owned channel curation UI either
  (Section 4, revised) — the plugin never independently decides what's
  worth recording; it only acts on channels and programs the user has
  already told Dispatcharr's native scheduling about, whatever those
  happen to be.
- No UI changes beyond what the plugin's own settings/actions panel
  provides.
- No support for non-Xtream (pure M3U) sources — catchup requires Xtream
  credentials to build a timeshift URL.

**Possible future (v2+, out of scope for now):** graduate parts of this
into Dispatcharr core — e.g. a real `has_archive` field on `Stream`, a
`capture_method` on `Recording`, native scheduler integration, a per-channel
UI toggle. Revisit once the plugin is proven.

---

## Section 2 — Why Plugin-First (Feasibility Summary) `[~] DECIDED`

Investigated by cloning both repos and reading the actual source (not just
docs). Key findings from Dispatcharr's codebase:

- **The Xtream archive flag is already fetched and silently discarded.**
  `apps/m3u/tasks.py` stores the *entire* raw stream dict from
  `get_live_streams` — including `tv_archive` / `tv_archive_duration` —
  into `Stream.custom_properties` (a JSONField) during every M3U sync.
  Nothing reads it back out today, but it's present for every existing
  install right now, no sync changes needed to access it.
- **EPG data needed for "program finished" detection already exists**:
  `Channel.epg_data → EPGData → ProgramData` (with `start_time`/`end_time`/
  `title`/`sub_title`/`description`), and a reusable timeslot-overlap
  matching helper already exists (`_match_epg_program_by_timeslot` in
  `apps/channels/tasks.py`).
- **Playback and comskip are decoupled from capture method.** The
  `GET /api/channels/recordings/{id}/file/` endpoint and the
  `comskip_process_recording` Celery task both operate purely on
  `Recording.custom_properties` (`file_path`, `file_name`, `status`, etc.)
  — they don't know or care whether ffmpeg captured the file live or a
  plugin downloaded it from an archive. A plugin can create a genuine
  `Recording` row with the right `custom_properties` shape and get both
  features for free.
- **Native recordings are already always MKV**, produced by concatenating
  HLS `.ts` segments at the end of a live capture
  (`_dvr_build_hls_concat_cmd`). This is a direct precedent for our
  segmented-download-then-stitch approach (Section 8) and confirms MKV
  output is not a compatibility risk.
- **Plugins have full Django ORM access** and, in principle, can register
  their own periodic Celery task via `core.scheduling.create_or_update_periodic_task`
  (the same mechanism core itself uses for the plugin-repo-refresh
  feature). **Correction, Session 21 — this doesn't reliably work in
  practice**, confirmed on a real deployment, not just reasoned about:
  Dispatcharr's own plugin-discovery-on-`worker_ready` hook
  (`dispatcharr/celery.py`) fires *after* the Celery Consumer has already
  built its dispatch table from `app.tasks`, so a task a plugin registers
  at `worker_ready` time is invisible to that table for the entire life
  of the worker process — the task publishes fine, but the worker
  rejects it as unregistered. Confirmed this wasn't about *how* the task
  was bound (identical failure with `@shared_task` and binding directly
  to the concrete `dispatcharr.celery.app` instance) — it's a timing gap
  in when plugin code gets imported relative to when the Consumer
  snapshots its own dispatch table, not something fixable from plugin
  code without a core change. **Plugin's own polling still works fine**
  — just via a self-contained mechanism the plugin fully controls
  (a background thread + persisted state, Section 3), not Celery's task
  registry. This affects every section below that assumed "register a
  periodic Celery task" as the polling mechanism, not just Section 3.

**What plugin-only genuinely costs us** (accepted tradeoffs):
- No reaction to Dispatcharr's native "EPG just refreshed" trigger for
  *program-finished detection* — that half still polls on its own timer
  rather than being pushed (Section 5). Superseded finding, Session 6:
  the plugin *can* react in real time to a different native event — a
  `Recording` row being created or updated — via an ordinary Django
  `post_save` signal receiver registered from the plugin's own module,
  since Django allows multiple receivers per signal/sender. Used for the
  live-capture takeover in Section 5, not for detecting "program
  finished," which still needs the periodic poll.
- Recordings updated this way are indistinguishable in the UI from a live
  recording unless we deliberately add a marker (open question, Section 7).

---

## Section 3 — Archive Detection `[~] DECIDED`

- Source of truth: `Stream.custom_properties["tv_archive"]` and
  `["tv_archive_duration"]`, already populated by Dispatcharr's own M3U
  sync for Xtream (`XC`) accounts.
- Plugin runs a periodic job (daily, matching Sportarr's own refresh
  cadence) that re-fetches `get_all_live_streams()` per active `M3UAccount`
  and updates its own cached view of which `Stream`s currently have
  `tv_archive > 0`, plus `tv_archive_duration` (retention window in days).
  **Mechanism resolved, Session 21** (superseding the original plan to use
  a Celery `PeriodicTask`, which turned out not to work reliably — see
  Section 2): a daemon background thread, started once when the plugin
  module loads, wakes every 30 minutes and checks a "last completed"
  timestamp persisted in Section 6's SQLite key-value store; runs the
  refresh if 24+ hours have passed (or it's never run), using a simple
  claim/timestamp check to avoid every process's own thread piling on at
  once (best-effort, not a strict lock — a small race window is accepted
  as low-stakes, worst case a harmless redundant API call). Delays 30s
  before its first check, mirroring Sportarr's own `CatchupDownloadService`
  startup delay, since the thread starts during Django's app-loading
  sequence, before every app is guaranteed finished loading.
- Empty/failed fetch → do not clear existing flags (provider hiccup should
  not silently downgrade catchup capability — same reasoning Sportarr
  uses).

**This is now the *only* gate for catchup eligibility** (Section 4,
revised, Session 6) — there is no separate manual per-channel opt-in
layered on top anymore. If a channel's `Stream` has `tv_archive > 0`, any
`Recording` scheduled against it is catchup-eligible.

**Resolved, Session 7:** the plugin reads `Stream.custom_properties`
directly at query time rather than maintaining a denormalized cache in
Section 6's SQLite — it's cheap (a single indexed ORM lookup, not a
network call), and avoids a second source of truth that could drift from
the real M3U sync data between the daily refresh passes. The daily
refresh job (above) is what keeps `custom_properties` itself current;
nothing further to cache on top of it.

---

## Section 4 — Recording Trigger: Reusing Native Scheduling `[~] DECIDED`

**Superseded, Session 6.** The original decision here was manual
per-channel flagging via a plugin-owned settings panel. Dropped entirely
after verifying Dispatcharr's own source — it turns out Dispatcharr
already has a native answer to "what should get recorded" that the plugin
can simply piggyback on, with zero plugin-owned curation UI.

**What was found:** `CoreSettings.get_dvr_series_rules()` backs a real,
already-shipped **Series Rules** feature (`evaluate_series_rules_impl`,
`apps/channels/tasks.py:452-730`) — title/description-matched rules
(exact/contains/regex, optional channel pin, "new episodes only" mode)
evaluated against upcoming EPG programs, which automatically create native
`Recording` rows. There's also `RecurringRecordingRule`
(`apps/channels/models.py:1151`, day-of-week/time-window recurring
schedules) and plain manual EPG-click scheduling. All three already exist,
all three end in the same place: a native `Recording` row.

**New decision:** the plugin does not care *how* a `Recording` row came to
exist — Series Rules, a recurring rule, or a manual click are all treated
identically. Its only question, for any `Recording`, is: **does this
channel's `Stream` have `tv_archive > 0`** (Section 3, fully automatic)?
If yes, the plugin takes over fulfillment via catchup (Section 5). If no,
the recording proceeds exactly as it always has, untouched.

This is also a closer match to Sportarr's actual architecture than the
original manual-flagging design was — Sportarr has no "flag a channel for
catchup" step either; it decides `Method=Catchup` vs. `Method=Live`
per-recording, automatically, off `channel.HasArchive`. The earlier
flagging design existed only because Dispatcharr's plugin doesn't have
Sportarr's own league/team monitoring system to answer "what to record" —
it turns out Dispatcharr has its own native answer to that question
already, just under a different name (Series Rules), so nothing needed
inventing after all.

**What this removes:**
- The plugin's own settings-panel channel curation step, entirely.
- Any risk of the plugin inventing "is this worth recording" heuristics of
  its own — moot, since the plugin never decides what's worth recording
  at all, regardless of content type.

**`comskip_enabled` needs a new home.** The per-channel marker from the
now-dropped flagging mechanism (`custom_properties["catchup_recordarr"]`)
no longer has a natural "flagging moment" to live in. Simplest fix,
applied for v1: a single plugin-wide setting, `comskip_enabled_default`
(bool, default **off**, same opt-in-by-default philosophy as before).
A per-channel override marker is still *possible* later via the same
`custom_properties["catchup_recordarr"]["comskip_enabled"]` shape
(now the only key it holds) if finer control turns out to matter — kept
as an easy follow-up, not designed further now since nothing currently
needs it. Section 7's gating logic is updated to match.

---

## Section 5 — Recording Trigger Detection & Live-Capture Takeover `[~] DECIDED`

**Superseded, Session 6.** Originally this section independently
re-derived "which programs just finished" via its own EPG query across
manually-flagged channels. That's now unnecessary — Section 4 means the
plugin acts on *existing* native `Recording` rows instead of discovering
programs itself, so this section is really about two things: taking over
a `Recording` the moment it's scheduled on a catchup-capable channel, and
polling for when its window has closed so catchup can actually run.

**Part A — real-time takeover (Django signal, not polling).** Verified
Dispatcharr's own scheduling trigger first (`apps/channels/signals.py`)
rather than assuming one was needed:
- `schedule_task_on_save` (`post_save` on `Recording`, lines 317-352)
  is what schedules native live capture — it creates a one-off
  `PeriodicTask`/`ClockedSchedule` (`schedule_recording_task`, lines
  229-267) at `start_time`, *unless* both `start_time` and `end_time` are
  already in the past, in which case it deliberately skips scheduling
  (line 351-352: `"start_time and end_time both in past, not scheduling"`).
  This confirms our original Section 7 design (only ever creating a row
  after the file already exists, so both times are already past) was
  never at risk of double-capture — but it doesn't help for the new
  Section 4 case, where a `Recording` is created *ahead of time* (Series
  Rules matches up to 7 days out), so native live capture *will* be
  scheduled normally unless the plugin intervenes.
- The plugin registers its own ordinary `@receiver(post_save, sender=Recording)`
  handler at plugin load time — Django supports multiple receivers per
  signal/sender, this needs no core change. When a `Recording`'s channel
  has `tv_archive > 0` and a `task_id` was just assigned with a future
  `start_time`, the plugin immediately calls `revoke_task(recording.task_id)`
  (`apps/channels/signals.py:270-293` — the exact same helper Dispatcharr's
  own "cancel this recording" flow uses; deletes the `PeriodicTask` and its
  `ClockedSchedule`, plain ORM, not a hack).
- **Deliberately does not clear `recording.task_id`** after revoking —
  `schedule_task_on_save` only (re-)schedules when `not instance.task_id`,
  so leaving the (now-stale) `task_id` populated is what prevents a later
  save (e.g. the plugin updating `custom_properties` for its own job
  tracking) from accidentally re-triggering native scheduling.
- **Needs empirical confirmation, not just assumed:** this only works if
  Dispatcharr's own `schedule_task_on_save` receiver runs *before* the
  plugin's (so `task_id` exists yet to revoke). Very likely true — plugins
  load after core apps are ready — but flagged as something to verify
  once this is actually built, not asserted as certain here.

**Implementation findings, Session 25 (step 4 build):**
- **A disabled plugin's signals stay connected.** Disabling a plugin in
  the UI doesn't unload its module, so the receiver keeps firing — it
  must check `PluginConfig.enabled` itself before acting, or a disabled
  plugin would keep cancelling captures. Built in.
- **Receiver ordering under load:** the receiver fires on *every*
  `Recording` save, including the ~2s progress writes during an active
  live capture — guards are ordered cheapest-first (`task_id` in memory,
  `start_time` in memory, job-exists in SQLite, then the two Django
  queries) so the hot path exits without touching the DB. It also fires
  at least twice per new Recording (original save + core's nested
  `task_id` save); idempotency via `INSERT OR IGNORE` on the jobs table.
  `dispatch_uid` on the receiver keeps reconnection idempotent across
  plugin reloads.
- **Known gap, deferred to step 6:** recordings scheduled while the
  plugin was disabled (or before install) keep native live capture —
  the signal never saw them. Part B's periodic tick should do a
  catch-up sweep of future-scheduled recordings on capable channels
  that aren't in the jobs table yet. Flagged, not yet built.
- **In-progress recordings are never touched:** the receiver only acts
  when `start_time` is still in the future — revoking a schedule can't
  stop a capture Celery Beat already dispatched, so anything already
  airing keeps its native live path.

**Part B — post-air poll (periodic tick, unchanged cadence, 5–15 min).**
Once a `Recording` has been taken over (Part A), the plugin still needs to
know when to actually pull it from the archive. **Mechanism note, Session
21:** the tick itself should be the same self-contained background-thread
approach as Section 3's archive-flag refresh, not a Celery `PeriodicTask`
— confirmed on a real deployment that plugin-registered Celery tasks
don't reliably work (Section 2). Not yet built; flagging now so this
doesn't get built the Celery way and hit the same wall later.

```
candidates = Recording.objects.filter(
    channel__in = catchup_capable_channels,   # Section 3
    end_time__lte = now - grace_period,
    end_time__gt  = now - lookback_window,
).exclude(<already handled, per Section 6 state keyed by recording.id>)
```

- `grace_period` / `lookback_window`: unchanged in concept from the
  original design (Sportarr-matching ~10-15 min grace; lookback bounded by
  `tv_archive_duration`).
- Program metadata (`title`, `sub_title`, `description`) no longer needs a
  separate EPG lookup at all in the common case — it's already present in
  `Recording.custom_properties["program"]`, populated at creation time by
  whichever native path created the row. **Confirmed, #12/#14:** true for
  Series Rules, `RecurringRecordingRule` (a synthetic dict, but present),
  and any manual recording tied to a specific EPG program entry — only
  genuinely absent for a fully ad-hoc manual recording with no EPG tie at
  all, covered by the fallback below.
- This confirms the feature has no content-type restriction of its own —
  it fires for *whatever the user has scheduled* on a catchup-capable
  channel, sports, news, or anything else in the guide. Scope is entirely
  a function of what Series Rules / recurring rules / manual recordings
  the user actually sets up, never enforced by the plugin.

**Known edge cases (carried over, still not solved in detail):**
- A `Recording` on a catchup-capable channel whose channel has no
  `epg_data` link at all, or a stale EPG source with no matching program
  for the window → **resolved, Session 10:** since the plugin never
  decides *whether* to record (Section 4/5) or *when* (`Recording`'s own
  `start_time`/`end_time`, independent of EPG), a missing/stale EPG link
  only ever affects the *cosmetic* title/description, never eligibility
  or timing. Mirrors Dispatcharr's own native fallback convention rather
  than inventing one — `sync_recurring_rule_impl` already falls back to
  `rule.name or rule.channel.name` when there's nothing better. Same idea
  here: if `custom_properties["program"]` is absent (#12) and a direct
  EPG lookup also comes up empty, synthesize `{"title": f"{channel.name}
  — {start_time:%Y-%m-%d %H:%M}", "description": "Catchup recording"}`
  rather than blocking the fetch.
- Broadcast overruns (real end time later than the scheduled `end_time`)
  → still absorbed by post-padding on the request window, not precise
  overrun detection.
- Timezone: still must convert to the **provider's** local time (Xtream
  `server_info.timezone`) when building the timeshift URL, never
  Dispatcharr's or the user's local time. A wrong conversion here is
  exactly the failure mode Section 10's validation design (Session 13)
  defends against — at this same resolution point, also cross-check the
  auth response's `server_info.timestamp_now` against Dispatcharr's own
  clock; see Section 10 for the full design.

---

## Section 6 — State Tracking `[~] DECIDED`

- **Decision: plugin-owned SQLite file** (not a core Django model
  migration). **Path corrected, Session 23:** originally specified as
  `/data/plugins/<plugin>/state.db` — inside the plugin's own folder.
  That's provably wrong: Dispatcharr's installer
  (`_install_plugin_from_zip`, `apps/plugins/api_views.py`) performs an
  atomic directory swap on every repo-based update and **deletes the old
  folder entirely**, so anything not in the release zip — including a
  state database — is destroyed on every update. Once job/segment state
  lives here, an update mid-download would silently wipe it. Actual
  location: a **sibling** data directory,
  `/data/plugins/catchup_recordarr_data/state.db` — survives updates,
  still on the persisted volume, and ignored by the plugin loader (no
  `plugin.py`/`__init__.py` inside).
- Needs to track, at minimum:
  - Per program: identity — simply the native `Recording.id` now (Section
    4/5, revised, Session 6), since the plugin acts on an existing native
    row rather than inventing its own job identity. Plus overall status
    (`pending` / `in_progress` / `completed` / `failed`), retry count,
    last error.
  - Per segment (see Section 8): index, byte range or time window,
    status (`pending` / `in_progress` / `completed` / `failed`), retry
    count.
- **Idempotency requirement:** the periodic poller (Section 5) must treat
  "in_progress" as claimed — not just "done vs not done" — to avoid
  double-queuing the same program if a poll fires before the previous pass
  finishes.

**Partially built, Session 21:** a minimal piece of this — a plain
key-value `get`/`set` over a `state.db` SQLite file — was pulled forward
into step 2 (Section 15) ahead of schedule, needed to persist the
background scheduler's "last completed" timestamp once the Celery-task
approach was replaced (Section 2/3).

**Schema built, Session 24 (step 3):** full schema now in `state.py` —
`kv` (scheduler state + `schema_version`), `jobs` (PK = native
`Recording.id`, status/retry_count/last_error/timestamps), `segments`
(composite PK recording_id+idx, per-segment window, status, retry count,
downloaded file path; failure returns to `pending` per Section 9's
no-dead-end state machine), and `account_dialects` (Section 8's
per-`M3UAccount` dialect: path/php/unknown, confirmed_at,
consecutive_failures). `PRAGMA foreign_keys=ON` per connection;
`CREATE TABLE IF NOT EXISTS` idempotent DDL with a stored
`schema_version` for future migrations. Schema only — no job/segment
logic wired to it yet, per the build plan.

---

## Section 7 — Recording Integration (Native Playback + Comskip) `[~] DECIDED`

**Updated, Session 6:** the plugin no longer *creates* a new `Recording`
row — per Section 4/5's revised design, a native `Recording` row already
exists (created by Series Rules, a recurring rule, or manual scheduling,
then taken over from live capture). On successful download + stitch +
verification, the plugin **updates that same row in place**, with
`custom_properties` populated to match the shape the native live pipeline
produces:

```
status: "completed"
file_path: <path to final stitched .mkv>
file_name: <filename>
file_url / output_file_url: "/api/channels/recordings/{id}/file/"
ended_at, bytes_written, remux_success
program: { title, sub_title, description, ... }   # already present on the row from creation, Section 5
```

**Comskip gating (updated for Section 4's new `comskip_enabled_default`
setting):** verified native Dispatcharr's own trigger logic first
(`apps/channels/tasks.py:2427-2431`) rather than assuming — after a live
recording finishes, it's a single global check:
`if CoreSettings.get_dvr_comskip_enabled(): comskip_process_recording.delay(recording_id)`.
There is no per-recording granularity natively; it's a system-wide on/off.

The plugin queues `comskip_process_recording.delay(recording.id)` —
the same Celery task the native pipeline calls, unmodified — only when
**both** of these are true:
1. `CoreSettings.get_dvr_comskip_enabled()` is True (the existing global
   Dispatcharr DVR-comskip switch), **and**
2. The plugin's own `comskip_enabled_default` setting (Section 4) is True
   — or the channel's optional `custom_properties["catchup_recordarr"]["comskip_enabled"]`
   override, if one is ever set, takes precedence over the default.

Chosen deliberately over letting the plugin's flag act alone: if an
operator has turned comskip off system-wide (not installed, or a
deliberate choice), a catchup recording silently running it anyway would
be a surprising override of stated intent — consistent with this
project's running rule of extending existing settings rather than routing
around them (Section 2, Section 8's UA resolution). Comskip's own
behavior (cut vs. mark mode, hardware accel, custom `.ini`) already comes
entirely from the same global `CoreSettings` the native pipeline reads —
nothing to duplicate there, the plugin only adds the extra gate before
deciding whether to call it at all.

**Critical ordering rule:** do NOT update the row's status to `completed`
(or otherwise make it look finished) until the file is fully downloaded,
stitched, AND verified (Section 9/10). The playback endpoint only checks
`os.path.exists` + non-zero size, not validity — exposing a partial or
corrupt file early would serve a broken recording to the user. Between
takeover (Section 5, Part A) and a successful catchup fetch, the row's
status should read as something in-progress/pending, never `completed`
prematurely.

**Visual distinction (resolved, Session 11): a small `"[Catchup] "` title
prefix**, e.g. `"[Catchup] Monday Night Football"` — the user wants a
light "nice to have" tag, kept plugin-only (no core/frontend change).
Checked both `RecordingCard.jsx` and `RecordingDetailsModal.jsx` first:
neither has a generic "render any custom property" surface — every
displayed field (title, `sub_title`, description, stats) is hardcoded to
a specific key, so there's no way to surface a new marker without editing
frontend source, which is off the table here (a deliberate, narrower
constraint than Section 1's general rule — this feature specifically
stays plugin-only even as a small exception, unlike the badge option
considered and rejected). Title over `sub_title`: `sub_title` only
renders when non-empty and is frequently blank (movies, specials, no
episode info) — using it as the marker would make the tag disappear for
exactly those recordings and would overwrite a field meant for real
episode subtitles when one exists. Title always renders. Cost is
trivial — a single string format at the point the plugin already builds
`custom_properties["program"]["title"]` (above) — clears the "skip if
expensive" bar easily.

---

## Section 8 — Timeshift URL Construction `[~] DECIDED`

**Verified against Sportarr's actual shipped source** (`CatchupDownloadService.cs`,
`XtreamCodesClient.cs`, `XtreamTimeshiftTests.cs` — cloned and read directly,
not inferred from docs), so the items below are confirmed against a live,
working implementation rather than ported secondhand.

**URL dialects** (byte-for-byte confirmed via Sportarr's unit tests):

- **Path style:** `{server}/timeshift/{user}/{pass}/{duration}/{start}/{streamId}.ts`
- **PHP style:** `{server}/streaming/timeshift.php?username=...&password=...&stream=...&start=...&duration=...`

`start` format: `yyyy-MM-dd:HH-mm`, in the **provider's** local time,
credentials URL-escaped. **Must be formatted with an invariant culture** —
Sportarr's own code comments flag that `:` is the culture-sensitive
time-separator specifier in .NET custom format strings, so a non-invariant
host locale can silently corrupt the URL. Python/Django's default string
formatting isn't locale-sensitive this way, but the equivalent risk for us
is any implicit locale-aware date formatting — build this string manually
or with an explicit format call, never a locale-aware strftime default.

**Per-account dialect state:** one row per `M3UAccount` in the plugin's
SQLite (Section 6) — not per-channel, since the dialect is a property of
the provider's panel software, not the individual stream: `dialect`
(`path` / `php` / `unknown`), `confirmed_at`, `consecutive_failures`.

**Cold start default:** when `unknown`, try `path` first (matches
Sportarr's own default — "most panels use it," though this is a soft
default that self-corrects on first success, not a hard finding).

**Fallback algorithm** (simplified to match Sportarr's proven behavior,
which is more binary than earlier drafts of this section proposed):

1. Attempt the account's current preferred dialect.
2. Classify the attempt as **failed** if either: the request/transfer
   errors out for any reason (connection error, timeout, HTTP error), OR
   it completes but the final downloaded size is **under 1MB** — Sportarr's
   own comment: *"A 2xx response with a tiny body is how panels signal
   'window not in archive' without an HTTP error."* Sportarr does not
   distinguish "wrong dialect" from "archive not ready yet" as separate
   failure types — both collapse to the same "failed, try the other
   dialect" path, and that's proven to work in production. An earlier
   draft of this section proposed a more granular DIALECT_FAILURE vs.
   NOT_READY split; dropped in favor of matching what's actually shipped.
3. On failure, immediately retry the same request with the other dialect.
4. If the second dialect succeeds, flip the account's preferred dialect
   and reset `consecutive_failures` — this is how a stale or wrong
   detection self-heals without user intervention.
5. If both dialects fail, that counts as one real failure at whatever
   granularity we're operating at — under this project's segmented-download
   design (Section 9, a deliberate divergence from Sportarr's single
   whole-file pull), that's a **segment** failure, consuming one retry-cap
   slot per Section 9's per-segment cap. The one-shot dialect swap in step
   3 itself does not consume a retry-cap slot — it's dialect discovery,
   not a real failure of a known-good path.

**"Not ready" threshold:** fixed at 1MB for v1, matching Sportarr's
hardcoded constant exactly rather than inventing our own number. Not
exposed as a user setting for now — revisit only if real-world testing
shows a provider needs a different cutoff.

**User-Agent:** resolve via `M3UAccount.get_user_agent().user_agent` —
the same call Dispatcharr's own live-viewing proxy path uses
(`apps/proxy/live_proxy/url_utils.py`) — for every outbound request the
plugin makes to a provider (archive-flag refresh in Section 3, the
auth/timezone lookup in Section 5, and the timeshift GET itself). This
resolves to the account's configured `UserAgent` if the user set one,
else `CoreSettings`'s global default, else falls through to Dispatcharr's
own hardcoded `VLC/3.0.20 LibVLC/3.0.20` (`apps/proxy/config.py`) — all
already-existing Dispatcharr mechanisms, no new plugin setting needed.

Deliberately **not** `dispatcharr_dvr_user_agent(recording_id)` (the
`"Dispatcharr-DVR/recording-{id}"` string native DVR capture sends) —
traced that one to `apps/channels/tasks.py`, where it turns out to be an
internal-only identifier between Dispatcharr's own ffmpeg client and its
own local TS proxy (`stream_url = f"{base}/proxy/ts/stream/{channel.uuid}"`);
it never reaches the actual provider, because the live-proxy layer
sitting in front of it re-authenticates upstream using the VLC-spoofed UA
above. Our plugin has no such internal-proxy hop — the timeshift request
goes straight to the provider — so the internal identifier is the wrong
precedent and would likely get the request dropped by panels that reject
unrecognized clients. Confirmed no per-`M3UAccountProfile` UA override
exists to reconcile against — UA is account-level only.

This independently matches Sportarr's own pattern
(`source.UserAgent ?? "VLC/3.0.18 LibVLC/3.0.18"`, applied consistently
across every provider-facing call including the catchup download) — two
separate implementations landing on "per-account override, VLC-spoofed
default" is a good signal this is the right shape, not just a guess.

---

## Section 9 — Segmented Download `[~] DECIDED (design only)`

**Segmenting:** a program is split into fixed-size chunks (e.g. 15 or 30
min), fetched **sequentially, one at a time** — never concurrently (see
"Turbo mode — considered and rejected" below). This is still a real
improvement over Sportarr's shipped implementation (a single whole-file
ffmpeg pull per attempt): a chunk failing only needs that chunk retried,
not the whole multi-hour archive pull, and there are natural resume points
if a worker restarts mid-download. It costs nothing extra versus Sportarr
in terms of provider load — still exactly one connection to the provider
at a time, same as a whole-file pull.

Native live DVR recording (HLS `.ts` segments concatenated into the final
MKV, `_dvr_build_hls_concat_cmd`) was cited in earlier drafts as a "direct
precedent" for this — that framing overstates it. Native HLS segments are
delivered by the provider as discrete, keyframe-aligned files; ours are
not. Each catchup segment is an *independently constructed* timeshift
request (its own `start`+`duration` window, Section 8) against an archive
endpoint with no obligation to align its internal encoding to cut points
we chose. See the stitch-boundary risk below — this is a real difference
from the native precedent, not just a labeling nitpick.

**Segment state machine** (stored in Section 6's SQLite):
`pending → in_progress → completed`, or on failure, **`in_progress → pending`**
(not a dead-end `failed` state) — so the same "claim next unclaimed
segment" logic naturally retries it on the next pass.

**Per-segment retry cap (resolved, Session 9): 5 attempts, hardcoded.**
No direct Sportarr precedent here — its `MaxAttemptsPerChannel = 4` is a
whole-job cap (it doesn't segment), a different granularity that doesn't
port directly. A segment failure only counts once *both* dialects have
failed (Section 8 — the dialect swap itself is free). No separate backoff
timer needed on top of the cap: the existing 5-15 min poll cadence
(Section 5) already spaces retries out naturally, so an explicit backoff
would be redundant. At that cadence, 5 attempts is roughly 25-75 minutes
of retrying — enough to survive a transient provider blip, not enough to
silently grind for hours on one bad chunk. Hardcoded rather than a plugin
setting, matching Section 8's "not ready" threshold precedent — add
configuration surface only if real-world testing against a provider shows
it's needed, not pre-emptively. When the cap is hit, mark the *whole job*
failed with a specific reason (e.g. "segment 4 of 9 failed after 5
attempts: `<last error>`"), rather than waiting on the retention-based
cutoff (Section 10) to eventually notice.

**Turbo mode (concurrent segment downloads) — considered and rejected for
v1.** Discussed in depth and deliberately dropped, not just deferred for
lack of time:

1. **Unproven speed gain.** Sportarr's own reasoning for staying
   single-threaded — *"a single archive pull already saturates most
   providers' per-connection throughput"* — implies the bottleneck on many
   panels is per-account bandwidth, not available connection slots. If
   true, two concurrent segment fetches split the same total bandwidth
   rather than doubling throughput, buying nothing for the added
   complexity and risk.
2. **Competes with a scarce, shared resource.** A second connection slot
   consumed by a catchup download is a slot unavailable for live viewing
   elsewhere in the household — a real cost even where the speed gain
   turns out to be genuine.
3. **Some panels reject a second simultaneous connection to the same
   `stream_id`/archive URL** even when the account's overall connection
   limit allows it — provider behavior we can't determine from
   Dispatcharr's code, would need clean single-threaded fallback either
   way.
4. **Compounds the stitch-boundary risk below.** Fetching neighboring
   segments out of order/in parallel makes any future boundary-correction
   logic (e.g. overlap-and-trim) harder to reason about than a strictly
   sequential "fetch N, then N+1."

Not ruled out forever — worth revisiting only if empirically validated
against a specific real provider showing genuine per-connection headroom,
not assumed up front.

**Segment boundary / stitch risk (parked, Session 14 — see Section 13):**
not just a turbo-mode concern, applies to sequential segmenting too —
because each segment is its own independently-requested archive window
rather than a provider-delivered keyframe-aligned chunk, consecutive
segments aren't guaranteed to concatenate cleanly, a dropped frame or
brief AV desync at the seam is possible where our chosen cut point doesn't
land on a provider-side keyframe boundary. Candidate mitigations (small
deliberate overlap between segments with trim-at-stitch, or accepting the
same `-fflags +genpts -avoid_negative_ts make_zero` treatment Sportarr
applies at remux time, Section 8/10 — which fixes monotonic
PTS/discontinuity but does **not** by itself guarantee no dropped frames
at a non-keyframe cut) were identified but not chosen. Deliberately not
resolved further here — this can't be settled by more design discussion,
only by testing segment concatenation against a real provider once
there's an actual build to test. See Section 13.

**Segment-level orphan recovery (resolved, Session 9): no timeout/TTL —
reuse the job-level crash-recovery pattern one level deeper.** Sportarr's
own job-level recovery reasoning (cited in Section 10) is: *"Downloads run
synchronously inside this tick... a catchup row already in Recording state
when a tick STARTS can only be a leftover from an app crash/restart
mid-download"* — no timeout math, just "still in-progress at the start of
a new tick is unambiguously orphaned." That applies identically to
segments, since Section 9 already committed to sequential (non-concurrent)
processing — a job's segments are worked through within one continuous
task invocation, so a segment can only still be `in_progress` when a *new*
tick begins if the worker that claimed it crashed or was killed before
finishing, never because it's "still legitimately working." At the start
of each periodic tick, before claiming new work, reset any segment found
`in_progress` back to `pending`. Deliberately not a separate mechanism
from the job-level one — same idea, applied at both granularities,
instead of two different recovery strategies doing the same job.

---

## Section 10 — Failure, Retry & Crash Recovery (Job-Level) `[~] DECIDED (design only)`

Native live-recording precedent studied for reference (not reused code,
since the plugin can't hook into core's Redis-lock recovery flow — this is
a plugin-owned reimplementation of the same *ideas*):

- Native: per-recording Redis lock with ~45s TTL; on app restart,
  `recover_recordings_on_startup` finds recordings mid-window, marks them
  `interrupted`, and resumes. Terminal states: `completed`, `interrupted`,
  `stopped`, `failed` — plain values in `custom_properties["status"]`.

**Plugin's equivalent, at the whole-job level:**
- A per-job lock with a TTL, so a dead worker's job can be picked back up
  by the next poll rather than double-processing.
- A retry counter with backoff between attempts.
- **A hard cap tied to archive retention**: once the requested window falls
  out of the channel's `tv_archive_duration` retention, stop retrying and
  mark the job permanently failed (with reason) rather than retrying
  forever. Sportarr pairs this with fallback-channel rotation (retry on a
  different channel airing the same event); **resolved out of scope for
  v1, Session 12** — Sportarr can do that because it has its own event
  database decoupled from any one channel; our plugin has no such concept
  (a native `Recording` is tied to exactly one channel, and Dispatcharr
  has no notion of "this program is the same broadcast on another
  channel"). Building one would mean re-inventing plugin-owned
  curation — exactly what Session 6 removed. See Section 13 for the
  future-consideration note.
- Failure surfaced by setting `custom_properties["status"] = "failed"`
  with a reason string on the taken-over `Recording` row (Section 4/5/7,
  revised Session 6 — no longer a row the plugin created itself) —
  renders correctly through the existing native UI with zero UI changes
  needed, since it's the same field the native pipeline uses.

**Validation against silently-wrong content (resolved, Session 13).**
Checked Sportarr's precedent first — it has none: its `MediaFileInspector`
only runs `ffprobe` for quality/codec scoring, never to validate a
recording captured the right window, and it parses the Xtream auth
response's `server_info.timestamp_now` into its client model but never
actually uses it anywhere. Genuine content validation ("is this really
the right broadcast") needs video/audio understanding and is out of
scope; three mechanically-detectable checks catch the realistic failure
symptoms instead, using data/tools already in the pipeline:

1. **Account-level clock/timezone sanity check** — the one that targets
   the actual worrying failure mode (a timezone bug producing a
   technically-successful download of the *wrong* window) directly,
   rather than a proxy for it. At the same point Section 5 resolves the
   provider's local timezone from the Xtream auth response
   (`server_info.timezone`), also compare that response's
   `server_info.timestamp_now` (present in the API shape, confirmed via
   Sportarr's own client model — just unused there) against Dispatcharr's
   own current UTC time. Normal clock drift is seconds to a couple
   minutes; a timezone-resolution bug shows up as tens of minutes to
   hours — easily distinguishable. Beyond a ~10-15 min tolerance, log a
   loud **account-level** warning (Section 14) — distinct from any single
   job's failure, since every catchup download for that account is
   suspect if this check fails, not just one. Best-effort: skip silently
   if `timestamp_now` is missing or zero rather than treating absence as
   a failure, since not every panel may report it reliably.
2. **Per-download duration check** — after stitching, `ffprobe` the final
   file and compare measured duration against the recording's own
   expected window (`end_time - start_time`), tolerance ±5% or ±2
   minutes (whichever is larger, to absorb segment-boundary rounding).
   Catches truncation and silent mid-download data loss; does not alone
   catch a "right duration, wrong window" shift — check 1 targets that.
3. **Per-download playability check** — `ffprobe` must successfully parse
   the file and report at least one valid video stream; a garbled/corrupt
   result fails this even if size and duration both look fine.

Failures of checks 2/3 are ordinary segment/job failures within the
existing retry-cap machinery (Sections 9/10) — log both expected and
actual values for diagnosability, retry since they may be transient.
Failure of check 1 is systemic — flag the account/source, not one job.

---

## Section 11 — Output Format `[~] DECIDED`

Final output is always MKV, produced by the same segment-concatenation
step as Section 9. This is not a new convention — it matches what native
Dispatcharr recordings already produce (raw `.ts` is never the final
format even for live capture), so there's no compatibility risk with
playback, comskip, or the Recordings UI.

---

## Section 12 — Explicitly Open Questions (Consolidated) `[!]`

Pulling every open item from above into one list, for quick scanning at
the start of a new session:

1. Read `custom_properties` live vs. cache archive flags in plugin's own
   state store? (Section 3)
2. ~~Fallback behavior for a catchup-eligible channel with no/stale EPG
   data?~~ **RESOLVED, Session 10** — only ever affects title/description
   cosmetics, never eligibility or timing; falls back to the channel name
   + time window, mirroring Dispatcharr's own native convention
   (`sync_recurring_rule_impl`). See Section 5.
3. ~~Should catchup recordings be visually distinguishable from live
   recordings anywhere in the UI?~~ **RESOLVED, Session 11** — a small
   `"[Catchup] "` title prefix, plugin-only, no frontend change (a real
   badge like "Recurring"/"Series" was considered and rejected — it would
   require editing `RecordingCard.jsx`). See Section 7.
4. ~~Exact retry/fallback ordering + "not ready yet" detection heuristic for
   the two timeshift URL dialects?~~ **RESOLVED** — see Section 8 (verified
   against Sportarr's shipped source: binary fail/retry-other-dialect,
   1MB threshold, per-`M3UAccount` dialect state).
5. ~~Per-segment retry cap value?~~ **RESOLVED, Session 9** — 5 attempts,
   hardcoded, no separate backoff (the existing poll cadence already
   spaces retries). See Section 9.
6. ~~Segment-level orphan/stuck-in-progress recovery: timeout-based
   auto-requeue, or wait for next poll?~~ **RESOLVED, Session 9** — no
   timeout/TTL; reuse Sportarr's own job-level "still in-progress at tick
   start = orphaned" pattern one level deeper, since segments process
   sequentially within one tick. See Section 9.
7. ~~Is fallback-channel rotation (switching to a different archive-capable
   channel after repeated failures, as Sportarr does) in scope for v1, or
   deferred?~~ **OUT OF SCOPE, Session 12** — no data-model equivalent to
   Sportarr's event database exists in our architecture; would require
   re-inventing plugin-owned channel curation. Moved to Section 13 as a
   future consideration rather than a live open question. (Section 10)
8. ~~How to validate a "successful" download actually captured the right
   window/content, not just that ffmpeg exited cleanly?~~ **RESOLVED,
   Session 13** — three mechanical checks, no Sportarr precedent existed
   for this: an account-level clock/timezone sanity check against the
   Xtream auth response's `server_info.timestamp_now` (targets the actual
   risk directly), a per-download `ffprobe` duration check, and a
   per-download `ffprobe` playability check. See Section 10, cross-
   referenced from Section 5.
9. ~~Sportarr's shipped implementation doesn't segment or run concurrent
   catchup downloads at all — how does this reconcile with Section 9's
   segmented + turbo-mode design?~~ **RESOLVED** — deliberate divergence,
   not an oversight: keep sequential segmenting (real improvement over
   Sportarr, same provider load), drop turbo mode/concurrency entirely for
   v1 (unproven speed gain, competes with a scarce shared connection
   resource, some panels reject concurrent same-stream connections). See
   Section 9.
10. **Segment boundary/stitch risk** — each catchup segment is an
    independently-requested archive window, not a provider-delivered
    keyframe-aligned chunk like native HLS segments are, so consecutive
    segments aren't guaranteed to concatenate cleanly. **Parked, Session
    14** — moved to Section 13, not resolved here: unlike this list's
    other items, this one can't be settled by more design discussion,
    only by testing segment concatenation against a real provider once
    there's something to build against. (Section 9)
11. ~~Must confirm Dispatcharr's own `schedule_task_on_save` signal
    receiver runs *before* the plugin's own `post_save` receiver on
    `Recording`, so `task_id` exists yet to revoke at takeover time.~~
    **RESOLVED, Session 8** — confirmed via `INSTALLED_APPS`
    (`dispatcharr/settings.py`): `apps.channels.apps.ChannelsConfig` loads
    well before `apps.plugins`, and `ChannelsConfig.ready()` connects
    `schedule_task_on_save` at that point. Plugin discovery (and thus our
    own receiver registration) happens in `PluginsConfig.ready()` or, for
    Celery workers, even later at `worker_ready` — strictly after, in
    every process type. Django dispatches receivers in connection order,
    so Dispatcharr's receiver is guaranteed to run first, in every case.
    Defensive coding kept anyway: the plugin's receiver should still check
    `if not instance.task_id: return` (log a warning) rather than assume,
    as a clean failure mode if a future Dispatcharr version ever reorders
    `INSTALLED_APPS`. (Section 5)
12. ~~Does a plain manually-scheduled `Recording` also populate
    `custom_properties["program"]` the same way `evaluate_series_rules_impl`
    does?~~ **RESOLVED, Session 8** — the real dividing line isn't
    "manual vs. Series Rule," it's whether the recording was tied to a
    specific EPG program entry at all (`RecordingSerializer.validate()`,
    `apps/channels/serializers.py:745-805`, gates its pre/post-offset
    logic on `isinstance(cp.get("program"), dict)` regardless of who
    created the row). Confirmed further by #14 below: even
    `RecurringRecordingRule` recordings, which are pure time-window based,
    get a synthetic `program` dict. So this only comes up empty for a
    fully ad-hoc manual recording with no EPG tie and no recurring rule
    behind it — narrower than originally assumed. Section 5 Part B's
    planned fallback (direct EPG lookup via `_match_epg_program_by_timeslot`,
    falling back further to just the channel name) already covers this;
    no design change needed. Also confirmed in passing: this endpoint
    hard-rejects `end_time < now`, so manual scheduling — like Series
    Rules — can never target something already aired. (Section 5)
13. ~~What `custom_properties["status"]` value should a taken-over
    `Recording` show between revoke and a successful catchup fetch?~~
    **RESOLVED, Session 8** — checked the actual frontend badge logic
    (`frontend/src/components/cards/RecordingCard.jsx:104-110, 328-346`).
    Before `start_time`, the card shows "Scheduled" purely from a time
    comparison (`isUpcoming = isBefore(now, start)`), regardless of
    `status` — so nothing needs setting while waiting for air time.
    During the original broadcast window
    (`start_time ≤ now < end_time`), leaving `status` unset falsely shows
    "Recording" (live capture appears to be happening). Of the values
    that suppress that, `"completed"` is forbidden (Section 7's ordering
    rule) and `"stopped"` renders even worse (falls through to a
    "Completed" badge with no file). **Use `"interrupted"`**, set only
    once `start_time` has passed — not at takeover time, since
    `isInterrupted` has no time-gating of its own and would show an alarming
    "Interrupted" badge days before air if set early. Pair it with a
    friendly `custom_properties["interrupted_reason"]` (the component
    already has a display slot for this, line 488) — e.g. "Will be
    fetched from the provider's catchup archive after the broadcast
    ends" — instead of it reading like an unexplained crash. Implemented
    as a small addition to Section 5 Part B's existing periodic tick:
    each pass also flips any taken-over recording whose `start_time` has
    just passed (but not `end_time`) to this state. (Section 5/7)
14. ~~Confirm `RecurringRecordingRule`-driven recordings end up as plain
    `Recording` rows the same way Series Rules and manual scheduling do.~~
    **RESOLVED, Session 8** — `sync_recurring_rule_impl`
    (`apps/channels/tasks.py:840-930`) ends with a plain
    `Recording.objects.create(...)`, the same ORM path (and thus the same
    `post_save` signal dispatch) as every other creation route. No
    special case needed in Section 5's takeover logic. Also confirmed:
    this path sets `custom_properties["status"] = "scheduled"` literally
    at creation (real precedent for that string, though it doesn't change
    #13's answer since it's set before `start_time` anyway), and the
    frontend reads `custom_properties.rule.type === 'recurring'` for its
    "Recurring" badge — a practical note for later: the plugin must merge
    `custom_properties` on update, not overwrite it, so it doesn't
    accidentally strip that marker. (Section 4/5)

---

## Section 13 — Explicitly Deferred to Future / Core Integration `[ ] NOT STARTED`

Ideas raised and consciously not pursued in v1, kept here so they aren't
re-litigated from scratch later:

- Auto-detection of "is this channel/program worth recording" by
  category or name (rejected — Section 1/4; the plugin never makes this
  call itself, it only acts on what the user has already scheduled via
  Dispatcharr's native recording features). Note: this item's original
  wording referenced "manual flagging, Section 4" — that mechanism was
  dropped entirely in Session 6 in favor of reusing native scheduling;
  fixed here while already editing this section for unrelated
  sports-specific wording, Session 16.
- Native per-channel UI toggle, `capture_method` field on `Recording`,
  scheduler-level integration — all would require core Dispatcharr
  changes; explicitly deferred until the plugin is proven (Section 2).
- Any core migration for archive flags (currently relying entirely on the
  already-populated `Stream.custom_properties`).
- **Fallback-channel rotation** (Session 12, from Section 12 #7 /
  Section 10): Sportarr retries a failed catchup on a different channel
  airing the same event, using its own event database to know the two
  channels are related. Not pursued here — Dispatcharr has no concept of
  "this program is the same broadcast on another channel," and building
  one would mean re-inventing plugin-owned curation, the exact thing
  Session 6 removed. Revisit only if this turns out to matter in
  practice, e.g. if core ever gains an event/broadcast abstraction
  independent of channel — not worth designing speculatively now.
- **Segment boundary/stitch risk** (Session 14, from Section 12 #10 /
  Section 9): different reason for being here than the items above — not
  a core-change requirement or a rejected idea, just something that can't
  be settled by more design discussion. Each catchup segment is its own
  independently-requested archive window, not a provider-delivered
  keyframe-aligned chunk the way native HLS segments are, so consecutive
  segments aren't guaranteed to concatenate cleanly at the seam.
  Candidate mitigations (small overlap-and-trim, or accepting Sportarr's
  genpts/avoid_negative_ts remux treatment, which helps timestamp
  continuity but doesn't by itself guarantee no dropped frames at a
  non-keyframe cut) were identified but not chosen. Revisit once there's
  an actual plugin build to test segment concatenation against a real
  provider — this genuinely can't be resolved on paper.

---

## Section 14 — Logging & Observability Philosophy `[~] DECIDED`

**Why this gets its own section:** this plugin is a headless, poll-driven
background system (Section 5) — there's no request/response cycle a user
is watching when something goes wrong. A finished game that silently
failed to download is only diagnosable after the fact, so logging isn't
incidental, it's the primary troubleshooting surface. Called out explicitly
as a cross-cutting design rule so it doesn't get treated as an
afterthought once building starts.

**Reuse Dispatcharr's existing logging plumbing — don't invent our own**,
consistent with the plugin-first philosophy in Section 2:

- Standard `logger = logging.getLogger(__name__)` per module, the same
  idiom every Dispatcharr app module already uses
  (`apps/channels/tasks.py`, `apps/m3u/tasks.py`, `apps/epg/tasks.py`,
  `apps/plugins/loader.py`). No custom handler, no separate log file, no
  separate rotation policy — Dispatcharr's `LOGGING` config
  (`dispatcharr/settings.py`) routes everything to the console handler via
  the root logger, and an unconfigured logger name (ours will be, since
  it's not one of the explicitly-listed loggers) still reaches that
  handler through normal Python logging propagation. Verified this
  actually works rather than assuming it.
- **Respect the operator's existing `DISPATCHARR_LOG_LEVEL` env var**
  instead of adding a separate plugin-specific global log level setting —
  one knob to turn, not two to keep in sync.

**Always self-identify every log line with a short tag**, e.g. `[Catchup]`
(mirrors Sportarr's own convention) — this is not optional stylistic
preference, it fixes a real gotcha found while tracing the plugin loader:
`PluginManager._build_context()` (`apps/plugins/loader.py`) hands every
plugin's `run()`/`stop()` call a `context["logger"]` that is literally the
loader's own shared module logger (`apps.plugins.loader`) — not a
per-plugin one. If our plugin ever uses that handed-in logger directly for
an action call, its lines are indistinguishable from any other installed
plugin doing the same in a multi-plugin install. Our own periodic
background-thread module (Section 2/3) gets a naturally unique
`__name__`-based logger already, so this mainly matters for action-handler
code paths — but tag every line either way for consistency and
grep-ability, rather than depending on which code path happened to produce
it.

**Refinement, Session 21: include the plugin version in the tag**, e.g.
`[Catchup v0.5.0]`, sourced from one shared constant (`_version.py`) —
came directly out of a real debugging session where it was genuinely
unclear whether a test was exercising newly-pushed code or a still-running
older version cached in a process's memory (updating plugin files on disk
doesn't reload code a process already imported; only a restart does).
Cheap to add, removes that entire class of ambiguity from future
debugging.

**Structured, greppable context in every message — never a bare string.**
Both reference implementations do this without exception; make it a hard
rule here too:

- Sportarr: `"[Catchup] Recording {Id} ('{Title}') ... "`
- Dispatcharr's own DVR task: `f"DVR recording {recording_id}: ..."`

Every log line from this plugin should carry at minimum the job/segment
identity it concerns (channel name, EPG program title, our own job id from
Section 6's state store) — a log line that just says "download failed"
with no identifying context is useless once more than one job is in
flight.

**Log at every point that matters for after-the-fact troubleshooting**,
matching what Sportarr's `CatchupDownloadService` does at each step:

- Tick start/end summary (archive flags refreshed, programs found due,
  segments claimed) at INFO.
- Every state transition (`pending → in_progress → completed/failed`,
  Sections 6/9/10) at INFO.
- Every retry or fallback (dialect swap from Section 8, segment retry,
  fallback-channel rotation from Section 10) at WARNING, with the reason
  — not just that it happened.
- Permanent failures at ERROR, with full reason and enough identity to
  find the failed `Recording` row without cross-referencing anything else.
- Successful completion at INFO with size/duration, same as Sportarr logs
  `(recording.FileSize / 1e9)` GB on success.

This should mirror what's persisted in Section 6's SQLite state, not
duplicate a separate taxonomy — every state-store write gets a matching
log line, so troubleshooting can use either the logs or a state-store
query interchangeably.

**Never log credentials.** The constructed timeshift URL (Section 8)
embeds the Xtream username/password directly in the path or query string.
Checked Sportarr's own logging calls for this — its `CatchupDownloadService`
logs recording id, title, channel name, window times, and output path, but
**never the constructed URL itself** — confirming this is already a
deliberate omission in the reference implementation, not an oversight to
copy blindly. Adopt explicitly as a rule for us: log a redacted form
(host + stream id + time window, credentials masked) if the URL itself
ever needs to appear in a message, e.g. for the dialect-fallback log line.

**Optional, not yet needed:** a plugin setting for extra-verbose segment-
claim/retry detail at DEBUG, independent of Dispatcharr's global log
level — worth adding only if v1 turns out to need finer-grained tracing
than the above provides; not designing this preemptively.

---

## Section 15 — Build Plan (Iterative Steps) `[ ] NOT STARTED`

Sequenced so each step is a safe pause/resume point — build, verify, stop
if needed, pick back up next session by pointing at this list and saying
which step number is next. Ordered by dependency, not by section number;
each step names the design section(s) it implements.

**Phase A — Scaffolding**
1. Plugin folder structure + `plugin.json` manifest + minimal `Plugin`
   class (name/version/description, no real logic yet). Verify: plugin
   appears, loads, and shows enabled in Dispatcharr's plugin list.

**Phase B — Read-only foundations (safest possible first real features)**
2. Archive detection (Section 3): daily periodic task reading
   `Stream.custom_properties["tv_archive"]`/`tv_archive_duration` per
   `M3UAccount`, logged per Section 14's conventions. Purely read-only,
   no side effects — easy to verify against known test data.
3. SQLite state store schema (Section 6): tables/models for per-recording
   job state, per-segment state, per-`M3UAccount` dialect state (Section
   8). Schema only at this step — no logic wired to it yet.

**Phase C — Recording takeover (Section 4/5 Part A)**
4. `post_save` signal receiver on `Recording`: detect catchup-capable
   channel + future `start_time`, call `revoke_task()`, defensive
   `if not instance.task_id` check (#11), log clearly. Verify: schedule a
   test recording on a catchup-capable channel, confirm its native
   `PeriodicTask` gets deleted.
5. Status-transition tick (#13): flip taken-over recordings to
   `"interrupted"` + friendly `interrupted_reason` once `start_time`
   passes, not at takeover. Verify against the actual frontend badge
   rendering.

**Phase D — Post-air detection (Section 5 Part B)**
6. Periodic poll for taken-over recordings whose `end_time` + grace has
   passed (Section 3's catchup-capable filter, Section 6's "already
   handled" exclusion). At this step: detect and log only, no download
   yet — verify the right recordings get picked up before wiring in any
   network calls.

**Phase E — Provider communication (Section 8)**
7. Timeshift URL builder: both dialects, invariant-culture date
   formatting, credential escaping.
8. User-Agent resolution (`M3UAccount.get_user_agent()`) and provider
   timezone resolution (`server_info.timezone`), including the
   `timestamp_now` clock-sanity check from Section 10.
9. Dialect fallback/retry logic with per-account persisted state
   (path/php, `consecutive_failures`, self-healing swap on success).

**Phase F — Segmented download (Section 9)**
10. Segment planning: split a job's window into fixed-size chunks.
11. Single-segment fetch using steps 7-9's URL/dialect logic, plus the
    1MB "not ready" threshold check (Section 8).
12. Segment state machine: `pending → in_progress → completed`/`pending`,
    5-attempt retry cap, orphan recovery (reset `in_progress` at tick
    start, Section 9).
13. Stitching: concatenate completed segments into the final MKV.

**Phase G — Validation (Section 10)**
14. Post-stitch `ffprobe` duration + playability checks.
15. Job-level retry cap tied to archive retention; permanent failure
    marking with reason.

**Phase H — Recording integration (Section 7)**
16. Update the taken-over `Recording` row in place on success:
    `custom_properties` shape, `"[Catchup] "` title prefix, merge (not
    overwrite) to preserve existing markers (#14).
17. Comskip gating: global `CoreSettings` switch AND plugin's
    `comskip_enabled_default` setting.

**Phase I — Polish**
18. Plugin settings fields: `comskip_enabled_default`, grace period,
    lookback window, any others surfaced along the way.
19. Logging audit pass: confirm every step above actually followed
    Section 14's conventions (tags, structured context, no credentials
    logged) rather than assuming it did while focused on functionality.

**Phase J — Real-world validation**
20. End-to-end test against a real provider account — this is where
    Section 9's parked stitch-boundary risk (Section 13) actually gets
    tested for the first time.

**Natural milestone checkpoints** (good "if I had to stop for a while,
stop here" points): after step 1 (plugin loads), after step 6 (takeover +
detection fully working, nothing downloaded yet), after step 13 (a real
file gets produced), after step 17 (a finished catchup recording is
fully indistinguishable-but-tagged and playable in Dispatcharr).

---

## Session Log

*(Append an entry each session — date, what was decided/built, what's next.
This is the authoritative "what actually happened" record if it ever
diverges from the sections above.)*

- **Session 1** — Investigated Sportarr's `CatchupDownloadService`
  architecture and Dispatcharr's existing DVR/EPG/M3U code in detail.
  Established plugin-first feasibility (Section 2), confirmed archive
  flags are already captured in `Stream.custom_properties`, confirmed
  playback/comskip are capture-method-agnostic. Talked through channel
  flagging (manual), EPG-driven completion detection, segmented download +
  stitching (mirroring native HLS concat), turbo mode (2-thread capped
  concurrent segment download), segment failure requeueing, and job/segment
  retry-and-recovery design. No code written yet. **Next:** resolve the
  open questions in Section 12 as they come up during build, starting with
  archive detection (Section 3) and channel flagging (Section 4) as the
  first buildable pieces.

- **Session 2** (2026-07-05) — Cloned and read Sportarr's actual shipped
  source (`CatchupDownloadService.cs`, `XtreamCodesClient.cs`,
  `XtreamTimeshiftTests.cs`) and Dispatcharr's own source
  (`apps/m3u/models.py`, `apps/proxy/live_proxy/url_utils.py`,
  `apps/channels/tasks.py`, `core/models.py`, `core/utils.py`) rather than
  relying on the Session 1 summary, to validate Section 8 before locking
  it in. Confirmed URL dialect formats byte-for-byte against Sportarr's
  tests; simplified the fallback/retry classification to match Sportarr's
  proven binary approach (dropped an earlier, more granular
  DIALECT_FAILURE/NOT_READY split); confirmed the 1MB "not ready" threshold
  is a real shipped constant, not a guess. Resolved the User-Agent
  question: use `M3UAccount.get_user_agent()` (Dispatcharr's existing
  live-proxy UA resolution), explicitly not the DVR-internal
  `dispatcharr_dvr_user_agent()` string, which was traced to an
  internal-only hop that never reaches the actual provider. Section 8 is
  now `[~] DECIDED` and open question #4 is resolved. Surfaced a new,
  unresolved tension for Section 9 (open question #9): Sportarr's
  production implementation doesn't segment or concurrently download at
  all, which counters this project's segmented/turbo-mode design.
  **Next:** either build against Section 8 as locked, or resolve Section
  9's open tension first since it affects what Section 8's per-segment
  fallback loop is wrapping.

- **Session 3** (2026-07-05) — Resolved Session 2's open question #9 by
  design discussion rather than deferral. Position: sequential segmenting
  is a genuine, low-risk improvement over Sportarr's whole-file pull
  (better retry granularity, same provider connection load) and is kept;
  turbo mode (concurrent segment downloads) is dropped entirely for v1 —
  not deferred for lack of time, but rejected on the merits after arguing
  through it: the assumed speed gain is unproven (providers may be
  bandwidth-capped per account, not connection-slot-capped, so concurrency
  might not add real throughput), it competes with a connection resource
  shared with live viewing, and some panels reject concurrent connections
  to the same stream id outright. Also surfaced and logged a new risk
  while arguing through turbo mode that applies regardless of concurrency:
  self-imposed segment cut points aren't guaranteed to land on provider
  keyframe boundaries the way native HLS segments do, so stitching could
  glitch at the seam (Section 12, open question #10) — flagged, not
  solved. Section 9 rewritten to drop turbo mode and reflect this.
  **Next:** decide the per-segment retry cap value and segment-level
  orphan recovery (Section 12 #5/#6), or start scaffolding the plugin
  against Sections 3/4 as originally planned.

- **Session 4** (2026-07-05) — Added Section 14 (Logging & Observability
  Philosophy) as a cross-cutting design rule, prompted by wanting solid
  troubleshooting support given this plugin is headless/poll-driven with
  no request-response cycle a user can watch. Verified against real code
  rather than asserted: confirmed Dispatcharr's own `LOGGING` config
  (`dispatcharr/settings.py`) routes an unconfigured module logger through
  to console via the root logger, so the plugin needs no logging
  infrastructure of its own; confirmed `DISPATCHARR_LOG_LEVEL` is the
  existing operator-facing level knob to defer to; and — the one genuine
  gotcha found — traced `PluginManager._build_context()`
  (`apps/plugins/loader.py`) and confirmed the `context["logger"]` handed
  to a plugin's `run()`/`stop()` is the loader's own shared module logger,
  not a per-plugin one, meaning every plugin using it directly would be
  indistinguishable in a shared log stream unless each line self-tags.
  Also checked Sportarr's actual log calls and confirmed it never logs the
  constructed timeshift URL (which embeds credentials) — adopted as an
  explicit rule here rather than assumed safe by default. No open
  questions added; this section is fully decided. **Next:** unchanged from
  Session 3 — per-segment retry cap/orphan recovery (Section 12 #5/#6), or
  start scaffolding against Sections 3/4.

- **Session 5** (2026-07-05) — Added a per-channel "strip commercials"
  option, presented at the same flagging moment as catchup-eligibility
  (Section 4). Verified native comskip's actual trigger logic first
  (`apps/channels/tasks.py:2427-2431`) rather than assuming: it's a single
  global `CoreSettings.get_dvr_comskip_enabled()` check with no
  per-recording granularity today. This forced a decision on Section 4's
  previously-open "comma-separated list vs. marker" mechanism question —
  a flat ID list can't carry a second per-channel bit, so the structured
  `custom_properties` marker approach is now the resolved choice, holding
  both `catchup_enabled` and the new `comskip_enabled`. Section 7 now
  gates the `comskip_process_recording.delay()` call on both the existing
  global switch AND the per-channel flag (AND, not the plugin flag acting
  alone) — recommended and applied by default, on the reasoning that
  overriding an operator's system-wide comskip-off choice would be
  surprising; flagged as easy to flip to plugin-flag-alone if that turns
  out to be wrong. **Next:** unchanged — per-segment retry cap/orphan
  recovery (Section 12 #5/#6), or start scaffolding against Sections 3/4.

- **Session 6** (2026-07-05) — Major architecture change to Sections 4
  and 5, prompted by the user asking whether catchup capability could be
  detected without manual per-channel setup (comparing to TiViMate/
  IPTVEditor's automatic catchup detection). Investigated Sportarr's real
  model again with fresh eyes: confirmed it has *no* channel-flagging step
  at all — `Method=Catchup` vs. `Method=Live` is decided per-recording,
  automatically, off `channel.HasArchive`. Then found Dispatcharr already
  has its own native answer to "what to record" — a full Series Rules
  feature (`CoreSettings.get_dvr_series_rules()`, `evaluate_series_rules_impl`,
  `apps/channels/tasks.py:452-730`) plus `RecurringRecordingRule` — both
  already create native `Recording` rows without any plugin involvement.
  Dropped Section 4's manual flagging entirely in favor of: the plugin
  acts on whatever `Recording` rows already exist, gated only by Section
  3's automatic `tv_archive` check.
  Traced how native live capture actually gets triggered
  (`apps/channels/signals.py` — `schedule_task_on_save`, `revoke_task`,
  `schedule_recording_task`) to answer the resulting double-capture
  question, and found a real decision point: asked the user whether
  catchup should actively cancel native live capture for catchup-capable
  channels or only backstop it after a live failure. User chose **active
  replacement** — matches Sportarr's actual behavior and this project's
  original "instead of relying on live capture" goal. Designed the
  takeover around a plugin-registered `post_save` signal receiver on
  `Recording` (Django allows multiple receivers per signal, no core
  change) that revokes the native scheduled task via Dispatcharr's own
  `revoke_task()` helper, deliberately leaving `task_id` populated so
  native re-scheduling doesn't fire again on a later save. Section 7
  changed from "create a new Recording" to "update the existing
  taken-over row in place." `comskip_enabled` (added Session 5) moved
  from a per-channel marker to a plugin-wide `comskip_enabled_default`
  setting, since the flagging moment it depended on no longer exists.
  Added four new open questions (#11-14, Section 12) — signal receiver
  ordering, whether manual (non-rule) recordings populate
  `custom_properties["program"]`, what status value represents "pending
  catchup," and whether `RecurringRecordingRule` recordings behave
  identically — none blocking, all worth confirming once building starts.
  **Next:** the four new open questions above are the most valuable to
  resolve early since they underpin whether the takeover mechanism works
  at all; otherwise, per-segment retry cap/orphan recovery (Section 12
  #5/#6), or start scaffolding.

- **Session 7** (2026-07-05) — Reviewed Section 13 (deferred-to-core
  ideas) at the user's request; noted its item 1 now references the
  dropped manual-flagging design from Session 6 and is stale, but left it
  alone for now (user wants to come back to it later, not blocking).
  Resolved Section 3's last open sub-question: the plugin reads
  `Stream.custom_properties` directly at query time rather than caching a
  denormalized copy in Section 6's SQLite. Section 3 (catchup-channel
  identification) is now fully decided end to end, no open items
  remaining in that section. **Next:** revisit Section 13's stale
  cross-reference when convenient; otherwise the Session 6 open questions
  (#11-14) or per-segment retry cap/orphan recovery (#5/#6) are the
  highest-value remaining design gaps before scaffolding.

- **Session 8** (2026-07-05) — Worked through open questions #11-14 one
  at a time, verifying each against actual Dispatcharr backend and
  frontend source rather than reasoning abstractly. All four resolved,
  none needed a design change beyond small refinements:
  - #11: `INSTALLED_APPS` order confirms Dispatcharr's own
    `schedule_task_on_save` receiver always connects before the plugin's.
  - #12: whether `custom_properties["program"]` is present depends on
    "was this tied to an EPG entry," not "manual vs. Series Rule" —
    narrower edge case than assumed, existing fallback plan already
    covers it.
  - #13: use `"interrupted"` + a friendly `interrupted_reason`, but only
    from `start_time` onward, not at takeover — found by reading the
    actual frontend badge logic in `RecordingCard.jsx`, which is more
    time-driven than status-driven than expected.
  - #14: `RecurringRecordingRule` recordings go through the same
    `Recording.objects.create()` path, no special case needed; also
    surfaced that they carry a synthetic `program` dict too (correcting
    part of #12) and that `custom_properties` must be merged, not
    overwritten, on update to preserve the frontend's `rule` marker.
  Section 12 now has zero unresolved items from the Session 6 redesign.
  **Next:** per-segment retry cap/orphan recovery (Section 12 #5/#6) are
  the last open items overall, or start scaffolding the plugin.

- **Session 9** (2026-07-05) — Resolved Section 12's remaining
  Section-9-scoped open questions. #5 (per-segment retry cap): 5
  attempts, hardcoded, no separate backoff beyond the existing poll
  cadence — reasoned from scratch since Sportarr has no per-segment
  precedent (it doesn't segment). #6 (segment orphan recovery): no
  timeout/TTL mechanism — reuse Sportarr's own job-level "still
  in-progress when a new tick starts = orphaned" pattern one level
  deeper, since segments already process sequentially within one
  continuous tick per Section 9's no-concurrency decision. Both written
  into Section 9 and Section 12. **Next:** remaining open items are #2
  (fallback for no/stale EPG data), #3 (should catchup recordings be
  visually distinguishable in the UI), #7 (is fallback-channel rotation
  in scope for v1), #8 (validating a download actually captured the
  right content), and #10 (segment stitch/boundary risk, needs real-
  provider testing) — plus Section 13's stale cross-reference set aside
  in Session 7. Otherwise the design is ready to start scaffolding.

- **Session 10** (2026-07-05) — Resolved #2 (fallback for no/stale EPG
  data): only ever affects title/description cosmetics, never eligibility
  or timing, since the plugin doesn't decide what/when to record anymore
  (Session 6) — falls back to channel name + time window, mirroring
  `sync_recurring_rule_impl`'s own native convention. See Section 5.

- **Session 11** (2026-07-05) — Resolved #3 (visual distinction). User
  wants a small "Catchup" tag, plugin-only even as an exception, skip if
  costly. Checked `RecordingCard.jsx` and `RecordingDetailsModal.jsx` —
  neither has a generic property-rendering surface, and a real badge
  (matching "Recurring"/"Series") would require editing frontend source,
  ruled out. Settled on a `"[Catchup] "` title prefix instead — always
  renders (unlike `sub_title`, often empty), trivial cost, no frontend
  touch. **Next:** #7 (fallback-channel rotation in scope for v1?), #8
  (validating catchup content correctness), #10 (segment stitch risk,
  needs real-provider testing) — plus Section 13's stale cross-reference.

- **Session 12** (2026-07-05) — Resolved #7 (fallback-channel rotation):
  out of scope for v1, per the user's call — Sportarr can rotate a failed
  catchup onto a different channel because it has its own event database
  decoupled from channel; Dispatcharr has no equivalent, and building one
  would mean re-inventing plugin-owned channel curation, the exact thing
  Session 6 removed. Moved to Section 13 as a future consideration rather
  than discarded outright, per the user's request. Section 10 and Section
  12 updated to match. **Next:** #8 (validating catchup content
  correctness) and #10 (segment stitch risk, needs real-provider testing)
  are the last two open design questions; Section 13's stale
  cross-reference (item 1, from Session 7) is still parked whenever
  convenient.

- **Session 13** (2026-07-05) — Resolved #8 (validating catchup content
  correctness), the user's "dive deeper" request. Checked Sportarr's
  precedent first and found none — its `ffprobe` usage is quality/codec
  scoring only, and it parses `server_info.timestamp_now` from the Xtream
  auth response but never uses it; genuinely new design territory, not a
  port. Landed on three mechanical checks since real content
  understanding is out of scope: an account-level clock/timezone sanity
  check comparing `timestamp_now` against Dispatcharr's own clock (targets
  the actual timezone-bug risk directly, flagged as the most valuable of
  the three since it's the only one that doesn't just proxy for the real
  risk), plus per-download `ffprobe` duration and playability checks.
  Written into Section 10 with a cross-reference from Section 5's
  timezone-resolution point.

- **Session 14** (2026-07-05) — Parked #10 (segment boundary/stitch risk)
  in Section 13, at the user's request. Distinct from Section 13's other
  entries: not a core-change requirement or a rejected idea, just a
  question that can't be settled by more design discussion — it needs
  segment concatenation tested against a real provider once there's an
  actual plugin to test, so labeled clearly to avoid a future reader
  assuming everything in Section 13 needs a core change. Section 12's
  list now has zero open items. **Next:** only Section 13's stale
  cross-reference (item 1, from Session 7) remains parked; the design is
  otherwise complete and ready for scaffolding.

- **Session 15** (2026-07-05) — Full start-to-finish validation pass at
  the user's request, given how much shifted across 14 sessions
  (especially Session 6's architecture change). Read the entire document
  fresh rather than trusting incremental memory, and re-verified several
  facts against the actual cloned source rather than assuming prior
  citations still held (`_match_epg_program_by_timeslot`,
  `schedule_task_on_save`/`schedule_recording_task` line ranges, the
  comskip trigger, the `VLC/3.0.20 LibVLC/3.0.20` UA constant,
  `sync_recurring_rule_impl`'s line number) — all confirmed accurate, no
  factual drift found. Found and fixed three real internal
  inconsistencies that were previously missed: (1) the document's own
  opening "Build strategy" blurb still said the plugin "creates a real
  Recording row," predating Session 6's switch to updating an existing
  native row; (2) Section 9's stitch-boundary-risk paragraph still
  carried an open `[!]` tag and "not yet solved" phrasing after Session
  14 had already parked it to Section 13 — Section 9's own text was never
  updated to match; (3) Section 5 still described #12 (program metadata
  presence) as "needs checking," despite it having been resolved back in
  Session 8. All three fixed. Reconfirmed Section 13 item 1's stale
  cross-reference is the one *deliberately* left stale (user's own call,
  Session 7) — not a new finding. Also surfaced a framing-only
  observation, not fixed: the document's opening line still describes the
  goal as downloading "a finished sports program," while Section 5
  explicitly established the mechanism is no longer sports-specific —
  left for the user to decide whether the opening line should reflect
  that. **Next:** the design is validated end to end and ready for
  scaffolding. Only remaining loose threads are Section 13 item 1 (stale,
  deliberately parked) and the sports-specific-wording question above,
  both cosmetic, neither blocking.

- **Session 16** (2026-07-05) — Clarified the project's actual scope:
  Sportarr was studied purely as a reference implementation for
  catchup/timeshift *mechanics* — this project itself was never meant to
  be sports-specific; Sportarr just happens to be a sports app because
  that's what its own author built it for. Removed sports-specific
  framing throughout: the opening project blurb, Section 1's non-goals
  (was "no automatic 'is this a sports channel' detection," now "no
  automatic 'is this worth recording' detection" — generalized, not just
  reworded), Section 4's heuristics note, and Section 5's "no longer
  inherently sports-specific" line (reworded to state plainly that there
  was never a content-type restriction, rather than implying one was
  dropped). Also fixed Section 13 item 1's stale cross-reference while
  already editing that line for the same wording pass — it referenced
  "manual flagging, Section 4," a mechanism dropped entirely back in
  Session 6; left alone in Session 7 at the user's request, now fixed
  since it needed touching anyway. All other `Sportarr` references (the
  app name, its actual behavior, code citations) left untouched — only
  the framing of *this* project's scope changed. **Next:** design is
  complete, validated, and scoped correctly; ready for scaffolding.

- **Session 17** (2026-07-05) — Added Section 15 (Build Plan), a
  20-step sequenced implementation plan, at the user's request — they
  want to build iteratively with clean pause/resume points in case of
  running out of token budget mid-session. Ordered by dependency: plugin
  scaffolding, then read-only foundations (archive detection, state
  store schema) as the safest possible first real steps, then recording
  takeover, post-air detection, provider communication, segmented
  download, validation, recording integration, and polish, ending with
  real-provider testing. Flagged four natural milestone checkpoints
  (plugin loads / takeover+detection working / a real file gets produced
  / a finished catchup recording is playable and tagged) as good "stop
  here for a while" points, not just the fine-grained step boundaries.
  No code written yet — this session was planning only, per the user's
  explicit sequencing ("list this out, then we start the build").
  **Next:** begin Phase A, step 1 (plugin scaffolding).

- **Session 18** (2026-07-05) — Built and verified step 1 (Section 15
  Phase A): `catchup_recordarr/plugin.py` + `plugin.json`, a minimal
  `Plugin` class with one `ping` action, following the exact shape
  `apps/plugins/loader.py` reads (plain class attributes via `getattr`,
  no required `__init__` args). Also built out repo-based distribution,
  at the user's request, after verifying Dispatcharr's actual plugin-repo
  protocol (`apps/plugins/api_views.py`/`models.py`) rather than assuming
  "paste a GitHub URL" would work — it's a two-file manifest protocol
  (a registry manifest listing plugins, each pointing to a per-plugin
  detail manifest with version→download-URL mapping) plus a release zip;
  confirmed GitHub's automatic tag-archive zip works as-is since the
  installer walks the whole extracted tree for `plugin.py`/`__init__.py`
  rather than requiring it at the zip root. Built `manifest.json` and
  `catchup_recordarr-manifest.json`, pushed to `main`, tagged
  `v0.1.0`. Hit a real blocker mid-way: the repo was private, so
  Dispatcharr's unauthenticated fetch would 404 — flagged rather than
  worked around, user made the repo public, then every URL was
  re-verified (HTTP 200, correct content, zip contents confirmed to
  contain `plugin.py` at the expected nested depth) before declaring it
  done. User then installed the plugin via Settings → Plugins → Add
  Repository using the manifest URL, enabled it, and successfully ran
  the `ping` action end-to-end. Step 1 fully verified on a real instance,
  not just structurally reviewed. **Next:** step 2 (Section 3, archive
  detection — daily read of `Stream.custom_properties["tv_archive"]`).

- **Session 19** (2026-07-05) — Built step 2 (Section 15 Phase B, Section
  3): `archive.py` (the actual refresh logic), `tasks.py` (Celery task
  wrapper), and `plugin.py` now registers a daily periodic task via
  `core.scheduling.create_or_update_periodic_task` at module-load time,
  plus a manual "Refresh Now" action for testing. Verified several
  implementation details against real code before writing anything,
  rather than assuming: `apps.channels.models.Stream` already has a
  first-class indexed `stream_id` field (simpler than the URL-parsing
  approach Sportarr needed), `core.xtream_codes.Client` is an existing,
  reusable Xtream API client (reused rather than writing raw HTTP calls),
  and — a real gotcha caught before it became a bug — the normal M3U sync
  stores `tv_archive`/`tv_archive_duration` as **strings** in
  `custom_properties` (confirmed via `apps/m3u/tasks.py`'s stream-parsing
  code), so a bare truthy check would treat `"0"` as True; added
  `_parse_bool_ish()` to guard against it. Also confirmed `Client`
  implements the context-manager protocol and fixed the code to use `with`
  for proper session cleanup, matching Dispatcharr's own call sites.
  Discussed direct deployment access (user's Dispatcharr runs as an Unraid
  Docker container) — recommended against giving direct push/SSH access
  to the live container this early in the build, since the more invasive
  code (takeover signal, segment downloads) is still ahead; user agreed to
  keep using the repo-install method. Bumped to v0.2.0 (both manifest
  files + plugin.json + Plugin.version) since Dispatcharr's UI diffs
  `latest_version` to show "update available." Pushed and tagged; GitHub
  reported the repo had moved to `jamesgallagher/CatchupRecordarr` -
  verified the redirect actually worked for both `raw.githubusercontent.com`
  and the archive-zip download before concluding nothing was broken.

- **Session 20** (2026-07-05) — Full project rename at the user's
  request: "DispatcharrRecordarr" → **"Catchup Recordarr"** (display
  name), plugin folder/slug `dispatcharr_recordarr` →
  `catchup_recordarr`, GitHub repo → `jamesgallagher/CatchupRecordarr`
  (the user had already renamed it). Updated everywhere rather than
  partially: design.md's title and its `custom_properties["dispatcharr_recordarr"]`
  key references (Sections 4/7 - the future comskip-override marker key,
  not yet built, so no code needed updating, just the design language),
  the plugin folder + `plugin.py`/`plugin.json`/`tasks.py` internals
  (task name/path strings, `help_url`), both distribution manifest files
  (`manifest.json`'s `slug`/`name`/`registry_url`/`registry_name`,
  `catchup_recordarr-manifest.json`'s version URLs), and the git remote
  itself (`git remote set-url`, rather than relying on GitHub's rename
  redirect indefinitely). Deliberately left every actual `Dispatcharr`
  (the target app) reference untouched - only this project's own name
  changed, not the app it targets. Bumped to v0.3.0 since the slug change
  means existing installs can't smoothly "update" (the plugin key
  changes) - will need a fresh install under the new key. Historical
  Session Log entries were also updated to the new name/paths for
  document readability, rather than left stale under the old name.
  **Next:** push + tag v0.3.0, verify every manifest URL resolves under
  the new repo name, user does a fresh install (not an update, given the
  slug change) and re-verifies archive detection, then step 3.

- **Session 21** (2026-07-05) — Extended, real debugging session on the
  user's actual Unraid deployment, root-causing a "Refresh Now" action
  that kept failing with `celery.worker.consumer.consumer: Received
  unregistered task ... KeyError` even across multiple full container
  restarts. Learned real infrastructure detail not previously known: the
  user's Dispatcharr runs as a single `DISPATCHARR_ENV=aio` container
  (web + Celery + Beat all bundled via `docker/entrypoint.aio.sh`) with
  Redis on a separate host, and - discovered from the actual boot log -
  **two** Celery workers exist (`default@...` on the `celery` queue,
  `dvr@...` on a dedicated `dvr` queue), both running Celery Beat. Ruled
  out several wrong theories in sequence by checking real logs rather
  than assuming: not a stale/non-restarted worker (confirmed via a
  genuinely fresh boot sequence, GPU checks and all); not a missing
  `worker_ready` discovery hook (confirmed `dispatcharr/celery.py:45-51`
  exists and both workers logged `Discovered 1 plugin(s)` cleanly before
  going `ready.`, in the correct order, no exceptions); not a queue
  routing mismatch (task correctly targets the `celery` queue, matching
  the `default` worker). Landed on the most likely remaining explanation:
  our task used `@shared_task`, which lazily binds to "whichever Celery
  app is current" - designed for library code reused across multiple
  app instances - while our plugin is loaded through the dynamic
  `importlib.util.spec_from_file_location` mechanism rather than a normal
  package import, a combination not exercised by core Dispatcharr's own
  (working) `@shared_task`-based periodic task. Changed `tasks.py` to
  bind directly to Dispatcharr's own concrete `Celery` app object
  (`from dispatcharr.celery import app as celery_app; @celery_app.task(...)`)
  instead, removing the indirection. Bumped to v0.4.0. **Not yet
  confirmed working** - this is a plausible, testable fix, not a verified
  one; next step is the user retesting "Refresh Now" against v0.4.0 and
  reporting back before step 2 can be marked complete.

- **Session 22** (2026-07-05) — v0.4.0 retested properly this time (a
  genuine full container restart confirmed first, after almost drawing
  the wrong conclusion from a test that turned out to not have restarted
  at all) - identical failure. This ruled out the "which app does the
  task bind to" theory definitively, confirming the real cause is a
  timing gap: Dispatcharr's `worker_ready`-triggered plugin discovery
  (`dispatcharr/celery.py:45-51`) runs *after* the Celery Consumer has
  already snapshotted its dispatch table from `app.tasks`, so a task
  registered that late is structurally invisible to the running worker
  for its entire lifetime - not fixable from plugin code, and would have
  affected every future Celery task this plugin tried to register (the
  post-air poll in step 6, not just archive detection). Rebuilt around a
  self-contained mechanism instead: removed `tasks.py`/the Celery
  `PeriodicTask` entirely; added `state.py` (a minimal SQLite key-value
  store, pulling a small piece of Section 6 forward ahead of schedule)
  and a daemon background thread in `plugin.py` that checks a persisted
  "last completed" timestamp every 30 minutes and runs the refresh when
  24+ hours have passed, with a best-effort claim check (not a strict
  lock) to reduce redundant runs across the multiple processes that each
  load this plugin independently. Added a 30s startup delay before the
  thread's first check, mirroring Sportarr's own `CatchupDownloadService`
  precedent, since Django's app registry isn't guaranteed fully loaded
  the instant our thread starts. Also added `_version.py` as a single
  source of truth for the plugin version, now included in every log tag
  (`[Catchup v0.5.0]`) - directly motivated by this debugging session,
  where it was genuinely ambiguous whether a test was exercising new code
  or a stale in-memory copy from before a restart. Updated Section 2's
  feasibility claim about plugin-registered Celery tasks with this
  empirical finding, and flagged Section 5 Part B (not yet built) to use
  the same background-thread approach rather than hitting the same wall
  later. Bumped to v0.5.0. **Still not confirmed working** - genuinely
  new territory (self-contained thread instead of Celery), not just a
  binding tweak this time; next step is the user testing this against
  their real deployment.

- **Session 23** (2026-07-05) — Full design + code review at the user's
  request (model switched to Fable mid-project), before testing the
  still-unverified v0.5.0. Three real defects found and fixed, folded
  into v0.6.0 so the user tests once rather than twice:
  1. **state.db would be destroyed on every plugin update** — verified in
     `_install_plugin_from_zip` (`apps/plugins/api_views.py:249-292`)
     that repo-based updates atomically swap the plugin directory and
     delete the old one, wiping any file not in the release zip. Moved
     the store to a sibling `catchup_recordarr_data/` directory that
     survives updates; corrected Section 6's original (wrong) path
     decision with the evidence.
  2. **Thundering herd at first boot** — read the real production
     `docker/uwsgi.ini`: 4 uWSGI workers under `lazy-apps=true` plus two
     Celery workers, beat, and daphne as attach-daemons, so ~8 processes
     each start our scheduler thread at boot and wake on the same
     interval. v0.5.0's claim was a non-atomic get-then-set, making
     simultaneous full provider fetches the *expected* first-boot
     behavior (violating our own Section 9 one-connection principle),
     not a rare race. Replaced with an atomic `state.claim()` using
     SQLite `BEGIN IMMEDIATE` (check-then-set under a write lock), plus
     random jitter (30-120s) on each thread's initial sleep.
  3. **Stale Django DB connections in the long-lived thread** — the
     scheduler's ORM connection idles ~24h between runs and PostgreSQL
     will close it server-side; added `close_old_connections()` around
     the refresh, matching Dispatcharr's own pattern in the plugin
     loader.
  Also verified as fine, no action: uWSGI thread support (the config's
  `gevent-early-monkey-patch=true` makes our threads greenlets in web
  workers — no `enable-threads` needed); `XCClient` accepting the
  `UserAgent` model object; design doc consistency post-Celery-pivot.
  Noted, deliberately not acted on: version string duplicated between
  `plugin.json` and `_version.py` (manual sync); per-row `stream.save()`
  vs `bulk_update` (fine at current scale). Bumped to v0.6.0.
  **Next:** user updates to v0.6.0, restarts the AIO container, and
  tests — expect `[Catchup v0.6.0] background scheduler thread started`
  in logs, then "Refresh Now" runs synchronously (no Celery involved)
  and logs per-account results. That verifies step 2 end-to-end.

- **Session 24** (2026-07-05) — v0.6.0 verified end-to-end on the real
  instance, in two rounds. First test: mechanics worked (scheduler
  thread started, no Celery error, synchronous action ran) but completed
  in 39ms with no per-account line — diagnosed from the timestamps as
  "the XC+active account query matched nothing," not a code failure.
  After the user's account situation changed, second test succeeded
  fully: XC auth OK, 57 streams retrieved, **32 catchup-capable**, 0 DB
  rows updated (correct — the normal M3U sync had already stored
  matching flag values, exactly as Section 2 predicted). **Step 2
  complete and verified.** Built step 3 (Section 6 schema): `jobs`,
  `segments`, `account_dialects` tables added to `state.py` alongside
  the existing `kv` table — idempotent DDL, `PRAGMA foreign_keys=ON`,
  stored `schema_version` for future migrations; schema only, no logic
  wired yet per the build plan. Also fixed the logging gap the first
  test round exposed (an explicit "no active XC accounts found" INFO
  line instead of a silent instant no-op — Section 14 philosophy applied
  retroactively to a case that actually cost a debugging round), and
  `ping` now reports state-store health + schema version so step 3 has
  a user-verifiable output. Bumped to v0.7.0. **Next:** user verifies
  v0.7.0 (ping should report "state store OK (schema v1)"), then step 4
  — the Recording takeover signal receiver (Section 5 Part A), the
  first step that actively modifies native scheduling behavior.

- **Session 25** (2026-07-05) — v0.7.0 verified (ping reported "state
  store OK (schema v1)"); step 3 complete. Built step 4 (Section 5 Part
  A): new `takeover.py` with a `post_save` receiver on `Recording` that
  revokes native live capture and records a pending job, exactly per the
  Session 6/8 design (leave `task_id` populated, defensive guards,
  `revoke_task` reused). Verified `Channel.streams` M2M and `Recording`
  fields against the model source before writing. Three implementation
  findings written into Section 5: disabled plugins keep their signals
  connected (receiver checks `PluginConfig.enabled` itself); the
  receiver fires on every Recording save including ~2s live-capture
  progress writes (guards ordered cheapest-first) and at least twice per
  new Recording (idempotent via jobs-table INSERT OR IGNORE +
  `dispatch_uid`); and a known gap deferred to step 6 — recordings
  scheduled while the plugin was disabled keep native capture until
  Part B's tick learns to sweep them. Bumped to v0.8.0. **Next:** user
  tests takeover with a THROWAWAY recording (nothing they want kept):
  schedule a few minutes ahead on a catchup-capable channel, expect the
  "took over recording" log line and no FFmpeg start at air time; also
  schedule one on a non-capable channel and confirm it records normally.
  Note: a taken-over recording will just sit there unfetched — the
  pipeline that fulfills it is steps 5-16, not built yet.

- **Session 26** (2026-07-05) — User asked whether the TV guide can show
  which channels are catchup-capable. Answer: no — the guide UI doesn't
  read `tv_archive`, and a real guide badge would need frontend edits
  (same plugin-only line as Session 11's recording-badge decision).
  Added a **"List Catchup Channels"** plugin action instead (v0.9.0):
  one click returns every channel with a catchup-capable stream on an
  active XC account, with per-channel archive retention days (max across
  the channel's streams), first dozen in the toast + full list at INFO
  in the logs. Doubles as the picker for step 4's takeover test —
  the user needs a known-capable channel to schedule the throwaway
  recording on. Step 4 (v0.8.0's takeover receiver) still awaiting its
  real-instance test; the v0.9.0 update includes it unchanged. **Next:**
  user updates to v0.9.0, restarts, uses "List" to pick a capable
  channel, then runs the step 4 takeover test from Session 25's plan.
