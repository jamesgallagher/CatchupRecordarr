# DispatcharrRecordarr — Design Document

**Project:** A Dispatcharr plugin that detects IPTV channels supporting
catchup/timeshift, waits for a scheduled sports program to finish, and
downloads it from the provider's archive — surfacing the result as a normal,
fully-functional Dispatcharr recording (watchable, comskip-eligible) rather
than a bolted-on side feature.

**Reference implementation studied:** Sportarr
(github.com/Sportarr/Sportarr) — a .NET app with an equivalent
`CatchupDownloadService`. Concepts are ported; no code is shared (different
stack entirely — Dispatcharr is Django/Celery/Python).

**Target app:** Dispatcharr (github.com/Dispatcharr/Dispatcharr)

**Build strategy:** Plugin-first. No core Dispatcharr changes in v1. This
was a deliberate choice after establishing that Dispatcharr's existing
`Recording` model, playback endpoint, and comskip task are all decoupled
from *how* a recording's file was produced — a plugin can create a real
`Recording` row and get native playback/comskip for free. See Section 7.

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
- No automatic "is this a sports channel" detection, and no plugin-owned
  channel curation UI either (Section 4, revised) — the plugin never
  independently decides what's worth recording; it only acts on channels
  and programs the user has already told Dispatcharr's native scheduling
  about.
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
- **Plugins have full Django ORM access** and can register their own
  periodic Celery task via `core.scheduling.create_or_update_periodic_task`
  (the same mechanism core itself uses for the plugin-repo-refresh
  feature) — so a plugin can poll on its own schedule without any core
  hook.

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
- Empty/failed fetch → do not clear existing flags (provider hiccup should
  not silently downgrade catchup capability — same reasoning Sportarr
  uses).

**This is now the *only* gate for catchup eligibility** (Section 4,
revised, Session 6) — there is no separate manual per-channel opt-in
layered on top anymore. If a channel's `Stream` has `tv_archive > 0`, any
`Recording` scheduled against it is catchup-eligible.

**Open sub-question `[!]`:** does the plugin read `custom_properties`
directly at query time (always fresh, more DB hits) or maintain its own
denormalized cache in its SQLite state store (Section 6)? Leaning toward
direct read since it's cheap and avoids a second source of truth, but not
finalized.

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
- Any risk of the plugin inventing "is this a sports channel" heuristics
  — moot, since the plugin never decides what's worth recording at all.

**`comskip_enabled` needs a new home.** The per-channel marker from the
now-dropped flagging mechanism (`custom_properties["dispatcharr_recordarr"]`)
no longer has a natural "flagging moment" to live in. Simplest fix,
applied for v1: a single plugin-wide setting, `comskip_enabled_default`
(bool, default **off**, same opt-in-by-default philosophy as before).
A per-channel override marker is still *possible* later via the same
`custom_properties["dispatcharr_recordarr"]["comskip_enabled"]` shape
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

**Part B — post-air poll (periodic tick, unchanged cadence, 5–15 min).**
Once a `Recording` has been taken over (Part A), the plugin still needs to
know when to actually pull it from the archive:

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
  whichever native path created the row (confirmed for Series Rules;
  needs checking for plain manual scheduling, Section 12).
- This also means the feature is no longer inherently sports-specific —
  it now fires for *whatever the user has scheduled* on a catchup-capable
  channel, sports or not. Scope is entirely a function of what Series
  Rules / recurring rules / manual recordings the user actually sets up,
  never enforced by the plugin.

**Known edge cases (carried over, still not solved in detail):**
- A `Recording` on a catchup-capable channel whose channel has no
  `epg_data` link at all → no `custom_properties["program"]` to fall back
  on either. Fallback behavior still undecided `[!]`.
- Broadcast overruns (real end time later than the scheduled `end_time`)
  → still absorbed by post-padding on the request window, not precise
  overrun detection.
- Timezone: still must convert to the **provider's** local time (Xtream
  `server_info.timezone`) when building the timeshift URL, never
  Dispatcharr's or the user's local time.

---

## Section 6 — State Tracking `[~] DECIDED`

- **Decision: plugin-owned SQLite file** (not a core Django model
  migration), e.g. `/data/plugins/<plugin>/state.db`.
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
   — or the channel's optional `custom_properties["dispatcharr_recordarr"]["comskip_enabled"]`
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

**Open question `[!]`:** should catchup-sourced recordings be visually
distinguishable from live-captured ones anywhere in the UI? Currently
undecided — "indistinguishable from a live recording" was floated as
possibly the goal, but no final call made. Revisit before wiring this up,
since it affects whether we need an extra `custom_properties` marker (the
existing UI won't render an unknown key, so this would need at minimum a
naming convention, e.g. prefixing the title).

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
segment" logic naturally retries it on the next pass. A per-segment retry
cap is needed (mirrors the whole-program retry cap / archive-retention-
expiry cutoff) so one permanently-bad segment doesn't loop forever — exact
cap value not decided `[!]`.

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

**Segment boundary / stitch risk `[!]` (open, newly surfaced — not just a
turbo-mode concern, applies to sequential segmenting too):** because each
segment is its own independently-requested archive window rather than a
provider-delivered keyframe-aligned chunk, consecutive segments aren't
guaranteed to concatenate cleanly — a dropped frame or brief AV desync at
the seam is possible where our chosen cut point doesn't land on a
provider-side keyframe boundary. Candidate mitigations not yet decided:
small deliberate overlap between segments with trim-at-stitch, or
accepting the same `-fflags +genpts -avoid_negative_ts make_zero` treatment
Sportarr applies at remux time (Section 8/10) — note that fixes monotonic
PTS/discontinuity but does **not** by itself guarantee no dropped frames at
a non-keyframe cut. Needs real-world testing against an actual provider
before considering this solved.

**Segment-level orphan recovery `[!]` (open, same shape as job-level
recovery in Section 10):** a segment stuck `in_progress` past some timeout
(worker died mid-chunk) — auto-requeue after a timeout, or wait for the
next poll cycle to notice? Not decided; same pattern as the whole-job lock
TTL below, just applied per-segment.

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
  forever — mirrors Sportarr's fallback-channel-rotation cutoff logic
  (note: fallback-channel rotation itself is not yet decided as in/out of
  scope for our v1 — `[!]` open question).
- Failure surfaced by setting `custom_properties["status"] = "failed"`
  with a reason string on the taken-over `Recording` row (Section 4/5/7,
  revised Session 6 — no longer a row the plugin created itself) —
  renders correctly through the existing native UI with zero UI changes
  needed, since it's the same field the native pipeline uses.

**Validation against silently-wrong content `[!]` (open, not yet designed):**
a timezone or scheduling error could result in a "successful" download of
the *wrong* window (different program) with no hard error. A basic
sanity check (e.g. comparing final duration to the expected
`end_time - start_time`) was suggested but not designed in detail.

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
2. Fallback behavior for a catchup-eligible channel with no/stale EPG
   data? (Section 5)
3. Should catchup recordings be visually distinguishable from live
   recordings anywhere in the UI? (Section 7)
4. ~~Exact retry/fallback ordering + "not ready yet" detection heuristic for
   the two timeshift URL dialects?~~ **RESOLVED** — see Section 8 (verified
   against Sportarr's shipped source: binary fail/retry-other-dialect,
   1MB threshold, per-`M3UAccount` dialect state).
5. Per-segment retry cap value? (Section 9)
6. Segment-level orphan/stuck-in-progress recovery: timeout-based
   auto-requeue, or wait for next poll? (Section 9)
7. Is fallback-channel rotation (switching to a different archive-capable
   channel after repeated failures, as Sportarr does) in scope for v1, or
   deferred? (Section 10)
8. How to validate a "successful" download actually captured the right
   window/content, not just that ffmpeg exited cleanly? (Section 10)
9. ~~Sportarr's shipped implementation doesn't segment or run concurrent
   catchup downloads at all — how does this reconcile with Section 9's
   segmented + turbo-mode design?~~ **RESOLVED** — deliberate divergence,
   not an oversight: keep sequential segmenting (real improvement over
   Sportarr, same provider load), drop turbo mode/concurrency entirely for
   v1 (unproven speed gain, competes with a scarce shared connection
   resource, some panels reject concurrent same-stream connections). See
   Section 9.
10. **New, from the turbo-mode discussion:** segment boundary/stitch risk
    — each catchup segment is an independently-requested archive window,
    not a provider-delivered keyframe-aligned chunk like native HLS
    segments are, so consecutive segments aren't guaranteed to
    concatenate cleanly. Mitigation (overlap-and-trim vs. accepting
    Sportarr's genpts/avoid_negative_ts treatment) not yet decided; needs
    real-provider testing. (Section 9)
11. **New, from Session 6's native-scheduling takeover redesign:** must
    confirm in practice that Dispatcharr's own `schedule_task_on_save`
    signal receiver (`apps/channels/signals.py`) runs *before* the
    plugin's own `post_save` receiver on `Recording`, so `task_id` exists
    yet to revoke at takeover time. Believed true (plugins load after
    core apps are ready) but not yet verified against a running instance.
    (Section 5)
12. **New, Session 6:** does a plain manually-scheduled `Recording` (user
    clicks "record" on an EPG program directly, not via a Series Rule)
    also populate `custom_properties["program"]` the same way
    `evaluate_series_rules_impl` does? If not, Section 5's Part B needs a
    fallback to a direct EPG lookup for that case. (Section 5)
13. **New, Session 6:** what `custom_properties["status"]` value should a
    taken-over `Recording` show between revoke and a successful catchup
    fetch? Needs a value the native UI renders sensibly (not `"recording"`,
    since no live capture is actually happening) — not yet decided.
    (Section 5/7)
14. **New, Session 6:** confirm `RecurringRecordingRule`-driven recordings
    end up as plain `Recording` rows the same way Series Rules and manual
    scheduling do, so the takeover logic in Section 5 applies uniformly
    without a special case. Likely true, not yet confirmed. (Section 4/5)

---

## Section 13 — Explicitly Deferred to Future / Core Integration `[ ] NOT STARTED`

Ideas raised and consciously not pursued in v1, kept here so they aren't
re-litigated from scratch later:

- Auto-detection of "sports" channels by category/name (rejected in favor
  of manual flagging, Section 4).
- Native per-channel UI toggle, `capture_method` field on `Recording`,
  scheduler-level integration — all would require core Dispatcharr
  changes; explicitly deferred until the plugin is proven (Section 2).
- Any core migration for archive flags (currently relying entirely on the
  already-populated `Stream.custom_properties`).

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
Celery-task module (registered per Section 2/5) gets a naturally unique
`__name__`-based logger already, so this mainly matters for action-handler
code paths — but tag every line either way for consistency and
grep-ability, rather than depending on which code path happened to produce
it.

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
