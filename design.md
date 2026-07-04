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
(Xtream `tv_archive=1`), automatically download a finished sports program
from the provider's archive instead of relying on live capture, and have
the result appear in Dispatcharr exactly like a normal recording — watchable
from the app, eligible for comskip commercial removal.

**Non-goals for v1:**
- No core Dispatcharr code changes (plugin-only).
- No automatic "is this a sports channel" detection — channels are
  manually flagged (Section 4).
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
- No per-channel toggle in the native Channels UI — flagging is done via
  the plugin's own settings panel (Section 4).
- No reaction to Dispatcharr's native "EPG just refreshed" trigger — the
  plugin polls on its own timer instead of being pushed (Section 5).
- Recordings created this way are indistinguishable in the UI from a live
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

**Open sub-question `[!]`:** does the plugin read `custom_properties`
directly at query time (always fresh, more DB hits) or maintain its own
denormalized cache in its SQLite state store (Section 6)? Leaning toward
direct read since it's cheap and avoids a second source of truth, but not
finalized.

---

## Section 4 — Channel Flagging (Catchup-Eligible Channels) `[~] DECIDED`

- **Decision: manual flagging**, not auto-detection by sports
  category/name (rejected — adds heuristic complexity and false
  positives/negatives for v1).
- Mechanism: plugin settings field(s) where the user identifies which
  channels are catchup-eligible. Concretely, one of:
  - A text field taking a comma-separated list of channel IDs/names, or
  - A plugin action button that toggles a marker
    (`custom_properties["catchup_enabled"] = true`) on a given `Stream`,
    keyed by channel identifier supplied by the user.
- **Accepted limitation:** no checkbox in the native Channels list. This
  is a plugin architectural boundary, not a bug to fix in v1.

---

## Section 5 — EPG-Driven "Program Finished" Detection `[~] DECIDED`

**Data chain:** `Channel.epg_data (EPGData) → epg_data.programs (ProgramData)`,
matched via `tvg_id`. Each `ProgramData` row has `start_time`, `end_time`,
`title`, `sub_title`, `description`.

**Query pattern** (per catchup-flagged channel):

```
candidates = channel.epg_data.programs.filter(
    end_time__lte = now - grace_period,
    end_time__gt  = now - lookback_window,
)
```

- `grace_period`: buffer after EPG-stated end time before attempting
  download, since the provider's archive needs time to finalize the tail
  of the broadcast (Sportarr default: ~10–15 min, configurable).
- `lookback_window`: how far back to still consider "recently finished and
  not yet queued" — bounded by the channel's archive retention
  (`tv_archive_duration`) at the outer edge; no point considering a program
  whose window has already aged out of the provider's archive.
- Program metadata (`title`, `sub_title`, `description`) feeds directly
  into the `Recording.custom_properties["program"]` dict on success
  (Section 7), so the finished recording shows a real title/episode info,
  not just the channel name.

**Polling, not push:** natively, `evaluate_series_rules.delay()` fires
immediately after each EPG refresh completes (`apps/epg/tasks.py`). A
plugin can't hook that exact trigger point without a core change, so this
runs on the plugin's own periodic task instead (every 5–15 min, matching
Sportarr's tick interval). Accepted as "not instant, but fine" since a
finished sports broadcast isn't time-critical the way live capture timing
is.

**Known edge cases (design-level acknowledgment, not yet solved in detail):**
- No `epg_data` link or stale EPG source for a flagged channel → no program
  end times to key off. Fallback behavior undecided `[!]`.
- Broadcast overruns (real end time later than EPG's stated `end_time`) →
  absorbed by post-padding on the request window, not by trying to detect
  overrun precisely.
- Timezone: `ProgramData` times are in Dispatcharr's normal (UTC-aware)
  datetimes; must be converted to the **provider's** local time (resolved
  from the Xtream auth response's `server_info.timezone`, same as
  Sportarr) when building the timeshift URL — not Dispatcharr's or the
  user's local time.

---

## Section 6 — State Tracking `[~] DECIDED`

- **Decision: plugin-owned SQLite file** (not a core Django model
  migration), e.g. `/data/plugins/<plugin>/state.db`.
- Needs to track, at minimum:
  - Per program: identity (channel + EPG program id or start/end time),
    overall status (`pending` / `in_progress` / `completed` / `failed`),
    retry count, last error.
  - Per segment (see Section 8): index, byte range or time window,
    status (`pending` / `in_progress` / `completed` / `failed`), retry
    count.
- **Idempotency requirement:** the periodic poller (Section 5) must treat
  "in_progress" as claimed — not just "done vs not done" — to avoid
  double-queuing the same program if a poll fires before the previous pass
  finishes.

---

## Section 7 — Recording Integration (Native Playback + Comskip) `[~] DECIDED`

On successful download + stitch + verification, the plugin creates a real
`Recording` row (same model native recordings use) with `custom_properties`
populated to match the shape the native pipeline produces:

```
status: "completed"
file_path: <path to final stitched .mkv>
file_name: <filename>
file_url / output_file_url: "/api/channels/recordings/{id}/file/"
ended_at, bytes_written, remux_success
program: { title, sub_title, description, ... }   # from EPG match
```

Then, optionally, call `comskip_process_recording.delay(recording.id)` —
the same Celery task the native pipeline calls — to get commercial removal
through the unmodified core code path.

**Critical ordering rule:** do NOT create/link the `Recording` row (or set
status to `completed`) until the file is fully downloaded, stitched, AND
verified (Section 9/10). The playback endpoint only checks
`os.path.exists` + non-zero size, not validity — exposing a partial or
corrupt file early would serve a broken recording to the user.

**Open question `[!]`:** should catchup-sourced recordings be visually
distinguishable from live-captured ones anywhere in the UI? Currently
undecided — "indistinguishable from a live recording" was floated as
possibly the goal, but no final call made. Revisit before wiring this up,
since it affects whether we need an extra `custom_properties` marker (the
existing UI won't render an unknown key, so this would need at minimum a
naming convention, e.g. prefixing the title).

---

## Section 8 — Timeshift URL Construction `[ ] NOT STARTED (design sketched, not finalized)`

Two known Xtream timeshift URL dialects exist in the wild (per Sportarr's
findings, itself ported from `timeshifter` by scottrobertson):

- **Path style:** `{server}/timeshift/{user}/{pass}/{duration}/{start}/{streamId}.ts`
- **PHP style:** `{server}/streaming/timeshift.php?username=...&password=...&stream=...&start=...&duration=...`

`start` format: `yyyy-MM-dd:HH-mm`, in the **provider's** local time.

Plan (mirroring Sportarr): try the previously-detected working dialect
first per source, fall back to the other on failure, and remember which
one worked for next time (avoids two failed attempts becoming the steady
state for a given provider).

**Not yet designed in detail:** exact retry/fallback ordering logic,
persisting "detected dialect" per `M3UAccount`, and the small-file/empty
response heuristic for "archive doesn't have this window yet" (Sportarr
uses "<1MB response = not ready," worth adopting but not finalized here).

---

## Section 9 — Segmented Download + Turbo Mode `[~] DECIDED (design only)`

**Segmenting:** a program is split into fixed-size chunks (e.g. 15 or 30
min) rather than pulled as one continuous request. Direct precedent:
native live DVR recording already works this way — HLS `.ts` segments
during capture, concatenated into the final MKV afterward
(`_dvr_build_hls_concat_cmd`). Reasoning ported to catchup: a single chunk
failing only needs that chunk retried, not the whole multi-hour archive
pull; natural resume points if a worker restarts mid-download.

**Segment state machine** (stored in Section 6's SQLite):
`pending → in_progress → completed`, or on failure, **`in_progress → pending`**
(not a dead-end `failed` state) — so the same "claim next unclaimed
segment" logic used for normal sequential processing and for turbo mode
naturally retries it on the next pass. A per-segment retry cap is needed
(mirrors the whole-program retry cap / archive-retention-expiry cutoff) so
one permanently-bad segment doesn't loop forever — exact cap value not
decided `[!]`.

**Turbo mode:** an optional setting — if a second provider connection slot
is available, download 2 segments concurrently (never more than 2), each
thread claiming the next `pending` segment from the shared queue. This is
a worker-count change on top of the same segment queue, not a new
architecture. Stitching at the end sorts by segment index before
concatenating (order of completion doesn't matter, order in the final file
does).

**Two risks flagged, not yet mitigated in code:**
1. Turbo must check the **provider's actual free connection slot** (via
   Dispatcharr's existing `reserve_profile_slot`/`release_profile_slot`
   mechanism on `M3UAccountProfile`), not just "a local thread is free" —
   otherwise it could consume a connection slot needed for live viewing
   elsewhere in the household.
2. Some Xtream panels reject a second simultaneous connection to the same
   `stream_id`/archive URL even if the account's overall connection limit
   allows it. Turbo mode needs a clean fallback to single-threaded on
   rejection rather than assuming it always works — this is provider
   behavior we can't determine from Dispatcharr's code and will need to be
   observed empirically.

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
  with a reason string on the created `Recording` row — renders correctly
  through the existing native UI with zero UI changes needed, since it's
  the same field the native pipeline uses.

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
2. Fallback behavior for a flagged channel with no/stale EPG data?
   (Section 5)
3. Should catchup recordings be visually distinguishable from live
   recordings anywhere in the UI? (Section 7)
4. Exact retry/fallback ordering + "not ready yet" detection heuristic for
   the two timeshift URL dialects? (Section 8)
5. Per-segment retry cap value? (Section 9)
6. Segment-level orphan/stuck-in-progress recovery: timeout-based
   auto-requeue, or wait for next poll? (Section 9)
7. Is fallback-channel rotation (switching to a different archive-capable
   channel after repeated failures, as Sportarr does) in scope for v1, or
   deferred? (Section 10)
8. How to validate a "successful" download actually captured the right
   window/content, not just that ffmpeg exited cleanly? (Section 10)

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
