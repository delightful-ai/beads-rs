# message

**Pin:** v1.0.2, c446a2ef

**Parity status:** `deferred`

**See also:** `primitives/mail.md` (TBD), `event.md`,
`primitives/metadata-remapping.md` (TBD).

## Purpose

A mail-style inter-agent communication bead. Messages have a sender, a
recipient, a subject line, a body, a threading link, and read/archive
state. In gascity, `message` is the built-in mail backend: the Provider
interface at `internal/mail/mail.go` is implemented by
`internal/mail/beadmail/beadmail.go::Provider`, which creates
`Type="message"` beads via the bead store (`beadmail.go:45-56` for send;
`:146-174` for reply).

Distinguishing feature vs adjacent:

- `event` — factual record of what happened. `message` — addressed
  communication.
- `gate { kind: Await, await_type: Mail }` — a gate waiting on message
  arrival. `message` — the message being waited on.
- `task` — work to do. `message` — something said; the recipient may then
  act on it.

## Typed field set

```rust
pub struct MessageFields {
    /// Recipient address. Maps to gascity's Assignee on the message bead
    /// (`beadmail.go:49, 167`). A message without `to` never appears in
    /// any inbox; gascity rejects empty `to` at send
    /// (`beadmail.go:32-34`).
    pub msg_to: Lww<MailAddress>,

    /// Sender address.
    /// Source: gascity's `Bead.From` (`beads.go:22` in gascity), populated
    /// from `beadmail.go:51`.
    pub msg_from: Lww<MailAddress>,

    /// Subject line. Stored in `title` on the underlying bead in gascity
    /// (`beadmail.go:46`). beads-rs can reuse `title` too — but naming it
    /// `msg_subject` here clarifies the type-specific semantic.
    /// In practice: store in `title`, surface via `subject` alias in the
    /// wire shape. Not a duplicate field.
    pub msg_subject: Lww<String>,    // aliased to `title`

    /// Body. Stored in `description` on the underlying bead
    /// (`beadmail.go:47`).
    pub msg_body: Lww<String>,       // aliased to `description`

    /// Read state — union of "read marker" events.
    /// Source: gascity stores this as a `"read"` label
    /// (`beadmail.go:99`). beads-rs lifts to a typed Lww because the flag
    /// is not accretive (you can unread; gascity uses RemoveLabels).
    pub msg_read: Lww<bool>,

    /// Thread identifier — all messages in a conversation share this.
    /// Source: gascity uses a `"thread:<id>"` label (`beadmail.go:35`).
    /// Write-once at creation.
    pub msg_thread: Lww<ThreadId>,

    /// When this message is a reply, the id of the message it replies to.
    /// Source: gascity uses a `"reply-to:<id>"` label (`beadmail.go:160`).
    /// beads-rs lifts to a typed BeadId field AND additionally adds a
    /// `DepKind::RepliesTo` dep edge (Go `types.go:780`) for graph queries.
    pub msg_reply_to: Lww<Option<BeadId>>,

    /// Carbon-copy recipients. Source: Go `Message.CC` at
    /// `gascity/internal/mail/mail.go:31`. gascity's beadmail does not
    /// currently populate CC (a forward gap); preserve the typed surface.
    pub msg_cc: OrSet<MailAddress>,

    /// Rig/namespace scoping. Source: Go `Message.Rig` at
    /// `gascity/internal/mail/mail.go:32`.
    pub msg_rig: Lww<Option<RigId>>,
}
```

| Field | Type | CRDT | Source (gascity) | Default |
|---|---|---|---|---|
| `msg_to` | `MailAddress` | `Lww` | `Bead.Assignee` | required at creation |
| `msg_from` | `MailAddress` | `Lww` | `Bead.From` | required at creation |
| `msg_subject` | aliased `title` | `Lww` | `Bead.Title` | required |
| `msg_body` | aliased `description` | `Lww` | `Bead.Description` | empty ok |
| `msg_read` | `bool` | `Lww` | `read` label presence | `false` |
| `msg_thread` | `ThreadId` | `Lww` (write-once) | `thread:<id>` label | required (generated) |
| `msg_reply_to` | `Option<BeadId>` | `Lww` (write-once) | `reply-to:<id>` label | `None` |
| `msg_cc` | `OrSet<MailAddress>` | `OrSet` union | `Message.CC` | empty |
| `msg_rig` | `Option<RigId>` | `Lww` | `Message.Rig` | `None` |

### CRDT merge behavior

- `msg_read` is `Lww` not `OrSet`: gascity explicitly supports MarkUnread
  (`beadmail.go:102-108`). If two agents concurrently toggle, last stamp
  wins — deterministic.
- `msg_cc` is `OrSet` so concurrent "add CC alice" and "add CC bob"
  converge to both.
- `msg_thread` and `msg_reply_to` are write-once via `Lww`: apply layer
  rejects updates post-creation. (Using `Lww` instead of a dedicated
  write-once primitive keeps the CRDT surface uniform; the immutability
  invariant is validated, not typed.)

## Enum variants

No new enums. `MailAddress`, `ThreadId`, `RigId` are newtype wrappers, not
sums.

```rust
pub struct MailAddress(String);  // validated syntactically
pub struct ThreadId(Uuid);       // new at first message, inherited by replies
pub struct RigId(String);        // see rig.md
```

## Lifecycle + invariants

- Workflow: `Open → Closed` (closed = archived). gascity's
  `Archive(id)` calls `store.Close` (`beadmail.go:111-126`); `Delete` is
  an alias for `Archive`.
- **Read transitions**: `msg_read` flips `false ↔ true` freely; not tied
  to workflow.
- **Invariant**: `msg_to != ""` at creation (gascity
  `beadmail.go:32-34`). beads-rs makes this unrepresentable via the
  required-field construction path.
- **Invariant**: if `msg_reply_to.is_some()`, the referenced bead must
  exist AND have the same `msg_thread`. Gascity does this implicitly via
  label inheritance (`beadmail.go:155-160`). beads-rs should validate at
  apply time.
- **Ready-exclusion**: YES. Excluded by Go's `readyExcludeTypes`
  (gascity `internal/beads/beads.go:88`). Mail is not work.
- **Container behavior**: none directly. Threading is expressed via
  `DepKind::RepliesTo` edges.

## Dependencies

- `DepKind::RepliesTo` (new; Go `types.go:780`): reply-message →
  original-message. beads-rs needs this dep kind.
- `DepKind::Related` for loose associations (e.g. a message mentioning
  a task).

## Wire shape

```json
{
  "id": "bd-msg-a1",
  "issue_type": "message",
  "title": "re: CRDT audit findings",
  "description": "full body here...",
  "status": "open",
  "priority": 2,
  "workflow": { "state": "open" },
  "claim": { "state": "unclaimed" },

  "msg_to": "alice@gc",
  "msg_from": "carol@gc",
  "msg_read": false,
  "msg_thread": "t-abc-123",
  "msg_reply_to": "bd-msg-a0",
  "msg_cc": ["bob@gc"],
  "msg_rig": "gastown",

  "labels": [],
  "dependencies": [
    { "kind": "replies_to", "target": "bd-msg-a0" }
  ],
  "created": { ... },
  "updated_stamp": { ... },
  "content_hash": "..."
}
```

Type-specific: `msg_to`, `msg_from`, `msg_read`, `msg_thread`,
`msg_reply_to`, `msg_cc`, `msg_rig`. `title` / `description` are shared
envelope fields repurposed as subject/body.

## Parity status

- **Rust source**: not implemented. Add `BeadType::Message` plus
  `MessageFields` and the `DepKind::RepliesTo` variant.
- **Gap vs Go bd / gascity**:
  - Go bd: `Sender` is a typed Issue field (`types.go:79`). beads-rs stores
    it in `msg_from`, no duplication.
  - Gascity's beadmail stores `thread:<id>` and `reply-to:<id>` as labels
    (`beadmail.go:160`). beads-rs lifts to typed fields AND emits
    `DepKind::RepliesTo` edges. The labels become redundant — drop them
    at import.
- **Migration**: existing message beads:
  - Read `thread:*` label → `msg_thread`; drop label.
  - Read `reply-to:*` label → `msg_reply_to` + emit `DepKind::RepliesTo`
    dep; drop label.
  - Read `read` label presence → `msg_read`; drop label.
  - Populate `msg_to` from `Assignee`, `msg_from` from `From`.

## Revisit

- `MailAddress` validation rules: see `primitives/addressing.md` (TBD).
  Gascity accepts arbitrary identifiers, not only `user@host`. Decide
  whether to validate a structured form or accept any non-empty string.
- `msg_cc` OrSet uncovering a second question: is "uncc'ing" a thing? If
  yes, `OrSet` becomes wrong (OrSet is add-only) and we need an
  add-wins/remove-wins variant. Today gascity has no uncc API — keep
  `OrSet`.
