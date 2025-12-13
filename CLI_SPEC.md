
**core parity (13 commands):**

```
init
create
show
list
ready
update
close
reopen
delete
claim / unclaim (they use update --status, we make explicit)
comment (they call it comment, we can alias to note)
dep add / dep rm / dep tree
status
```

**their flags we match:**

```
create:
  -t, --type
  -p, --priority
  -d, --description
  -a, --assignee
  -l, --labels
  --design
  --acceptance
  --deps (their format: "blocks:bd-1,discovered-from:bd-2")
  --parent

show:
  --json

list:
  -s, --status
  -t, --type
  -p, --priority
  -a, --assignee
  -l, --label
  -n, --limit
  --sort
  --json

update:
  --title
  -d, --description
  --design
  --acceptance
  -s, --status
  -p, --priority
  -a, --assignee
  --add-label
  --remove-label
  --notes

close:
  --reason

dep:
  add <from> <to>
  (their format handles kind via prefix)
```

**what we skip:**

```
blocked         → list -s blocked
search          → list "query"
stale           → list --sort updated
count           → list | wc -l
edit            → update
epic            → create -t epic + dep --parent
label           → update --add-label / --remove-label
comment/comments → single 'note' command
stats/info      → status
```

**what's automatic (no command needed):**

```
sync            → background
daemon          → auto-spawn
clean/cleanup   → git gc
compact         → tombstone TTL
merge           → CRDT
hooks           → not needed
doctor/validate → can't corrupt
repair-deps     → can't corrupt
```

**parity checklist:**

| command | status |
|---------|--------|
| init | need |
| create | need |
| show | need |
| list | need |
| ready | need |
| update | need |
| close | need |
| reopen | need |
| delete | need |
| claim | need (new) |
| unclaim | need (new) |
| note | need |
| dep add | need |
| dep rm | need |
| dep tree | need |
| status | need |


global flags:

--json             Machine-readable output
--repo <path>      Repository path (default: current dir)
--actor <string>   Actor identity (default: $BD_ACTOR or user@hostname)
-q, --quiet        Errors only
-v, --verbose      Debug output
--version


what we drop and why:
dropped	reason
--allow-stale	can't be stale, memory is truth
--db	no sqlite
--no-auto-flush	sync is automatic
--no-auto-import	no import, git is source
--no-daemon	daemon IS the architecture
--no-db	no dual mode
--profile	dev tooling, not user facing, reserve ability to add later
--sandbox	no sandbox needed
