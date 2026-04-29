const appState = {
  config: null,
  issues: [],
  pendingIssueId: null,
};

const dom = {
  board: document.querySelector("#board"),
  repoInput: document.querySelector("#repo-input"),
  searchInput: document.querySelector("#search-input"),
  statusFilter: document.querySelector("#status-filter"),
  refreshButton: document.querySelector("#refresh-button"),
  issueCount: document.querySelector("#issue-count"),
  healthStatus: document.querySelector("#health-status"),
  flashMessage: document.querySelector("#flash-message"),
  createForm: document.querySelector("#create-form"),
  commentDialog: document.querySelector("#comment-dialog"),
  commentTitle: document.querySelector("#comment-title"),
  commentText: document.querySelector("#comment-text"),
  commentSave: document.querySelector("#comment-save"),
  commentCancel: document.querySelector("#comment-cancel"),
  cardTemplate: document.querySelector("#issue-card-template"),
};

const orderedStatuses = [
  "Todo",
  "In Progress",
  "Human Review",
  "Rework",
  "Merging",
  "Done",
  "Cancelled",
  "Duplicate",
];

async function boot() {
  dom.refreshButton.addEventListener("click", () => refreshBoard());
  dom.searchInput.addEventListener("input", renderBoard);
  dom.statusFilter.addEventListener("change", renderBoard);
  dom.createForm.addEventListener("submit", onCreateIssue);
  dom.commentSave.addEventListener("click", onSaveComment);
  dom.commentCancel.addEventListener("click", () => dom.commentDialog.close());

  appState.config = await fetchConfig();
  dom.repoInput.value = appState.config.repo || "";
  hydrateStatusFilter(appState.config.statuses || orderedStatuses);
  await refreshBoard();
}

async function fetchConfig() {
  const response = await fetch("/api/config");
  if (!response.ok) {
    throw new Error(`config request failed: ${response.status}`);
  }
  return response.json();
}

async function rpc(payload) {
  const body = {
    repo: dom.repoInput.value.trim(),
    ...payload,
  };

  const response = await fetch("/api/rpc", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  const parsed = await response.json();
  if (!response.ok) {
    const error = parsed.error || parsed.err || {};
    throw new Error(error.message || `request failed: ${response.status}`);
  }
  if (parsed.err) {
    throw new Error(parsed.err.message || "upstream request failed");
  }
  if (!parsed.ok) {
    throw new Error("unexpected response envelope");
  }
  return parsed.ok;
}

async function refreshBoard() {
  setFlash("Refreshing board...");
  try {
    await refreshHealth();
    const result = await rpc({ op: "tracker_list" });
    appState.issues = result.data || [];
    dom.issueCount.textContent = String(appState.issues.length);
    renderBoard();
    setFlash("Board refreshed.");
  } catch (error) {
    setFlash(error.message, true);
  }
}

async function refreshHealth() {
  const response = await fetch("/api/healthz");
  const payload = await response.json();
  dom.healthStatus.textContent = payload.ok ? "Healthy" : "Degraded";
}

function hydrateStatusFilter(statuses) {
  for (const status of statuses) {
    const option = document.createElement("option");
    option.value = status;
    option.textContent = status;
    dom.statusFilter.append(option);
  }
}

function renderBoard() {
  const search = dom.searchInput.value.trim().toLowerCase();
  const focusedStatus = dom.statusFilter.value;
  const groups = new Map(orderedStatuses.map((status) => [status, []]));

  const filtered = appState.issues.filter((issue) => {
    if (focusedStatus && issue.status !== focusedStatus) {
      return false;
    }
    if (!search) {
      return true;
    }
    const haystack = [
      issue.id,
      issue.identifier,
      issue.title,
      issue.description,
      ...(issue.labels || []),
    ]
      .join(" ")
      .toLowerCase();
    return haystack.includes(search);
  });

  for (const issue of filtered) {
    if (!groups.has(issue.status)) {
      groups.set(issue.status, []);
    }
    groups.get(issue.status).push(issue);
  }

  dom.board.innerHTML = "";
  for (const [status, issues] of groups) {
    const column = document.createElement("section");
    column.className = "board-column";

    const head = document.createElement("div");
    head.className = "column-head";
    head.innerHTML = `<h2>${status}</h2><span class="column-count">${issues.length}</span>`;
    column.append(head);

    if (issues.length === 0) {
      const empty = document.createElement("p");
      empty.className = "empty-column";
      empty.textContent = "No issues in this lane.";
      column.append(empty);
    }

    issues
      .sort((left, right) => left.priority - right.priority || left.identifier.localeCompare(right.identifier))
      .forEach((issue) => column.append(renderIssue(issue)));

    dom.board.append(column);
  }
}

function renderIssue(issue) {
  const fragment = dom.cardTemplate.content.cloneNode(true);
  const card = fragment.querySelector(".issue-card");
  card.dataset.issueId = issue.id;

  fragment.querySelector(".issue-id").textContent = issue.identifier;
  fragment.querySelector(".issue-title").textContent = issue.title;
  fragment.querySelector(".issue-description").textContent =
    issue.description || "No description provided.";
  fragment.querySelector(".priority-pill").textContent = `P${issue.priority}`;

  const labels = fragment.querySelector(".labels");
  const issueLabels = issue.labels || [];
  if (issueLabels.length === 0) {
    labels.innerHTML = '<span class="label-pill">No labels</span>';
  } else {
    for (const label of issueLabels) {
      const pill = document.createElement("span");
      pill.className = "label-pill";
      pill.textContent = label;
      labels.append(pill);
    }
  }

  const blockedBy = fragment.querySelector(".blocked-by");
  if ((issue.blocked_by || []).length > 0) {
    blockedBy.textContent =
      "Blocked by: " +
      issue.blocked_by
        .map((blocker) => `${blocker.identifier} [${blocker.status}]`)
        .join(", ");
  } else {
    blockedBy.textContent = "No open blockers.";
  }

  const statusSelect = fragment.querySelector(".status-select");
  for (const status of orderedStatuses) {
    const option = document.createElement("option");
    option.value = status;
    option.textContent = status;
    option.selected = status === issue.status;
    statusSelect.append(option);
  }

  fragment.querySelector(".move-button").addEventListener("click", async () => {
    try {
      setFlash(`Moving ${issue.identifier}...`);
      await rpc({
        op: "tracker_transition",
        id: issue.id,
        status: statusSelect.value,
      });
      await refreshBoard();
      setFlash(`${issue.identifier} moved to ${statusSelect.value}.`);
    } catch (error) {
      setFlash(error.message, true);
    }
  });

  fragment.querySelector(".comment-button").addEventListener("click", () => {
    appState.pendingIssueId = issue.id;
    dom.commentTitle.textContent = `${issue.identifier}: ${issue.title}`;
    dom.commentText.value = `## Codex Workpad\n- `;
    dom.commentDialog.showModal();
    dom.commentText.focus();
  });

  return fragment;
}

async function onCreateIssue(event) {
  event.preventDefault();
  const formData = new FormData(dom.createForm);
  try {
    setFlash("Creating issue...");
    await rpc({
      op: "create",
      title: formData.get("title"),
      description: formData.get("description"),
      type: "task",
      priority: Number(formData.get("priority")),
    });
    dom.createForm.reset();
    await refreshBoard();
    setFlash("Issue created.");
  } catch (error) {
    setFlash(error.message, true);
  }
}

async function onSaveComment() {
  if (!appState.pendingIssueId) {
    return;
  }
  try {
    setFlash("Adding tracker comment...");
    await rpc({
      op: "tracker_comment",
      id: appState.pendingIssueId,
      content: dom.commentText.value,
    });
    dom.commentDialog.close();
    await refreshBoard();
    setFlash("Comment added.");
  } catch (error) {
    setFlash(error.message, true);
  }
}

function setFlash(message, isError = false) {
  dom.flashMessage.textContent = message;
  dom.flashMessage.style.color = isError ? "var(--accent)" : "var(--olive)";
}

boot().catch((error) => {
  setFlash(error.message, true);
});
