# Shared sqlCompile Dashboard (No Sign-In)

One host computer runs the dashboard. Everyone else opens its address in a browser.
They do not install Python, SQL software, VS Code, or this repository. Everyone
uses the host's database and saved corrections; there is no copy to merge later.
The existing local dashboard command still works.

## Before sharing student records

There is deliberately **no sign-in and no read-only role**. Anyone who can reach
the dashboard can view, download, and edit the available student records. Hiding
host maintenance controls is not an authentication boundary.

Use an institution-approved, staff-only network or restricted VPN. Campus Wi-Fi
alone is not a staff access restriction. Ask IT to restrict inbound access to
approved computers/subnets. Do not forward this port through a router, publish
it on the internet, or use a public tunnel. Direct connections use HTTP, not
encrypted HTTPS; use an IT-managed HTTPS proxy or a trusted encrypted VPN when
needed. CORS and XSRF protection remain enabled, but neither replaces access
control. This setup does not identify who made an edit.

If Windows requires administrator approval for a firewall exception, IT must
approve that network access. The launcher does not change firewall settings or
bypass the computer's restrictions.

## 1. Choose and prepare the host

Use the computer with the complete, current sqlCompile data and corrections.
Keep it on and awake while others work. The existing `uv` setup is sufficient;
no separate SQLite server or DB Browser installation is needed.

Update the code on that computer. Do not overwrite its existing `config` or
`output` folders with empty folders from a fresh checkout.

When moving to a different host, stop editing first and copy these files from
the working computer, preserving their relative locations:

- `output/sqlCompile/sqlCompile.sqlite`
- `config/sqlCompile_manual_status.csv`
- `config/sqlCompile_duplicate_name_resolutions.csv`, if present
- `config/sqlCompile_duplicate_name_recheck.csv`, if present
- `config/sqlCompile_zero_member_periods.csv`, including chapter exceptions

These private files are intentionally not in Git. Pulling code does not transfer
weeks of manual decisions. Copy the `_backups` folders as well when available.
Check the cohort totals and saved manual rows against the old host before others
start work. An absent correction ledger means an empty ledger, not an automatic
import from the old dashboard.

Keep the live SQLite database and correction ledgers on the host's local disk,
outside OneDrive, Dropbox, or a shared/network drive. Copies of backups can go
to approved backup storage. Raw roster inputs may remain in their usual source
location; other dashboard users do not need them.

## 2. Configure the host once

The defaults use the existing sqlCompile file locations and port 8502. For a
custom location or port, create a host configuration from the example, only if
you do not already have one:

```powershell
Copy-Item config\sqlCompile_host.example.json config\sqlCompile_host.json
notepad config\sqlCompile_host.json
```

Set the five data paths to the host's authoritative files. Absolute paths are
supported; use forward slashes in JSON, for example
`C:/FSLData/config/sqlCompile_manual_status.csv`. Relative paths are resolved
from the project folder, not the configuration file's folder. Keep each setting
pointing to a different file. The private host configuration is ignored by Git.

`address: "0.0.0.0"` listens on the host's network interfaces. To test only on the
host, use `address: "127.0.0.1"`. Change `port` if 8502 is already occupied.

## 3. Start it

Double-click `Start_Shared_SQL_Dashboard.bat`, or run from the project folder:

```powershell
$env:UV_CACHE_DIR=".uv-cache"
uv run --with-requirements requirements.txt python -X utf8 run_sql_compile_dashboard.py --shared
```

The first run may download the added file-locking dependency. Leave the launcher
window open. The host prints a local address and any detected network addresses.
Open the local address first; then test the appropriate network address from one
approved second computer. `localhost` only works on the host itself.

Shared-mode address and port come from the host configuration. Do not append
`--server.port` in shared mode. A different configuration can be selected with
`--shared --host-config config/another_host.json`.

If the second computer cannot connect, confirm the host is running and awake,
the address is correct, and both computers have the required network/VPN access.
Ask IT about the inbound port if the local address works but the remote one does
not. Multiple printed addresses can include virtual adapters. Ask IT for a
stable hostname or reserved IP for a permanent bookmark.

## 4. Work together

- Everyone has the same filters, rates, Manual Checker, and editable Manual Rows.
- File locations, compilation, report generation, and legacy-file import controls
  stay out of the shared browser. Host maintenance remains available through the
  original local dashboard or command line.
- Manual saves are serialized and written atomically. Edits to different rows
  merge. A conflicting save from a stale session is rejected as a whole rather
  than silently replacing newer decisions. Saving the same decision again is OK.
- Each browser keeps its current data and unfinished edits until an explicit
  refresh. A lightweight check every 15 seconds announces changed saved records;
  it does not rebuild charts or discard drafts.
- Use **Refresh Dashboard Data** after saving to recalculate rates. Refresh
  replaces the current checker draft with saved data. Resolve a conflict by
  reviewing the newer saved decision before applying another correction.

Do not edit the live CSVs in Excel or modify the database in another SQL program
while the host is running. Those programs do not participate in the save locks.
Run only one authoritative host, not separate hosts over a synchronized folder.

## 5. Update rosters and recover files

Use the existing compilation command on the host, with the same configured data
paths. For custom host paths, pass the corresponding `--output` and
`--manual-status-file` options to `sqlCompile.py`; the compiler does not read the
host JSON. Roster source paths still come from the normal pipeline configuration.
Compilation preserves the manual ledgers. Updated SQLite tables are published
only after that database write finishes successfully; readers keep using the
prior complete file if the write fails. Browser users refresh to see new data.

Before a changed CSV or database is published, its previous contents are backed
up under `<file folder>/_backups/<file name>/`. The latest 10 backups per file
are retained after successful saves. No backup is created for an unchanged CSV.
These are recovery snapshots, not a complete audit trail or off-machine disaster
backup. Stop the host before restoring a backup to the original file path; keep
a separate copy of the current files first. Restart and verify the restored
records before resuming review.

To stop the host, press Ctrl+C in its launcher window. Restart after changing
code or host configuration. Existing browser bookmarks work again once the same
host address and port are available.

## References

- [Streamlit client-server architecture](https://docs.streamlit.io/develop/concepts/architecture/architecture)
- [SQLite guidance on network filesystems](https://sqlite.org/useovernet.html)
- [Streamlit server configuration](https://docs.streamlit.io/develop/api-reference/configuration/config.toml)
