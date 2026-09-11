# Local Path Configuration

For the shared sqlCompile browser dashboard, copy `sqlCompile_host.example.json`
to `sqlCompile_host.json` on the host only, then set its local data paths and
network port. See [shared dashboard setup](../docs/shared_dashboard_setup.md).
This is separate from the roster source configuration below. Do not replace
existing correction CSVs when updating code or configuring a new host.

The repository should track code, app configuration, tests, and templates only.
Raw student files, roster PDFs, grade reports, generated exports, and caches
should stay outside Git.

Set up each work computer like this:

```powershell
copy config\example_paths.yaml config\local_paths.yaml
notepad config\local_paths.yaml
```

Edit `config/local_paths.yaml` so `raw_data_root` points at the shared-drive
folder that contains the current FSL source data. The local file is ignored by
Git and can differ on each computer.

You can also point the pipeline at a different config file without copying it:

```powershell
$env:FSL_PATH_CONFIG = "C:\path\to\local_paths.yaml"
py run_canonical_pipeline.py
```

Or pass the config explicitly:

```powershell
py run_canonical_pipeline.py --config config\local_paths.yaml
```

The pipeline creates output and cache folders if needed. It does not create raw
source folders automatically because those should already exist on the shared
drive.
