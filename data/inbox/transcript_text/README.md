Place transcript-style `.txt` files in this folder.

The Manual Corrections tab creates paste-in templates under `Transcripts/` using filenames like `A01234567_Doe_Jane.txt`. Those files are scanned by the same transcript-text parser because this folder is read recursively.

Recommended matching order:
- `config/transcript_text_manifest.csv` exact `source_file` match
- filename containing a student ID such as `A01234567`
- filename containing last and first name tokens

Supported content includes:
- term headers such as `Spring 2024`
- course rows
- `Term at a glance:` blocks
- `Credits`
- `Credit Comp %`
- `Term GPA`
- `Cum GPA`
- `Academic Standing`

Summary values may be pasted on the next line or same line, for example `Credits:` followed by `13`, or `Credits: 13`.

Important:
- transcript text is academic evidence only
- transcript history does not imply graduation
- a student is only marked as graduated if the text explicitly states graduation
