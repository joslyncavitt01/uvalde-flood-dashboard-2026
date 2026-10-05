"""One-off/periodic manual load of five ShelterLuv "flood week" custom reports --
diagnostic tests, vaccines, physical exams, surgeries, and general treatments -- each
an event-level log (multiple rows per animal), unlike the single-row-per-animal
profile snapshot. NOT scoped to flood-attributable animals specifically; these are
org-wide reports for the date window, filtered down to flood animals later in
fetch_data.py via a join.

The surgeries report exists separately from the data team's own CompletedSurgeries
table because that table's sync stopped updating before 2026-06-17 -- well before the
flood -- so it can't be relied on for anything in the flood window. The treatments
report is the structured counterpart to the data team's live AnimalTreatments table
(same shape/columns) -- unlike CompletedSurgeries that one IS current, so this report
mainly exists to double-check it and to have a matching per-animal treatments history
on the Animals page, not because the live table was broken.

Each animal's "Attributes" tag list shows up in all five reports and is confirmed
consistent wherever it appears (checked directly), so it's read here but not stored
separately -- fetch_data.py pulls it live from whichever table has it for a given
animal.

Refresh model: same as backfill_animal_profiles.py -- manual, whenever Joslyn wants
this brought current. Each run fully replaces the destination table's contents
(WRITE_TRUNCATE load job, not DML), since these are periodic whole-window snapshots,
not incremental logs -- there's no stable per-row key to dedupe against across runs.
**This means loading a file is NOT additive: it replaces the whole table, so a file
that doesn't cover as far back as what's already loaded will delete the older rows.**
Each load now refuses to run if the incoming file's earliest date is later than the
existing table's earliest date, to make that failure loud instead of silent (see
load_file's date-coverage check). Pass --force as an extra CLI arg to skip the check
and accept the loss on purpose.

Usage: python3 backfill_animal_medical.py /path/to/floodweekdiagnostictests.xlsx \
    /path/to/floodweekvaccines.xlsx /path/to/floodweekphysicalexams.xlsx \
    /path/to/floodweeksurgeries.xlsx /path/to/floodweektreatments.xlsx [--force]
"""
import sys
from datetime import date, datetime
import openpyxl
from google.cloud import bigquery

PROJECT = "apa-data-410213"
DATASET = "shelterluv"

# The BQ column holding each row's own date, used only for the coverage-shrink guard below
# (not otherwise special -- each report's real date semantics live in its own field map).
DATE_COLUMN = {
    "diagnostic": "TestDate",
    "vaccine": "DateCompleted",
    "exam": "DateCompleted",
    "surgery": "DateCompleted",
    "treatment": "DateGiven",
}

DIAGNOSTICS_MAP = {
    "Animal ID": "AnimalID",
    "Name": "Name",
    "Species": "Species",
    "Primary Breed": "PrimaryBreed",
    "Current Location": "CurrentLocation",
    "Attributes": "Attributes",
    "Test Date": "TestDate",
    "Test Status": "TestStatus",
    "Test Name": "TestName",
    "Test Product": "TestProduct",
    "Test By": "TestBy",
    "Test Notes": "TestNotes",
    "Result Name": "ResultName",
    "Result": "Result",
}

VACCINES_MAP = {
    "Animal ID": "AnimalID",
    "Name": "Name",
    "Species": "Species",
    "Current Location": "CurrentLocation",
    "Attributes": "Attributes",
    "Date Completed": "DateCompleted",
    "Vaccine Product": "VaccineProduct",
    "Lot #": "LotNumber",
    "Vaccinated By": "VaccinatedBy",
    "Rabies Tag Number": "RabiesTagNumber",
    "Supervising Veterinarian": "SupervisingVeterinarian",
}

SURGERIES_MAP = {
    "Animal ID": "AnimalID",
    "Name": "Name",
    "Species": "Species",
    "Primary Breed": "PrimaryBreed",
    "Sex": "Sex",
    "Age (Y/M/D)": "AgeYMD",
    "Altered In Care": "AlteredInCare",
    "Altered Before Arrival": "AlteredBeforeArrival",
    "Current Location": "CurrentLocation",
    "Current Status": "CurrentStatus",
    "Attributes": "Attributes",
    "Current Weight": "CurrentWeight",
    "Date Completed": "DateCompleted",
    "Procedure/Surgery Type": "SurgeryType",
    "Surgeon": "Surgeon",
    "Clinic": "Clinic",
    "Memo": "Memo",
}

TREATMENTS_MAP = {
    "Animal ID": "AnimalID",
    "Name": "Name",
    "Species": "Species",
    "Primary Breed": "PrimaryBreed",
    "Current Location": "CurrentLocation",
    "Attributes": "Attributes",
    "Current Weight": "CurrentWeight",
    "Date Given": "DateGiven",
    "Time Given": "TimeGiven",
    "Given By": "GivenBy",
    "Product": "Product",
    "Amount": "Amount",
    "Dose Notes": "DoseNotes",
    "Treatment Notes": "TreatmentNotes",
    "Supervising Veterinarian": "SupervisingVeterinarian",
}

EXAMS_MAP = {
    "Animal ID": "AnimalID",
    "Name": "Name",
    "Species": "Species",
    "Primary Breed": "PrimaryBreed",
    "Secondary Breed": "SecondaryBreed",
    "Sex": "Sex",
    "Altered": "Altered",
    "Current Location": "CurrentLocation",
    "Current Status": "CurrentStatus",
    "Attributes": "Attributes",
    "Date Completed": "DateCompleted",
    "Vet or Tech Exam": "VetOrTechExam",
    "Type": "Type",
    "Exam Reason": "ExamReason",
    "Subjective": "Subjective",
    "Objective": "Objective",
    "Assessment": "Assessment",
    "Plan": "Plan",
    "New Diagnoses": "NewDiagnoses",
    "Performed By": "PerformedBy",
}

TARGETS = {
    "diagnostic": ("DiagnosticTestsJoslyn", DIAGNOSTICS_MAP),
    "vaccine": ("VaccinesJoslyn", VACCINES_MAP),
    "exam": ("PhysicalExamsJoslyn", EXAMS_MAP),
    "surgery": ("SurgeriesJoslyn", SURGERIES_MAP),
    "treatment": ("TreatmentsJoslyn", TREATMENTS_MAP),
}


def clean(v):
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "—", "-"):
        return None
    return s


def detect_kind(headers):
    if "Test Name" in headers:
        return "diagnostic"
    if "Vaccine Product" in headers:
        return "vaccine"
    if "Exam Reason" in headers:
        return "exam"
    if "Procedure/Surgery Type" in headers:
        return "surgery"
    if "Time Given" in headers:
        return "treatment"
    raise ValueError(f"Couldn't identify report type from headers: {headers[:5]}...")


def parse_date(s):
    # Must handle two different input shapes: a raw "%m/%d/%Y" string when called on rows
    # freshly read from an xlsx (via clean()), and a native datetime.date/datetime when
    # called on rows queried back from BigQuery (autodetect=True typed the date columns
    # as DATE, so they come back as date objects, not strings). Confirmed broken 2026-10-05:
    # strptime() on a date object raises TypeError, which this used to swallow silently and
    # return None for every single existing row, making the coverage-shrink guard below a
    # complete no-op without ever printing a warning.
    if not s:
        return None
    if isinstance(s, datetime):
        return s.date()
    if isinstance(s, date):
        return s
    try:
        return datetime.strptime(s, "%m/%d/%Y").date()
    except (ValueError, TypeError):
        return None


def to_json_safe(v):
    """BigQuery query results can hand back native date/datetime objects (for whichever
    column autodetect typed as DATE) that load_table_from_json can't serialize -- convert
    back to the same MM/DD/YYYY string shape rows_out already uses, everything else as-is."""
    if isinstance(v, datetime):
        return v.date().strftime("%m/%d/%Y")
    if isinstance(v, date):
        return v.strftime("%m/%d/%Y")
    return v


def load_file(client, path, force=False, merge=False):
    wb = openpyxl.load_workbook(path, data_only=True)
    ws = wb.active
    headers = [c.value for c in ws[1]]
    col_idx = {h: i for i, h in enumerate(headers)}

    kind = detect_kind(headers)
    table_name, field_map = TARGETS[kind]

    rows_out = []
    for raw in ws.iter_rows(min_row=2, values_only=True):
        row = {}
        for report_col, bq_col in field_map.items():
            idx = col_idx.get(report_col)
            row[bq_col] = clean(raw[idx]) if idx is not None else None
        if row.get("AnimalID"):
            row["AnimalID"] = row["AnimalID"].replace("APA-A-", "")
            rows_out.append(row)

    print(f"{path.split('/')[-1]}: parsed {len(rows_out)} rows -> {table_name} ({kind})")

    table_ref = f"{PROJECT}.{DATASET}.{table_name}"

    # WRITE_TRUNCATE below fully replaces this table -- it is not additive. If the file
    # being loaded doesn't cover as far back as what's already there, this would silently
    # delete the older rows instead of adding to them (confirmed to happen for real,
    # 2026-09-17: a same-day batch of 5 reports that all started 8/1+ wiped every table's
    # 7/13-7/31 flood-week history at once). Refuse to proceed when that's about to happen.
    date_col = DATE_COLUMN.get(kind)
    if date_col and not force and not merge:
        new_dates = [d for d in (parse_date(r.get(date_col)) for r in rows_out) if d]
        new_min = min(new_dates) if new_dates else None
        try:
            existing_rows = list(client.query(
                f"SELECT {date_col} FROM `{table_ref}`"
            ).result())
            existing_dates = [d for d in (parse_date(r[date_col]) for r in existing_rows) if d]
            existing_min = min(existing_dates) if existing_dates else None
            if existing_rows and not existing_dates:
                # Every row failed to parse -- almost certainly a bug in parse_date for
                # whatever type this column is now coming back as, NOT "no existing data".
                # Say so loudly instead of silently treating it as nothing-to-protect.
                print(
                    f"  WARNING: {len(existing_rows)} existing {table_name} rows found but "
                    f"none parsed as dates (sample raw value: {existing_rows[0][date_col]!r}) "
                    f"-- coverage-shrink check could not run, proceeding without it."
                )
        except Exception as e:
            existing_min = None
            print(f"  WARNING: couldn't check existing {table_name} coverage ({e}) -- proceeding without that check.")

        if existing_min and new_min and new_min > existing_min:
            print(
                f"  REFUSING TO LOAD: existing {table_name} data goes back to {existing_min}, "
                f"but this file only starts at {new_min}. Loading it would delete the "
                f"{existing_min}-to-{new_min} window (full-table replace, not additive)."
            )
            print(
                f"  Pull a report covering back to at least {existing_min} and re-run, or pass "
                f"--merge to union this file with what's already in the table (safe when the "
                f"new file is a non-overlapping continuation window), or --force to load anyway "
                f"and accept the loss."
            )
            return

    if merge:
        # Union with whatever's already in the table instead of requiring a fresh full-range
        # export -- safe as long as the new file doesn't overlap the existing date range (a
        # same-event row appearing in both would get double-counted; this only dedupes EXACT
        # duplicate rows, not near-duplicates from genuinely overlapping windows).
        bq_cols = list(field_map.values())
        try:
            existing_rows = list(client.query(
                f"SELECT {', '.join(bq_cols)} FROM `{table_ref}`"
            ).result())
        except Exception as e:
            print(f"  ERROR: --merge requested but couldn't read existing {table_name} rows ({e}). Aborting, nothing loaded.")
            return
        existing_dicts = [{c: to_json_safe(r[c]) for c in bq_cols} for r in existing_rows]
        seen = set()
        combined = []
        for row in existing_dicts + rows_out:
            key = tuple(row.get(c) for c in bq_cols)
            if key in seen:
                continue
            seen.add(key)
            combined.append(row)
        print(f"  Merging {len(existing_dicts)} existing rows + {len(rows_out)} new rows -> {len(combined)} rows after exact-duplicate dedup")
        rows_out = combined

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(rows_out, table_ref, job_config=job_config)
    job.result()
    print(f"  Loaded {len(rows_out)} rows into {table_ref} (full replace).")


def run(paths):
    force = "--force" in paths
    merge = "--merge" in paths
    paths = [p for p in paths if p not in ("--force", "--merge")]
    client = bigquery.Client(project=PROJECT)
    for path in paths:
        load_file(client, path, force=force, merge=merge)


if __name__ == "__main__":
    run(sys.argv[1:])
