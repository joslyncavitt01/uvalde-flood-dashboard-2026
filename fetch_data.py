import json
import os
import urllib.request
from datetime import datetime, timezone
from google.cloud import bigquery
from shapely.geometry import shape, Point

PROJECT = "apa-grants-manager"

# Live TAHC NWS zone feed -- same source the tx-flood-radar-shelters map uses.
# Fetched fresh on every run so zone tags never go stale even if TAHC updates the boundaries.
TAHC_ZONES_URL = (
    "https://services1.arcgis.com/9Astik9VqLUMFtxK/arcgis/rest/services/"
    "NWS_Zones_and_Areas_PUBLIC_VIEW/FeatureServer/33/query"
    "?where=1%3D1&outFields=*&f=geojson&outSR=4326"
)

QUERY = f"""
SELECT
  animalInternalID,
  animalAID,
  name,
  species,
  intakeDate,
  originShelter,
  shelterCity,
  shelterCounty,
  shelterLat,
  shelterLon,
  currentStatus,
  dispositionBucket,
  lastOutcomeType,
  lastOutcomeDate,
  transferredTo,
  currentLocationTier1,
  currentLocationTier2,
  foundCity,
  foundCounty
FROM `{PROJECT}.shelterluv.flood_animals`
"""

# Profile snapshot lives in the data team's project (apa-data-410213), loaded manually
# and periodically via appsscript/backfill_animal_profiles.py from a rich ShelterLuv
# export -- not every flood animal has a match, since that export is scoped to recently
# created animals, not an org-wide snapshot. Missing profile fields just render as blank
# on the animal detail page.
PROFILE_QUERY = """
SELECT
  AnimalID, Name, Species, PrimaryBreed, SecondaryBreed, Sex, AgeYMD, AgeGroup,
  PrimaryColor, SecondaryColor, Pattern, CurrentWeight, AdoptionCategory,
  BehaviorCategory, MedicalCategory, VolunteerCategory, AlteredInCare,
  AlteredBeforeArrival, FosterPersonName, FosterPersonCity, FosterPersonState,
  Photo, Video, KennelCardMemo, MemosJSON
FROM `apa-data-410213.shelterluv.AnimalProfileSnapshotJoslyn`
"""

# Three more manually-loaded "flood week" reports (see backfill_animal_medical.py) --
# event-level logs (multiple rows per animal), org-wide rather than flood-scoped, joined
# down to just flood animals here. Attributes shows up in all three and is consistent
# wherever an animal appears in more than one, so it's read from whichever has it.
DIAGNOSTICS_QUERY = """
SELECT CAST(AnimalID AS STRING) AS AnimalID, TestDate, TestStatus, TestName, TestProduct,
  TestBy, TestNotes, ResultName, Result, Attributes
FROM `apa-data-410213.shelterluv.DiagnosticTestsJoslyn`
"""
VACCINES_QUERY = """
SELECT CAST(AnimalID AS STRING) AS AnimalID, DateCompleted, VaccineProduct, LotNumber,
  VaccinatedBy, RabiesTagNumber, SupervisingVeterinarian, Attributes
FROM `apa-data-410213.shelterluv.VaccinesJoslyn`
"""
EXAMS_QUERY = """
SELECT CAST(AnimalID AS STRING) AS AnimalID, DateCompleted, VetOrTechExam, Type, ExamReason,
  Subjective, Objective, Assessment, Plan, NewDiagnoses, PerformedBy, Attributes
FROM `apa-data-410213.shelterluv.PhysicalExamsJoslyn`
"""
SURGERIES_QUERY = """
SELECT CAST(AnimalID AS STRING) AS AnimalID, DateCompleted, SurgeryType, Surgeon, Clinic,
  Memo, Attributes
FROM `apa-data-410213.shelterluv.SurgeriesJoslyn`
"""
TREATMENTS_QUERY = """
SELECT CAST(AnimalID AS STRING) AS AnimalID, DateGiven, TimeGiven, GivenBy, Product,
  Amount, DoseNotes, TreatmentNotes, SupervisingVeterinarian, Attributes
FROM `apa-data-410213.shelterluv.TreatmentsJoslyn`
"""


def fetch_nws_zones():
    """Returns (infested_polys, surveillance_polys), or ([], []) if the live feed is unreachable."""
    try:
        with urllib.request.urlopen(TAHC_ZONES_URL, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"WARNING: couldn't fetch live TAHC zones ({e}); shelters will show no zone tag this run.")
        return [], []

    infested, surveillance = [], []
    for f in data.get("features", []):
        try:
            geom = shape(f["geometry"])
        except Exception:
            continue
        zone = f.get("properties", {}).get("zone_name")
        if zone == 1:
            infested.append(geom)
        elif zone == 2:
            surveillance.append(geom)
    return infested, surveillance


def zone_for(lat, lon, infested, surveillance):
    if lat is None or lon is None:
        return None
    p = Point(lon, lat)
    if any(poly.contains(p) for poly in infested):
        return "Infested Zone"
    if any(poly.contains(p) for poly in surveillance):
        return "Surveillance Zone"
    return None


def run():
    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(QUERY).result())
    profile_rows = list(client.query(PROFILE_QUERY).result())
    profiles = {r.AnimalID: r for r in profile_rows}

    def group_by_animal(query):
        grouped = {}
        for r in client.query(query).result():
            grouped.setdefault(r.AnimalID, []).append(r)
        return grouped

    diagnostics_by_animal = group_by_animal(DIAGNOSTICS_QUERY)
    vaccines_by_animal = group_by_animal(VACCINES_QUERY)
    exams_by_animal = group_by_animal(EXAMS_QUERY)
    surgeries_by_animal = group_by_animal(SURGERIES_QUERY)
    treatments_by_animal = group_by_animal(TREATMENTS_QUERY)

    def attributes_for(aid):
        for source in (diagnostics_by_animal, vaccines_by_animal, exams_by_animal, surgeries_by_animal, treatments_by_animal):
            for r in source.get(aid, []):
                if r.Attributes:
                    return sorted(set(t.strip() for t in r.Attributes.split(",") if t.strip()))
        return []

    infested, surveillance = fetch_nws_zones()
    zone_cache = {}

    animals = []
    for r in rows:
        animals.append({
            "id": r.animalInternalID,
            "aid": r.animalAID,
            "name": r.name,
            "species": r.species,
            "intakeDate": str(r.intakeDate),
            "shelter": r.originShelter,
            "shelterCity": r.shelterCity,
            "shelterCounty": r.shelterCounty,
            "shelterLat": r.shelterLat,
            "shelterLon": r.shelterLon,
            "status": r.currentStatus,
            "bucket": r.dispositionBucket,
            "outcomeType": r.lastOutcomeType,
            "outcomeDate": str(r.lastOutcomeDate.date()) if r.lastOutcomeDate else None,
            "transferredTo": r.transferredTo,
            "property": r.currentLocationTier1,
            "area": r.currentLocationTier2,
            "foundCity": r.foundCity,
            "foundCounty": r.foundCounty,
        })

    # Per-animal profile detail (breed, photo, memos, etc.) for the filterable Animals
    # page -- joined in Python rather than SQL since the profile table lives in a
    # different project and only ~80% of flood animals have a matching row.
    # Deceased animals stay in this list (so the total count here still matches the
    # flood-attributable total everywhere else) but the page itself never renders a
    # card for them -- that's handled client-side in animals.html, not by dropping
    # them from the data. Return-to-Owner is split out of the generic "Adopted /
    # Pending" bucket here (using the true outcome type) so an owned pet going home
    # reads differently from an adoption.
    PROFILE_BUCKETS = [
        "On Property",
        "In Foster (Available for Adoption)",
        "In Foster (Unavailable for Adoption)",
        "Safety Net Foster (Pending RTO)",
        "Adopted / Pending",
        "Return to Owner",
        "Transferred Out",
    ]

    animal_profiles_out = []
    for a in animals:
        p = profiles.get(a["aid"])
        memos = {}
        if p and p.MemosJSON:
            try:
                memos = json.loads(p.MemosJSON)
            except (json.JSONDecodeError, TypeError):
                memos = {}
        display_bucket = a["bucket"]
        if display_bucket == "Adopted / Pending" and a["outcomeType"] == "Outcome.ReturnToOwner":
            display_bucket = "Return to Owner"
        animal_profiles_out.append({
            "id": a["id"],
            "aid": a["aid"],
            "name": a["name"],
            "species": a["species"],
            "intakeDate": a["intakeDate"],
            "shelter": a["shelter"],
            "shelterCity": a["shelterCity"],
            "shelterCounty": a["shelterCounty"],
            "status": a["status"],
            "bucket": display_bucket,
            "outcomeType": a["outcomeType"],
            "outcomeDate": a["outcomeDate"],
            "transferredTo": a["transferredTo"],
            "property": a["property"],
            "area": a["area"],
            "foundCity": a["foundCity"],
            "foundCounty": a["foundCounty"],
            "breed": p.PrimaryBreed if p else None,
            "secondaryBreed": p.SecondaryBreed if p else None,
            "sex": p.Sex if p else None,
            "age": p.AgeYMD if p else None,
            "ageGroup": p.AgeGroup if p else None,
            "color": p.PrimaryColor if p else None,
            "secondaryColor": p.SecondaryColor if p else None,
            "pattern": p.Pattern if p else None,
            "weight": p.CurrentWeight if p else None,
            "adoptionCategory": p.AdoptionCategory if p else None,
            "behaviorCategory": p.BehaviorCategory if p else None,
            "medicalCategory": p.MedicalCategory if p else None,
            "volunteerCategory": p.VolunteerCategory if p else None,
            "alteredInCare": p.AlteredInCare if p else None,
            "alteredBeforeArrival": p.AlteredBeforeArrival if p else None,
            "fosterName": p.FosterPersonName if p else None,
            "fosterCity": p.FosterPersonCity if p else None,
            "fosterState": p.FosterPersonState if p else None,
            "photo": p.Photo if p else None,
            "video": p.Video if p else None,
            "kennelCardMemo": p.KennelCardMemo if p else None,
            "memos": memos,
            "hasProfile": p is not None,
            "attributes": attributes_for(a["aid"]),
            "diagnosticTests": [
                {
                    "date": str(r.TestDate) if r.TestDate else None,
                    "status": r.TestStatus,
                    "name": r.TestName,
                    "product": r.TestProduct,
                    "by": r.TestBy,
                    "notes": r.TestNotes,
                    "resultName": r.ResultName,
                    "result": r.Result,
                }
                for r in diagnostics_by_animal.get(a["aid"], [])
            ],
            "vaccines": [
                {
                    "date": str(r.DateCompleted) if r.DateCompleted else None,
                    "product": r.VaccineProduct,
                    "lotNumber": r.LotNumber,
                    "by": r.VaccinatedBy,
                    "rabiesTag": r.RabiesTagNumber,
                    "vet": r.SupervisingVeterinarian,
                }
                for r in vaccines_by_animal.get(a["aid"], [])
            ],
            "physicalExams": [
                {
                    "date": str(r.DateCompleted) if r.DateCompleted else None,
                    "examType": r.Type,
                    "vetOrTech": r.VetOrTechExam,
                    "reason": r.ExamReason,
                    "subjective": r.Subjective,
                    "objective": r.Objective,
                    "assessment": r.Assessment,
                    "plan": r.Plan,
                    "newDiagnoses": r.NewDiagnoses,
                    "performedBy": r.PerformedBy,
                }
                for r in exams_by_animal.get(a["aid"], [])
            ],
            "surgeries": [
                {
                    "date": str(r.DateCompleted) if r.DateCompleted else None,
                    "surgeryType": r.SurgeryType,
                    "surgeon": r.Surgeon,
                    "clinic": r.Clinic,
                    "memo": r.Memo,
                }
                for r in surgeries_by_animal.get(a["aid"], [])
            ],
            "treatments": [
                {
                    "date": str(r.DateGiven) if r.DateGiven else None,
                    "time": r.TimeGiven,
                    "by": r.GivenBy,
                    "product": r.Product,
                    "amount": r.Amount,
                    "doseNotes": r.DoseNotes,
                    "treatmentNotes": r.TreatmentNotes,
                    "vet": r.SupervisingVeterinarian,
                }
                for r in treatments_by_animal.get(a["aid"], [])
            ],
        })

    # Medical/preventive-care metrics for the homepage grid -- all scoped to flood
    # animals only (animal_profiles_out is already that list), summed from the same
    # per-animal event lists attached above rather than re-querying.
    def is_positive_heartworm(test):
        return "heartworm" in (test["name"] or "").lower() and (test["result"] or "").lower() == "positive"

    def is_spay_neuter(surgery):
        t = (surgery["surgeryType"] or "").lower()
        return "spay" in t or "neuter" in t

    medical_metrics = {
        "vaccinesAdministered": sum(len(a["vaccines"]) for a in animal_profiles_out),
        "heartwormPositive": sum(
            1 for a in animal_profiles_out if any(is_positive_heartworm(t) for t in a["diagnosticTests"])
        ),
        "capstarDoses": sum(
            1 for a in animal_profiles_out for t in a["treatments"]
            if "capstar" in (t["product"] or "").lower()
        ),
        "spayNeuterSurgeries": sum(
            1 for a in animal_profiles_out for s in a["surgeries"] if is_spay_neuter(s)
        ),
    }

    buckets = [
        "On Property",
        "In Foster (Available for Adoption)",
        "In Foster (Unavailable for Adoption)",
        "Safety Net Foster (Pending RTO)",
        "Adopted / Pending",
        "Transferred Out",
        "Deceased",
    ]

    # Totals
    totals = {
        "total": len(animals),
        "dogs": sum(1 for a in animals if a["species"] == "Dog"),
        "cats": sum(1 for a in animals if a["species"] == "Cat"),
    }
    for b in buckets:
        totals[b] = sum(1 for a in animals if a["bucket"] == b)
    # dispositionBucket classifies "Adopted / Pending" off ShelterLuv's currentStatus text
    # (e.g. "Healthy in Home"), which doesn't distinguish a true adoption from an owned pet
    # being returned to its owner -- but the true outcomeType does (Outcome.ReturnToOwner is
    # its own top-level outcome, a sibling of Outcome.Adoption, not a subtype of it). Split
    # these two apart for the top-line tiles only (chart/shelter table keep the unsplit
    # "Adopted / Pending" bucket, per explicit scope) so the tiles don't double-count the
    # same animal under both "Adopted / Pending" and "Return to Owner."
    totals["Return to Owner"] = sum(
        1 for a in animals
        if a["bucket"] == "Adopted / Pending" and a["outcomeType"] == "Outcome.ReturnToOwner"
    )
    totals["Adopted / Pending (tile)"] = totals["Adopted / Pending"] - totals["Return to Owner"]

    # By day
    by_day = {}
    for a in animals:
        d = a["intakeDate"]
        if d not in by_day:
            by_day[d] = {"date": d, "total": 0, **{b: 0 for b in buckets}}
        by_day[d]["total"] += 1
        by_day[d][a["bucket"]] = by_day[d].get(a["bucket"], 0) + 1
    days_out = [by_day[d] for d in sorted(by_day.keys())]

    # By shelter (with city/county/live NWS zone tag) -- excludes field intakes (no shelter
    # of origin), which get their own section below grouped by the city they were found in.
    by_shelter = {}
    for a in animals:
        s = a["shelter"]
        if s.startswith("Field intake"):
            continue
        if s not in by_shelter:
            key = (a["shelterLat"], a["shelterLon"])
            if key not in zone_cache:
                zone_cache[key] = zone_for(a["shelterLat"], a["shelterLon"], infested, surveillance)
            by_shelter[s] = {
                "shelter": s, "total": 0, "dogs": 0, "cats": 0,
                "city": a["shelterCity"], "county": a["shelterCounty"],
                "nwsZone": zone_cache[key],
                "firstIntake": a["intakeDate"], "lastIntake": a["intakeDate"],
                **{b: 0 for b in buckets},
            }
        rec = by_shelter[s]
        rec["total"] += 1
        rec["dogs"] += 1 if a["species"] == "Dog" else 0
        rec["cats"] += 1 if a["species"] == "Cat" else 0
        rec["firstIntake"] = min(rec["firstIntake"], a["intakeDate"])
        rec["lastIntake"] = max(rec["lastIntake"], a["intakeDate"])
        rec[a["bucket"]] = rec.get(a["bucket"], 0) + 1
    shelters_out = sorted(by_shelter.values(), key=lambda r: -r["total"])

    # Field intakes (no shelter of origin) -- owned/found pets, grouped by where they
    # were found rather than by shelter, since there isn't one. Expected to grow over
    # time as more of these get logged.
    # Keyed on city alone -- county is informational only. Different intake batches
    # (core sync vs. emailed snapshot) don't always carry county for the same city, and
    # keying on (city, county) was splitting one city into multiple rows whenever a
    # later batch came in with a blank county.
    by_found_city = {}
    for a in animals:
        if not a["shelter"].startswith("Field intake"):
            continue
        city = a["foundCity"] or "Unknown"
        county = a["foundCounty"] or ""
        if city not in by_found_city:
            by_found_city[city] = {
                "city": city, "county": county, "total": 0, "dogs": 0, "cats": 0,
                "firstIntake": a["intakeDate"], "lastIntake": a["intakeDate"],
                **{b: 0 for b in buckets},
            }
        rec = by_found_city[city]
        if not rec["county"] and county:
            rec["county"] = county
        rec["total"] += 1
        rec["dogs"] += 1 if a["species"] == "Dog" else 0
        rec["cats"] += 1 if a["species"] == "Cat" else 0
        rec["firstIntake"] = min(rec["firstIntake"], a["intakeDate"])
        rec["lastIntake"] = max(rec["lastIntake"], a["intakeDate"])
        rec[a["bucket"]] = rec.get(a["bucket"], 0) + 1
    found_cities_out = sorted(by_found_city.values(), key=lambda r: -r["total"])

    # On-property animals, by location
    by_location = {}
    for a in animals:
        if a["bucket"] != "On Property":
            continue
        prop = a["property"] or "Unspecified"
        area = (a["area"] or "Unspecified").strip()
        key = (prop, area)
        if key not in by_location:
            by_location[key] = {"property": prop, "area": area, "total": 0, "dogs": 0, "cats": 0}
        rec = by_location[key]
        rec["total"] += 1
        rec["dogs"] += 1 if a["species"] == "Dog" else 0
        rec["cats"] += 1 if a["species"] == "Cat" else 0
    locations_out = sorted(by_location.values(), key=lambda r: (r["property"], -r["total"]))

    # Status detail -- exact ShelterLuv status text, grouped under its parent bucket,
    # so specific reasons (medical hold, too young, plain "unavailable", etc.) are visible
    # rather than collapsed into the top-level available/unavailable split.
    by_status = {}
    for a in animals:
        key = (a["bucket"], a["status"])
        if key not in by_status:
            by_status[key] = {"bucket": a["bucket"], "status": a["status"], "total": 0, "dogs": 0, "cats": 0}
        rec = by_status[key]
        rec["total"] += 1
        rec["dogs"] += 1 if a["species"] == "Dog" else 0
        rec["cats"] += 1 if a["species"] == "Cat" else 0
    bucket_order = {b: i for i, b in enumerate(buckets)}
    status_detail_out = sorted(
        by_status.values(),
        key=lambda r: (bucket_order.get(r["bucket"], 99), -r["total"]),
    )

    # Transfer destinations, by date so waves of transport are visible
    dest = {}
    for a in animals:
        if a["bucket"] == "Transferred Out" and a["transferredTo"]:
            key = (a["outcomeDate"], a["transferredTo"])
            dest[key] = dest.get(key, 0) + 1
    destinations_out = sorted(
        [{"date": k[0], "destination": k[1], "count": v} for k, v in dest.items()],
        key=lambda r: (r["date"] or "", -r["count"]),
    )

    output = {
        "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totals": totals,
        "byDay": days_out,
        "byShelter": shelters_out,
        "fieldIntakesByCity": found_cities_out,
        "byLocation": locations_out,
        "statusDetail": status_detail_out,
        "transferDestinations": destinations_out,
        "buckets": buckets,
        "medicalMetrics": medical_metrics,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/flood_animals.json", "w") as f:
        json.dump(output, f, indent=2)

    with open("data/animal_profiles.json", "w") as f:
        json.dump({
            "lastUpdated": output["lastUpdated"],
            "buckets": PROFILE_BUCKETS,
            "animals": animal_profiles_out,
        }, f, indent=2)

    profiled = sum(1 for a in animal_profiles_out if a["hasProfile"])
    print(f"Done. Total flood-attributable animals: {totals['total']} ({profiled} with profile detail)")


if __name__ == "__main__":
    run()
