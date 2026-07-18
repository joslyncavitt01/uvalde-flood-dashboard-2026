import json
import os
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT = "apa-grants-manager"

QUERY = f"""
SELECT
  animalInternalID,
  animalAID,
  name,
  species,
  intakeDate,
  originShelter,
  currentStatus,
  dispositionBucket,
  lastOutcomeType,
  transferredTo,
  currentLocationTier1,
  currentLocationTier2
FROM `{PROJECT}.shelterluv.flood_animals`
"""


def run():
    client = bigquery.Client(project=PROJECT)
    rows = list(client.query(QUERY).result())

    animals = []
    for r in rows:
        animals.append({
            "id": r.animalInternalID,
            "aid": r.animalAID,
            "name": r.name,
            "species": r.species,
            "intakeDate": str(r.intakeDate),
            "shelter": r.originShelter,
            "status": r.currentStatus,
            "bucket": r.dispositionBucket,
            "outcomeType": r.lastOutcomeType,
            "transferredTo": r.transferredTo,
            "property": r.currentLocationTier1,
            "area": r.currentLocationTier2,
        })

    buckets = ["On Property", "In Foster", "Safety Net Foster (Pending RTO)", "Adopted / Pending", "Transferred Out"]

    # Totals
    totals = {
        "total": len(animals),
        "dogs": sum(1 for a in animals if a["species"] == "Dog"),
        "cats": sum(1 for a in animals if a["species"] == "Cat"),
    }
    for b in buckets:
        totals[b] = sum(1 for a in animals if a["bucket"] == b)

    # By day
    by_day = {}
    for a in animals:
        d = a["intakeDate"]
        if d not in by_day:
            by_day[d] = {"date": d, "total": 0, **{b: 0 for b in buckets}}
        by_day[d]["total"] += 1
        by_day[d][a["bucket"]] += 1
    days_out = [by_day[d] for d in sorted(by_day.keys())]

    # By shelter
    by_shelter = {}
    for a in animals:
        s = a["shelter"]
        if s not in by_shelter:
            by_shelter[s] = {
                "shelter": s, "total": 0, "dogs": 0, "cats": 0,
                "firstIntake": a["intakeDate"], "lastIntake": a["intakeDate"],
                **{b: 0 for b in buckets},
            }
        rec = by_shelter[s]
        rec["total"] += 1
        rec["dogs"] += 1 if a["species"] == "Dog" else 0
        rec["cats"] += 1 if a["species"] == "Cat" else 0
        rec["firstIntake"] = min(rec["firstIntake"], a["intakeDate"])
        rec["lastIntake"] = max(rec["lastIntake"], a["intakeDate"])
        rec[a["bucket"]] += 1
    shelters_out = sorted(by_shelter.values(), key=lambda r: -r["total"])

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

    # Transfer destinations
    dest = {}
    for a in animals:
        if a["bucket"] == "Transferred Out" and a["transferredTo"]:
            dest[a["transferredTo"]] = dest.get(a["transferredTo"], 0) + 1
    destinations_out = sorted(
        [{"destination": k, "count": v} for k, v in dest.items()],
        key=lambda r: -r["count"],
    )

    output = {
        "lastUpdated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "totals": totals,
        "byDay": days_out,
        "byShelter": shelters_out,
        "byLocation": locations_out,
        "transferDestinations": destinations_out,
        "buckets": buckets,
    }

    os.makedirs("data", exist_ok=True)
    with open("data/flood_animals.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"Done. Total flood-attributable animals: {totals['total']}")


if __name__ == "__main__":
    run()
