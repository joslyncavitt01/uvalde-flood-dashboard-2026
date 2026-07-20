// Paste this as a NEW FILE in your existing "APA Austin ShelterLuv Sync" Apps Script project
// (the same project that already has completed_surgeries.gs / physical_exams.gs).
//
// Watches Gmail for the 13 "Hourly Flood Update <hour>" emails you're scheduling in ShelterLuv
// (8am-8pm), downloads each report via its signed link, and appends every row into
// apa-data-410213.shelterluv.TempFloodJoslyn with an ingestion timestamp. Re-running throughout
// the day is expected to produce multiple rows per animal (one per hourly snapshot) -- that's
// intentional, "latest per animal" is resolved downstream in BigQuery, not here.

var BQ_PROJECT = "apa-data-410213";
var BQ_DATASET = "shelterluv";
var BQ_TABLE   = "TempFloodJoslyn";

var COLUMN_MAP = {
  "animal id":                "AnimalID",
  "name":                     "AnimalName",
  "species":                  "Species",
  "current status":           "CurrentStatus",
  "current location":         "CurrentLocation",
  "location at intake":       "LocationAtIntake",
  "intake date":              "IntakeDate",
  "intake time":              "IntakeTime",
  "intake type":              "IntakeType",
  "intake subtype":           "IntakeSubtype",
  "intake transfer from":     "IntakeTransferFrom",
  "intake original source":   "IntakeOriginalSource",
  "intake from person name":  "IntakeFromPersonName",
  "intake from city":         "IntakeFromCity",
  "intake from county":       "IntakeFromCounty",
  "intake from zip":          "IntakeFromZip",
  "intake found city":        "IntakeFoundCity",
  "intake found county":      "IntakeFoundCounty",
  "outcome date":             "OutcomeDate",
  "outcome type":             "OutcomeType",
  "outcome subtype":          "OutcomeSubtype",
  "transfer to":              "TransferTo"
};

var PROCESSED_LABEL = "FloodSyncProcessed";

function syncIntakeSnapshot() {
  // Matches all 13+ emails regardless of which hour is in the subject
  // (Gmail's subject: operator does a substring/phrase match, not exact-equality).
  // Deliberately does NOT filter on is:unread -- relying on read/unread status is fragile
  // (a preview pane, another device, or anything else touching the inbox can mark a message
  // read before this ever runs, silently skipping it with no error). Instead, track what's
  // already been processed with a dedicated Gmail label applied after a successful load.
  var label = getOrCreateLabel_(PROCESSED_LABEL);
  var threads = GmailApp.search('from:shelterluv subject:"Hourly Flood Update" -label:' + PROCESSED_LABEL);
  if (threads.length === 0) {
    Logger.log("No new Hourly Flood Update emails found.");
    return;
  }
  Logger.log("Found " + threads.length + " thread(s).");
  var totalLoaded = 0;
  var ingestedAt = Utilities.formatDate(new Date(), "UTC", "yyyy-MM-dd'T'HH:mm:ss'Z'");

  threads.forEach(function(thread) {
    var threadLabels = thread.getLabels().map(function(l) { return l.getName(); });
    if (threadLabels.indexOf(PROCESSED_LABEL) !== -1) return;

    thread.getMessages().forEach(function(message) {
      Logger.log("Processing: " + message.getSubject());

      var url = extractDownloadUrl(message.getBody());
      if (!url) {
        Logger.log("  No download URL found in email body.");
        return;
      }
      Logger.log("  Download URL found, fetching report...");

      var rows = downloadAndParse(url, ingestedAt, message.getSubject());
      Logger.log("  Parsed " + rows.length + " rows.");
      if (rows.length > 0) {
        totalLoaded += loadToBigQuery(rows);
      }
    });
    thread.addLabel(label);
  });

  Logger.log("Done. " + totalLoaded + " total rows written to BigQuery.");
}

function getOrCreateLabel_(name) {
  var label = GmailApp.getUserLabelByName(name);
  return label || GmailApp.createLabel(name);
}

function extractDownloadUrl(body) {
  var match = body.match(/https:\/\/new\.shelterluv\.com\/signed\/automated-report\/[^\s"<]+/);
  return match ? match[0] : null;
}

function downloadAndParse(url, ingestedAt, subject) {
  var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
  if (response.getResponseCode() !== 200) {
    Logger.log("  Download failed: HTTP " + response.getResponseCode());
    return [];
  }
  var blob = response.getBlob()
    .setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    .setName("intake_snapshot_report.xlsx");
  return parseExcel(blob, ingestedAt, subject);
}

function parseExcel(blob, ingestedAt, subject) {
  var file = DriveApp.createFile(blob);
  try {
    var converted = Drive.Files.copy(
      { title: "tmp_intake_snapshot_import", mimeType: MimeType.GOOGLE_SHEETS },
      file.getId(),
      { convert: true }
    );
    var sheet = SpreadsheetApp.openById(converted.id).getActiveSheet();
    var data = sheet.getDataRange().getValues();
    DriveApp.getFileById(converted.id).setTrashed(true);
    file.setTrashed(true);
    if (data.length < 2) return [];
    return buildRows(data[0], data.slice(1), ingestedAt, subject);
  } catch (e) {
    file.setTrashed(true);
    Logger.log("  Excel parse error: " + e.message);
    return [];
  }
}

function buildRows(headers, dataRows, ingestedAt, subject) {
  var colIndex = {};
  headers.forEach(function(h, i) {
    var key = String(h).trim().toLowerCase();
    if (COLUMN_MAP[key]) colIndex[i] = COLUMN_MAP[key];
  });
  if (Object.keys(colIndex).length === 0) {
    Logger.log("  WARNING: no recognized columns. Headers: " + headers.slice(0, 8).join(", "));
    return [];
  }
  var rows = [];
  dataRows.forEach(function(raw) {
    var row = { IngestedAt: ingestedAt, SourceEmailSubject: subject };
    var hasData = false;
    Object.keys(colIndex).forEach(function(i) {
      var field = colIndex[i];
      var val = raw[i];
      if (field === "AnimalID") {
        row[field] = String(val || "").replace(/^APA-A-/, "").trim();
      } else if (field === "IntakeDate") {
        row[field] = parseDate(val);
      } else {
        row[field] = (val !== null && val !== undefined) ? String(val).trim() : null;
      }
      if (val) hasData = true;
    });
    if (hasData && row.AnimalID) rows.push(row);
  });
  return rows;
}

function parseDate(val) {
  if (!val) return null;
  if (val instanceof Date) {
    return Utilities.formatDate(val, "UTC", "yyyy-MM-dd");
  }
  var s = String(val).trim();
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  var d = new Date(s);
  if (!isNaN(d)) {
    return Utilities.formatDate(d, "UTC", "yyyy-MM-dd");
  }
  return s;
}

function loadToBigQuery(rows) {
  var chunkSize = 500;
  var loaded = 0;
  for (var i = 0; i < rows.length; i += chunkSize) {
    var chunk = rows.slice(i, i + chunkSize);
    var insertRows = chunk.map(function(row, j) {
      return {
        insertId: row.AnimalID + "|" + row.IngestedAt + "|" + (i + j),
        json: row
      };
    });
    var response = BigQuery.Tabledata.insertAll(
      { rows: insertRows },
      BQ_PROJECT,
      BQ_DATASET,
      BQ_TABLE
    );
    if (response.insertErrors && response.insertErrors.length > 0) {
      Logger.log("  Insert errors: " + JSON.stringify(response.insertErrors[0]));
    } else {
      loaded += chunk.length;
    }
  }
  Logger.log("  Loaded " + loaded + " rows.");
  return loaded;
}

// Run this ONCE manually (select it in the function dropdown and click Run) to install
// a recurring trigger that checks for new emails every 15 minutes, 8am-8pm coverage.
// Safe to re-run -- it removes any previous trigger for this function first.
function setupTrigger_IntakeSnapshot() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === "syncIntakeSnapshot") {
      ScriptApp.deleteTrigger(t);
    }
  });
  ScriptApp.newTrigger("syncIntakeSnapshot")
    .timeBased()
    .everyMinutes(15)
    .create();
  Logger.log("Trigger installed: syncIntakeSnapshot every 15 minutes.");
}
