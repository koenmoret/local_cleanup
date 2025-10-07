#!/usr/bin/env php
<?php
/**
 * run_cleanup_pipeline.php
 *
 * Orkestreert:
 *  1) local/cleanup/build_randomset_cats.php
 *  2) local/cleanup/delete_duplicate_unused.php
 *  3) SQL checks via Moodle $DB (broken slots + counters)
 *
 * Plaats dit bestand in: <moodle>/local/cleanup/
 * Logs: <moodle>/local/cleanup/logs/cleanup-pipeline_YYYY-mm-dd_HHMMSS.log
 */

ini_set('display_errors', 'stderr');

/* ---------- CLI helpers ---------- */
function usage(): void {
    $u = <<<TXT
Usage:
  php run_cleanup_pipeline.php [--php=/usr/bin/php]
    [--context=10] [--instance=0] [--category=]
    [--limit=2000] [--batch=50000] [--dryrun=0]

Opties:
  --php       Pad naar PHP CLI (default /usr/bin/php)
  --context   10 = system, 50 = course
  --instance  Course id (verplicht als context=50), anders 0
  --category  Optioneel: specifieke question_category id
  --limit     limit_hashes (aantal duplicaat-groepen per run)
  --batch     batch (max # deletes per run)
  --dryrun    1 = rapporteren, 0 = echt verwijderen

Voorbeeld:
  php run_cleanup_pipeline.php --context=10 --limit=2000 --batch=50000 --dryrun=0
TXT;
    fwrite(STDERR, $u . PHP_EOL);
    exit(1);
}

function arg(string $name, $default = null) {
    global $argv;
    foreach ($argv as $a) {
        if (strpos($a, "--{$name}=") === 0) {
            return substr($a, strlen("--{$name}="));
        }
    }
    return $default;
}

/** Zoek Moodle-root door omhoog te lopen tot config.php wordt gevonden. */
function find_moodle_root(string $startDir): ?string {
    $dir = $startDir;
    while ($dir && $dir !== '/' && $dir !== '.' && $dir !== dirname($dir)) {
        if (is_file($dir . '/config.php')) {
            return $dir;
        }
        $dir = dirname($dir);
    }
    return null;
}

/* ---------- Parameters ---------- */
$phpBin   = arg('php', '/usr/bin/php');
$context  = (int) arg('context', 10);
$instance = (int) arg('instance', 0);
$category = arg('category', '');
$limit    = (int) arg('limit', 2000);
$batch    = (int) arg('batch', 50000);
$dryrun   = (int) arg('dryrun', 0);

if (!in_array($context, [10, 50], true)) usage();
if ($context === 50 && $instance <= 0) usage();

/* ---------- Moodle-root autodetect ---------- */
$cliDir    = __DIR__;                   // /.../moodle/local/cleanup
$moodleDir = find_moodle_root($cliDir); // verwacht /.../moodle
if (!$moodleDir || !is_file($moodleDir . '/config.php')) {
    fwrite(STDERR, "Kon Moodle-root niet autodetecteren vanaf {$cliDir} (config.php niet gevonden).\n");
    exit(1);
}

/* ---------- Moodle bootstrap (DB-API beschikbaar) ---------- */
define('CLI_SCRIPT', true);
require_once($moodleDir . '/config.php'); // $DB, $CFG etc.

/* ---------- Paden naar helper/cleanup ---------- */
$helper  = $moodleDir . '/local/cleanup/build_randomset_cats.php';
$cleanup = $moodleDir . '/local/cleanup/delete_duplicate_unused.php';
if (!is_file($helper) || !is_file($cleanup)) {
    fwrite(STDERR, "Helper of cleanup script niet gevonden in {$moodleDir}/local/cleanup.\n");
    exit(1);
}

/* ---------- Logging ---------- */
$logdir = $cliDir . '/logs';
if (!is_dir($logdir)) { @mkdir($logdir, 0775, true); }
$ts  = date('Y-m-d_His');
$log = "{$logdir}/cleanup-pipeline_{$ts}.log";

function logline(string $msg, string $log): void {
    $line = $msg . PHP_EOL;
    echo $line;
    file_put_contents($log, $line, FILE_APPEND);
}

/* ---------- Start ---------- */
logline("== Cleanup pipeline started @ {$ts} ==", $log);
logline("php={$phpBin}", $log);
logline("moodle={$moodleDir}", $log);
logline("context={$context} instance={$instance} category=" . ($category === '' ? 'ALL' : $category), $log);
logline("limit={$limit} batch={$batch} dryrun={$dryrun}", $log);
logline("log={$log}", $log);
logline("", $log);

/* ---------- Stap 1: helper ---------- */
logline(">> Step 1/3: Building random-set category helper...", $log);
$cmd1 = escapeshellarg($phpBin) . ' ' . escapeshellarg($helper);
logline("Command: {$cmd1}", $log);
$out1 = shell_exec($cmd1 . ' 2>&1');
logline(rtrim($out1 ?? ''), $log);
logline("", $log);

/* ---------- Stap 2: cleanup ---------- */
logline(">> Step 2/3: Running duplicate cleanup...", $log);
$args = [
    "--contextlevel={$context}",
    "--limit_hashes={$limit}",
    "--batch={$batch}",
    "--dryrun={$dryrun}",
];
if ($context === 50 && $instance > 0) { $args[] = "--instanceid={$instance}"; }
if ($category !== '' && $category !== null) { $args[] = "--category={$category}"; }

$cmd2 = escapeshellarg($phpBin) . ' ' . escapeshellarg($cleanup) . ' ' .
        implode(' ', array_map('escapeshellarg', $args));
logline("Command: {$cmd2}", $log);
$out2 = shell_exec($cmd2 . ' 2>&1');
logline(rtrim($out2 ?? ''), $log);
logline("", $log);

/* ---------- Stap 3: SQL checks via $DB ---------- */
logline(">> Step 3/3: Running SQL sanity checks via Moodle DB API ...", $log);

// A) Broken slots (verwachting: 0)
$brokenSql = "
SELECT COUNT(*) AS broken_slots
FROM {quiz_slots} s
JOIN {question_references} qr
  ON qr.component='mod_quiz' AND qr.questionarea='slot' AND qr.itemid=s.id
LEFT JOIN (
  SELECT questionbankentryid, MAX(version) AS maxv
  FROM {question_versions}
  GROUP BY questionbankentryid
) latest ON latest.questionbankentryid=qr.questionbankentryid
LEFT JOIN {question_versions} qv
  ON qv.questionbankentryid=qr.questionbankentryid AND qv.version=latest.maxv
LEFT JOIN {question} q ON q.id=qv.questionid
WHERE q.id IS NULL
";
try {
    $broken = (int)$DB->get_field_sql($brokenSql, []);
    logline("[SQL] Broken slots (expect 0): {$broken}", $log);
} catch (Throwable $e) {
    logline("[SQL] Broken slots query error: " . $e->getMessage(), $log);
}

// B) Counters (deletable_now / in_random_sets / truly_deletable_now)
// NB: gebruikt CTE; vereist MySQL 8+/MariaDB 10.2+. Gebruik anders een alternatieve query.
$countersSql = "
WITH base AS (
  SELECT q.id,
         qbe.questioncategoryid AS catid,
         MD5(CONCAT_WS('|', q.qtype, q.name, q.questiontext, q.generalfeedback)) AS qhash
  FROM {question} q
  JOIN {question_versions} qv      ON qv.questionid = q.id
  JOIN {question_bank_entries} qbe ON qbe.id = qv.questionbankentryid
  JOIN {question_categories} qc    ON qc.id = qbe.questioncategoryid
  JOIN {context} ctx               ON ctx.id = qc.contextid
  WHERE ctx.contextlevel = 10 AND ctx.instanceid = 0
),
grp AS (SELECT qhash FROM base GROUP BY qhash HAVING COUNT(*) > 1),
keepers AS (
  SELECT b.qhash, MIN(b.id) AS keeper_id
  FROM base b JOIN grp g ON g.qhash = b.qhash
  GROUP BY b.qhash
),
inuse_slots AS (
  SELECT DISTINCT v.questionid
  FROM {question_versions} v
  JOIN {question_references} r
    ON r.questionbankentryid=v.questionbankentryid
   AND r.component='mod_quiz' AND r.questionarea='slot'
),
deletable AS (
  SELECT b.id AS questionid, b.catid
  FROM base b
  JOIN grp g     ON g.qhash=b.qhash
  JOIN keepers k ON k.qhash=b.qhash
  LEFT JOIN inuse_slots u ON u.questionid=b.id
  WHERE b.id<>k.keeper_id AND u.questionid IS NULL
)
SELECT
  COUNT(*)                                                     AS deletable_now,
  SUM(CASE WHEN rch.catid IS NOT NULL THEN 1 ELSE 0 END)      AS in_random_sets,
  SUM(CASE WHEN rch.catid IS NULL  THEN 1 ELSE 0 END)         AS truly_deletable_now
FROM deletable d
LEFT JOIN {randomset_cats_helper} rch ON rch.catid = d.catid
";
try {
    $row = $DB->get_record_sql($countersSql, []);
    if ($row) {
        logline("[SQL] Counters: deletable_now={$row->deletable_now} | in_random_sets={$row->in_random_sets} | truly_deletable_now={$row->truly_deletable_now}", $log);
    } else {
        logline("[SQL] Counters: geen resultaat.", $log);
    }
} catch (Throwable $e) {
    logline("[SQL] Counters query error: " . $e->getMessage(), $log);
}

logline("== Cleanup pipeline finished. Log: {$log} ==", $log);
