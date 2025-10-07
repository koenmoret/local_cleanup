<?php
// local/cleanup/delete_duplicate_unused.php
// Verwijdert ongebruikte dubbele vragen per inhouds-hash (houdt 1 "keeper" over).
// Werkt met Moodle 4.x (gebruik-detectie via question_references).
define('CLI_SCRIPT', true);

require(__DIR__ . '/../../config.php');
require_once($CFG->libdir . '/clilib.php');
require_once($CFG->libdir . '/questionlib.php');

// Zet tijdelijk aan voor debugging (maakt traag en veel output):
// $DB->set_debug(true);

// Args:
//  --contextlevel (10 = system, 50 = course) [required]
//  --instanceid   (courseid indien contextlevel=50)
//  --batch        (aantal deletes per run; default 2000)
//  --dryrun       (1 = alleen rapporteren; 0 = echt verwijderen)
//  --limit_hashes (max # hashes per run)
//  --category     (optioneel: beperk tot specifieke category-id in deze context)
list($options, $unrecognized) = cli_get_params([
    'contextlevel' => null,
    'instanceid'   => 0,
    'batch'        => 2000,
    'dryrun'       => 1,
    'limit_hashes' => 200,
    'category'     => 0,
    'help'         => false,
], [
    'c' => 'contextlevel',
    'i' => 'instanceid',
    'b' => 'batch',
    'n' => 'dryrun',
    'l' => 'limit_hashes',
    'h' => 'help',
]);

if (!empty($options['help']) || !$options['contextlevel'] || !in_array((int)$options['contextlevel'], [10, 50])) {
    $help = "Delete unused duplicate questions by content-hash (keep 1 per hash).

Options:
  -c, --contextlevel   10 = system, 50 = course   (required)
  -i, --instanceid     course id (required if contextlevel=50)
  -b, --batch          max questions to delete per run (default 2000)
  -n, --dryrun         1 = no delete, just report (default 1)
  -l, --limit_hashes   max number of duplicate hashes to process (default 200)
      --category       limit to specific question_category id (optional)
  -h, --help           show this help

Examples:
  php local/cleanup/delete_duplicate_unused.php --contextlevel=10 --batch=5000 --dryrun=1
  php local/cleanup/delete_duplicate_unused.php --contextlevel=10 --batch=5000 --dryrun=0
  php local/cleanup/delete_duplicate_unused.php --contextlevel=10 --category=28 --batch=5000 --dryrun=0
  php local/cleanup/delete_duplicate_unused.php --contextlevel=50 --instanceid=716 --batch=2000 --dryrun=0
";
    cli_error($help);
}

$contextlevel = (int)$options['contextlevel'];
$instanceid   = (int)$options['instanceid'];
$batch        = max(1, (int)$options['batch']);
$dryrun       = (int)$options['dryrun'] ? 1 : 0;
$limitHashes  = max(1, (int)$options['limit_hashes']);
$onlycat      = (int)$options['category'];

if ($contextlevel == 50 && $instanceid <= 0) {
    cli_error("For contextlevel=50 (course) you must pass --instanceid=<courseid>.");
}

// Resolve context id.
if ($contextlevel == 10) {
    $ctxid = $DB->get_field('context', 'id', ['contextlevel' => 10, 'instanceid' => 0], IGNORE_MISSING);
} else {
    $ctxid = $DB->get_field('context', 'id', ['contextlevel' => 50, 'instanceid' => $instanceid], IGNORE_MISSING);
}
if (!$ctxid) {
    cli_error("Context not found.");
}

mtrace("Contextlevel=$contextlevel, instanceid=$instanceid, ctxid=$ctxid, dryrun=$dryrun, batch=$batch, limit_hashes=$limitHashes, category=" . ($onlycat ?: 'ALL'));

// ---------------------------------------------------------------------
// 1) Haal duplicate hashes in (optioneel) beperkte set (context + category)
//    Snelle hash: MD5(type|name|questiontext|generalfeedback) — voldoende voor exacte dubbels.
mtrace('Starting duplicate hash scan...');

$params = ['ctxid' => $ctxid];
$catfilter = '';
if ($onlycat > 0) {
    $catfilter = " AND qc.id = :onlycat ";
    $params['onlycat'] = $onlycat;
}

$sqlhashes = "
    WITH candidates AS (
        SELECT
          q.id,
          MD5(CONCAT_WS('|', q.qtype, q.name, q.questiontext, q.generalfeedback)) AS qhash
        FROM {question} q
        JOIN {question_versions} qv      ON qv.questionid = q.id
        JOIN {question_bank_entries} qbe ON qbe.id = qv.questionbankentryid
        JOIN {question_categories} qc    ON qc.id = qbe.questioncategoryid
        WHERE qc.contextid = :ctxid
          $catfilter
    )
    SELECT qhash
    FROM (
        SELECT qhash, COUNT(*) AS cnt
        FROM candidates
        GROUP BY qhash
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
    ) t
    LIMIT $limitHashes
";

$duplicatehashes = $DB->get_fieldset_sql($sqlhashes, $params);
mtrace('After duplicate hash scan. Found duplicate hashes: ' . count($duplicatehashes));
if (empty($duplicatehashes)) {
    mtrace("No duplicate hashes found in this scope.");
    exit(0);
}

// ---------------------------------------------------------------------
// 2) Verwerk per hash: kies keeper (laagste id) en verwijder ongebruikte dubbels
$deleted = 0;
$processedHashes = 0;

foreach ($duplicatehashes as $hash) {
    $processedHashes++;

    // 2a) Bepaal keeper (laagste id) binnen deze context (+ optioneel category) en hash
    $paramsK = ['ctxid' => $ctxid, 'qhash' => $hash];
    $sqlCatK = '';
    if ($onlycat > 0) {
        $sqlCatK = " AND qc.id = :onlycat ";
        $paramsK['onlycat'] = $onlycat;
    }

    $keeper = $DB->get_field_sql("
        SELECT MIN(q.id)
        FROM {question} q
        JOIN {question_versions} qv      ON qv.questionid = q.id
        JOIN {question_bank_entries} qbe ON qbe.id = qv.questionbankentryid
        JOIN {question_categories} qc    ON qc.id = qbe.questioncategoryid
        WHERE qc.contextid = :ctxid
          $sqlCatK
          AND MD5(CONCAT_WS('|', q.qtype, q.name, q.questiontext, q.generalfeedback)) = :qhash
    ", $paramsK, IGNORE_MISSING);

    if (!$keeper) {
        mtrace("Hash $hash: no keeper found (skipping).");
        continue;
    }

    // 2b) Haal direct alle ongebruikte duplicaten (excl. keeper) op, tot batchlimiet
    $left = max(0, $batch - $deleted);
    if ($left === 0) {
        break;
    }

    $paramsD = ['ctxid' => $ctxid, 'qhash' => $hash, 'keeper' => $keeper];
    $sqlCatD = '';
    if ($onlycat > 0) {
        $sqlCatD = " AND qc.id = :onlycat ";
        $paramsD['onlycat'] = $onlycat;
    }

    $todelete = $DB->get_fieldset_sql("
        SELECT q.id
        FROM {question} q
        JOIN {question_versions} qv      ON qv.questionid = q.id
        JOIN {question_bank_entries} qbe ON qbe.id = qv.questionbankentryid
        JOIN {question_categories} qc    ON qc.id = qbe.questioncategoryid
        LEFT JOIN {question_references} qr
               ON qr.questionbankentryid = qv.questionbankentryid
              AND qr.component = 'mod_quiz'
              AND qr.questionarea = 'slot'
        WHERE qc.contextid = :ctxid
          $sqlCatD
          AND MD5(CONCAT_WS('|', q.qtype, q.name, q.questiontext, q.generalfeedback)) = :qhash
          AND q.id <> :keeper
          AND qr.id IS NULL           -- alleen vragen die niet in quiz-slots gebruikt worden
        ORDER BY q.id
        LIMIT $left
    ", $paramsD);

    mtrace("Hash $hash: keeper=$keeper, unusedToDelete=" . count($todelete));

    if (empty($todelete)) {
        continue;
    }

    // 2c) Verwijderen (of tellen in dry-run)
    if ($dryrun) {
        $deleted += count($todelete);
    } else {
        // Start a short transaction and guarantee close via try/finally.
        $tx = $DB->start_delegated_transaction();
        $reachedlimit = false;

        try {
            foreach ($todelete as $qid) {
                // Verwijderen via API; kan exceptions gooien.
                question_delete_question($qid);
                $deleted++;

                if ($deleted % 200 === 0) {
                    mtrace("  deleted so far: $deleted");
                }

                if ($deleted >= $batch) {
                    $reachedlimit = true;
                    break; // breek UIT de foreach, maar commit in finally
                }
            }

            // Als we hier komen zonder exception: commit.
            $tx->allow_commit();
        } catch (\Throwable $e) {
            // Rollback en gooi door voor logging/afbreken (zonder “active transaction” warning).
            $tx->rollback($e);
        }

        if ($reachedlimit) {
            // Buiten de transactie beslissen om de outer loop te stoppen.
            break;
        }
    }

}

// ---------------------------------------------------------------------
mtrace("Processed hashes: $processedHashes");
mtrace("Deleted (or would delete in dryrun): $deleted");
mtrace("Done.");
