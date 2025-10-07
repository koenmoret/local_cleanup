<?php
// local/cleanup/build_randomset_cats.php
// Bouwt/verniewt hulplijst met alle (sub)categorieën die door random-sets geraakt worden.

define('CLI_SCRIPT', true);

require(__DIR__ . '/../../config.php');
require_once($CFG->libdir . '/clilib.php');
require_once($CFG->libdir . '/ddl/sql_generator.php');
require_once($CFG->libdir . '/ddllib.php');

mtrace("Building random-set category helper...");

$manager = $DB->get_manager(); // DDL manager

// 1) Maak de tabel via DDL-API als die nog niet bestaat.
$table = new xmldb_table('randomset_cats_helper');
$field_catid = new xmldb_field('catid', XMLDB_TYPE_INTEGER, '10', null, XMLDB_NOTNULL, null, null);

if (!$manager->table_exists($table)) {
    $table->addField($field_catid);
    $table->addKey(new xmldb_key('primary', XMLDB_KEY_PRIMARY, ['catid']));
    $manager->create_table($table);
    mtrace("Created table {randomset_cats_helper}.");
} else {
    // Zorg dat de kolom bestaat (voor het geval van oudere versie).
    if (!$manager->field_exists($table, $field_catid)) {
        $manager->add_field($table, $field_catid);
    }
}

// 2) Leegmaken via records-API (geen TRUNCATE).
$DB->delete_records('randomset_cats_helper');

// ------------------ helpers ------------------

function cleanup_decode_filtercondition(string $txt) {
    $json = json_decode($txt, true);
    if (is_array($json)) { return $json; }
    $unser = @unserialize($txt);
    if ($unser !== false || $txt === 'b:0;') { return $unser; }
    return null;
}

function cleanup_find_category_rules($node, array &$acc): void {
    if (!is_array($node)) { return; }

    if (isset($node['filtertype']) && in_array($node['filtertype'], ['category','question_category'], true)) {
        $opts = $node['filteroptions'] ?? ($node['options'] ?? $node);
        $cat  = $opts['questioncategoryid'] ?? ($opts['categoryid'] ?? ($opts['category'] ?? null));
        $inc  = $opts['includingsubcategories'] ??
                ($opts['includesubcategories'] ?? ($opts['includeSubcategories'] ?? ($opts['includesubcats'] ?? 0)));
        if (!empty($cat)) {
            $acc[] = ['catid' => (int)$cat, 'inc' => (int)$inc ? 1 : 0];
        }
    }

    if (isset($node['questioncategoryid']) || isset($node['categoryid']) || isset($node['category'])) {
        $cat  = $node['questioncategoryid'] ?? ($node['categoryid'] ?? ($node['category'] ?? null));
        $inc  = $node['includingsubcategories'] ??
                ($node['includesubcategories'] ?? ($node['includeSubcategories'] ?? ($node['includesubcats'] ?? 0)));
        if (!empty($cat)) {
            $acc[] = ['catid' => (int)$cat, 'inc' => (int)$inc ? 1 : 0];
        }
    }

    foreach ($node as $v) {
        cleanup_find_category_rules($v, $acc);
    }
}

function cleanup_build_children_cache(moodle_database $DB): array {
    $map = [];
    $rs = $DB->get_recordset_sql("SELECT id, parent FROM {question_categories}");
    foreach ($rs as $r) {
        $p = (int)$r->parent;
        $map[$p][] = (int)$r->id;
    }
    $rs->close();
    return $map;
}

function cleanup_expand_with_subcats(int $root, array $childrenByParent): array {
    $out = [$root];
    $stack = [$root];
    $seen = [$root => true];
    while ($stack) {
        $p = array_pop($stack);
        if (!empty($childrenByParent[$p])) {
            foreach ($childrenByParent[$p] as $kid) {
                if (!isset($seen[$kid])) {
                    $seen[$kid] = true;
                    $out[] = $kid;
                    $stack[] = $kid;
                }
            }
        }
    }
    return $out;
}

// ------------------ main ------------------

$childrenByParent = cleanup_build_children_cache($DB);

$rs = $DB->get_recordset_select('question_set_references',
    "component = :c AND questionarea = :a",
    ['c' => 'mod_quiz', 'a' => 'slot'],
    '',
    'id, questionscontextid, filtercondition'
);

$collected = [];

foreach ($rs as $row) {
    $cond = (string)$row->filtercondition;
    if ($cond === '' || $cond === null) { continue; }

    $decoded = cleanup_decode_filtercondition($cond);
    if ($decoded === null) { continue; }

    $rules = [];
    cleanup_find_category_rules($decoded, $rules);
    if (!$rules) { continue; }

    foreach ($rules as $r) {
        $cat = (int)$r['catid'];
        $inc = (int)$r['inc'];
        if ($cat <= 0) { continue; }

        if ($inc) {
            foreach (cleanup_expand_with_subcats($cat, $childrenByParent) as $cid) {
                $collected[$cid] = true;
            }
        } else {
            $collected[$cat] = true;
        }
    }
}
$rs->close();

$count = 0;
if (!empty($collected)) {
    $batch = [];
    foreach (array_keys($collected) as $cid) {
        $batch[] = (object)['catid' => (int)$cid];
        if (count($batch) >= 1000) {
            $DB->insert_records('randomset_cats_helper', $batch);
            $count += count($batch);
            $batch = [];
        }
    }
    if ($batch) {
        $DB->insert_records('randomset_cats_helper', $batch);
        $count += count($batch);
    }
}

mtrace("randomset_cats_helper filled with {$count} category ids.");
mtrace("Done.");
