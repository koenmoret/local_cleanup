<?php
// local/cleanup/build_randomset_cats.php
// Bouwd een hulplijst met alle (sub)categorieën die door random-sets geraakt worden,
// zodat het cleanup-script die vragen kan overslaan.
//
// Werking:
// 1) Leest alle records uit mdl_question_set_references voor mod_quiz/slot.
// 2) Haalt uit filtercondition (JSON, of fallback PHP serialize) de category-id
//    en een vlag of subcategorieën mee moeten (includeSubcategories).
// 3) Expand naar alle subcategorieën (via mdl_question_categories.parent).
// 4) Schrijft unieke catid's naar mdl_randomset_cats_helper.
//
// Aanroep:
//   php /path/to/moodle/local/cleanup/build_randomset_cats.php
//
// Output voorbeeld:
//   randomset_cats_helper filled with 123 category ids.
//
// Vereist: Moodle-CLI (config.php), DB-toegang.

define('CLI_SCRIPT', true);

require(__DIR__ . '/../../config.php');
require_once($CFG->libdir . '/clilib.php');

// -- Helpers ------------------------------------------------------------------

/**
 * Decode filtercondition: probeer JSON, anders PHP-serialize.
 */
function cleanup_decode_filtercondition(string $txt) {
    // Probeer JSON
    $json = json_decode($txt, true);
    if (is_array($json)) {
        return $json;
    }
    // Probeer PHP serialize (sommige oudere sites gebruiken dat)
    $unser = @unserialize($txt);
    if ($unser !== false || $txt === 'b:0;') {
        return $unser;
    }
    return null;
}

/**
 * Zoek (mogelijk geneste) category-regels in de gedecodeerde structuur.
 * Accepteert diverse sleutelvarianten:
 *  - questioncategoryid / categoryid
 *  - includingsubcategories / includesubcategories / includeSubcategories / includesubcats
 */
function cleanup_find_category_rules($node, array &$acc): void {
    if (!is_array($node)) {
        return;
    }

    // Soms als "filtertype: category|question_category" met "filteroptions" of "options".
    if (isset($node['filtertype']) && in_array($node['filtertype'], ['category','question_category'], true)) {
        $opts = $node['filteroptions'] ?? ($node['options'] ?? $node);
        $cat  = $opts['questioncategoryid'] ?? ($opts['categoryid'] ?? ($opts['category'] ?? null));
        $inc  = $opts['includingsubcategories'] ??
                ($opts['includesubcategories'] ?? ($opts['includeSubcategories'] ?? ($opts['includesubcats'] ?? 0)));
        if (!empty($cat)) {
            $acc[] = [
                'catid' => (int)$cat,
                'inc'   => (int)$inc ? 1 : 0,
            ];
        }
    }

    // Soms rechtstreeks als platte structuur:
    if (isset($node['questioncategoryid']) || isset($node['categoryid']) || isset($node['category'])) {
        $cat  = $node['questioncategoryid'] ?? ($node['categoryid'] ?? ($node['category'] ?? null));
        $inc  = $node['includingsubcategories'] ??
                ($node['includesubcategories'] ?? ($node['includeSubcategories'] ?? ($node['includesubcats'] ?? 0)));
        if (!empty($cat)) {
            $acc[] = [
                'catid' => (int)$cat,
                'inc'   => (int)$inc ? 1 : 0,
            ];
        }
    }

    // Dieper zoeken
    foreach ($node as $v) {
        cleanup_find_category_rules($v, $acc);
    }
}

/**
 * Bouw een parent->children cache voor question_categories.
 * @return array<int, int[]> map: parentid => [childid, ...]
 */
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

/**
 * Expand met alle subcategorieën via DFS.
 * @param int   $root
 * @param array $childrenByParent
 * @return int[]
 */
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

// -- Start --------------------------------------------------------------------

mtrace("Building random-set category helper...");

// 1) Maak hulpetabel indien nodig (prefix-safe).
$prefix = $DB->get_prefix();
$createtable = "CREATE TABLE IF NOT EXISTS `{$prefix}randomset_cats_helper` (
  `catid` BIGINT(10) NOT NULL,
  PRIMARY KEY (`catid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
";
$DB->execute($createtable);

// 2) Leeg de hulpetabel.
$DB->execute("TRUNCATE `{$prefix}randomset_cats_helper`");

// 3) Cache van subcategorierelaties
$childrenByParent = cleanup_build_children_cache($DB);

// 4) Lees alle set-references (mod_quiz / slot)
$rs = $DB->get_recordset_select('question_set_references',
    "component = :c AND questionarea = :a",
    ['c' => 'mod_quiz', 'a' => 'slot'],
    '',
    'id, questionscontextid, filtercondition'
);

$collected = []; // set van catid => true

foreach ($rs as $row) {
    $cond = (string)$row->filtercondition;
    if ($cond === '' || $cond === null) {
        continue;
    }
    $decoded = cleanup_decode_filtercondition($cond);
    if ($decoded === null) {
        // Onbekend formaat; sla over maar noteer een hint als je wilt.
        // mtrace("  (warn) Unparseable filtercondition for id {$row->id}");
        continue;
    }

    $rules = [];
    cleanup_find_category_rules($decoded, $rules);

    if (!$rules) {
        // Geen categoriefilter gevonden in dit object.
        continue;
    }

    foreach ($rules as $r) {
        $cat = (int)$r['catid'];
        $inc = (int)$r['inc'];

        if ($cat <= 0) {
            continue;
        }

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

// 5) Schrijf unieke catids weg.
if (!empty($collected)) {
    // Batches invoegen voor efficiency
    $batch = [];
    $i = 0;
    foreach (array_keys($collected) as $cid) {
        $batch[] = (object)['catid' => (int)$cid];
        if (count($batch) >= 1000) {
            $DB->insert_records('randomset_cats_helper', $batch);
            $batch = [];
        }
        $i++;
    }
    if ($batch) {
        $DB->insert_records('randomset_cats_helper', $batch);
    }
    mtrace("randomset_cats_helper filled with {$i} category ids.");
} else {
    mtrace("randomset_cats_helper filled with 0 category ids (no random-set categories detected).");
}

mtrace("Done.");
