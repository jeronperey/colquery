#!/usr/bin/env bash
# End-to-end checks:
#  1. generated CQF vs. CSV import produce byte-identical query results
#  2. the "raw CSV size" we report equals the real CSV file size
#  3. vectorized (with/without zone maps) and row-at-a-time match an independent
#     brute-force reference on 25 queries, including empty results and absent
#     dictionary literals
set -euo pipefail
cd "$(dirname "$0")/.."
D=build_data
mkdir -p $D

ROWS=200003   # not a multiple of the block size -> exercises the partial tail block
./colquery gen $ROWS $D/t.cq --csv $D/t.csv
./colquery import $D/t.csv $D/t_import.cq

real=$(stat -f%z $D/t.csv 2>/dev/null || stat -c%s $D/t.csv)
reported=$(./colquery info $D/t.cq | sed -n 's/.*raw CSV equivalent: \([0-9.]*\) MB.*/\1/p')
python3 - "$real" "$reported" <<'EOF'
import sys
real, rep = int(sys.argv[1]), float(sys.argv[2])
assert abs(real / 1e6 - rep) < 0.06, f"CSV size mismatch: real {real} vs reported {rep} MB"
print(f"csv size check ok ({real} bytes)")
EOF

Q="SELECT country, COUNT(*), SUM(amount), AVG(amount) FROM t WHERE status != 2 AND amount > 100 GROUP BY country"
diff <(./colquery query $D/t.cq "$Q" | grep -v '^--') <(./colquery query $D/t_import.cq "$Q" | grep -v '^--')
echo "gen vs csv-import results identical"

./colquery verify $D/t.cq
./colquery verify $D/t_import.cq
echo "ALL TESTS PASSED"
