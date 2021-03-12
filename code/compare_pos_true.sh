place=$1;
pos=output/relations_pos_${place}.db;
true=output/relations_true_${place}.db;

calc(){ awk "BEGIN { print "$*" }"; }

TP=`sqlite3 $pos 'ATTACH DATABASE '"'${true}'"' as true; SELECT COUNT(*) FROM (SELECT x||"--"||y FROM equivalent INTERSECT SELECT x||"--"||y FROM true.equivalent);'`;
P=`sqlite3 $pos 'SELECT COUNT(*) FROM equivalent;'`;
T=`sqlite3 $true 'SELECT COUNT(*) FROM equivalent;'`;

echo equivalent under equivalent $TP $P $T; echo PREC; calc 100.0*$TP/$P; echo REC; calc 100.0*$TP/$T; 

TP=`sqlite3 $pos 'ATTACH DATABASE '"'${true}'"' as true; SELECT COUNT(*) FROM (SELECT x||"--"||y FROM equivalent INTERSECT SELECT x||"--"||y FROM true.supersets);'`;
P=`sqlite3 $pos 'SELECT COUNT(*) FROM equivalent;'`;
T=`sqlite3 $true 'SELECT COUNT(*) FROM supersets;'`;

echo equivalent under superset $TP $P $T; echo PREC; calc 100.0*$TP/$P; echo REC; calc 100.0*$TP/$T; 

TP=`sqlite3 $pos 'ATTACH DATABASE '"'${true}'"' as true; SELECT COUNT(*) FROM (SELECT pair FROM (SELECT x||"--"||y as pair FROM supersets UNION SELECT x||"--"||y FROM equivalent) INTERSECT SELECT pair_ FROM (SELECT x||"--"||y as pair_ FROM true.supersets UNION SELECT x||"--"||y FROM true.equivalent));'`;
P=`sqlite3 $pos 'SELECT COUNT(*) FROM (SELECT x||"--"||y FROM supersets UNION SELECT x||"--"||y FROM equivalent);'`;
T=`sqlite3 $true 'SELECT COUNT(*) FROM (SELECT x||"--"||y FROM supersets UNION ALL SELECT x||"--"||y FROM equivalent);'`;

echo superset+equivalent under superset+equivalent $TP $P $T; echo PREC; calc 100.0*$TP/$P; echo REC; calc 100.0*$TP/$T; 

TP=`sqlite3 $pos 'ATTACH DATABASE '"'${true}'"' as true; SELECT COUNT(*) FROM (SELECT x||"--"||y FROM supersets INTERSECT SELECT x||"--"||y FROM true.equivalent);'`;
P=`sqlite3 $pos 'SELECT COUNT(*) FROM supersets;'`;
T=`sqlite3 $true 'SELECT COUNT(*) FROM equivalent;'`;

echo superset under equivalent $TP $P $T; echo PREC; calc 100.0*$TP/$P; echo REC; calc 100.0*$TP/$T; 

TP=`sqlite3 $pos 'ATTACH DATABASE '"'${true}'"' as true; SELECT COUNT(*) FROM (SELECT x||"--"||y FROM supersets INTERSECT SELECT x||"--"||y FROM true.supersets);'`;
P=`sqlite3 $pos 'SELECT COUNT(*) FROM supersets;'`;
T=`sqlite3 $true 'SELECT COUNT(*) FROM supersets;'`;

echo superset under superset $TP $P $T; echo PREC; calc 100.0*$TP/$P; echo REC; calc 100.0*$TP/$T; 
