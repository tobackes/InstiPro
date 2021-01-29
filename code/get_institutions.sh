mapping=$1;
subset=$2;

folder=representations/${mapping}/representations/${subset}/;
infolder=representations/${mapping}/institutions/${subset}/;
outfile=representations/${mapping}/institutions/${subset}.db;

for file in $folder*; do
    filename="${file##*/}";
    filenum="${filename%.*}";
    echo $mapping $subset $filenum;
    python get_institutions.py $mapping $subset $filenum &
done

wait;

sqlite3 $outfile "DROP TABLE IF EXISTS representations";

first="";

for file in $infolder*; do
    echo $file;
    filename="${file##*/}";
    filenum="${filename%.*}";
    if [ -z $first ]; then
        first=ok;
        schema=`sqlite3 ${file} "select sql from sqlite_master where type='table' and name='representations'"`;
        sqlite3 $outfile "${schema}";
    fi;
    sqlite3 $outfile "ATTACH '${file}' AS temp_${filenum}; INSERT INTO main.representations SELECT * FROM temp_${filenum}.representations;";
done

while IFS= read -r line; do
    vals=(${line//|/ });
    column=${vals[1]};
    echo $column;
    sqlite3 $outfile "CREATE INDEX ${column}_index ON representations(${column})";
done <<< `sqlite3 $outfile "PRAGMA table_info(representations);"`;
