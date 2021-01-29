sqlite3 representations/6/representations/bielefeld/0.db "SELECT string FROM representations WHERE id=86" | while read line; do python parser_test.py "${line}"; wait; done
