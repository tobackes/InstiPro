sqlite3 representations/6/representations/bielefeld/0.db "SELECT string FROM representations WHERE id=192" | while read line; do python code/parser_test.py "${line}"; wait; done
