#!/bin/sh

echo "You are about to commit" $(git diff --cached --name-only --diff-filter=ACM)
echo "to" $(git branch --show-current)
found=0; cut -d= -f2 .env | while read value; do ggrep -r --exclude-from=.gitignore -q -F "$value" * && { echo "Warning: Value '$value' found in file: $(ggrep -r --exclude-from=.gitignore -l -F "$value" *)"; found=1; }; done; exit $found

while : ; do
    read -p "Do you really want to do this? [y/n] " RESPONSE < /dev/tty
    case "${RESPONSE}" in
        [Yy]* ) exit 0; break;;
        [Nn]* ) exit 1;;
    esac
done