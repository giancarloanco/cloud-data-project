chmod +x daily_retail.sh

crontab -e

0 7 * * * /bin/bash daily_retail.sh
