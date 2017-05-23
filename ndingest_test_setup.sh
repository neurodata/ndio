pwd
if [ -d /tmp/ndiotest01/ ]; then
  rm -r /tmp/ndiotest01/
  echo "wow"
fi
if [ -d /tmp/ndiotest02/ ]; then
  rm -r /tmp/ndiotest02/
  echo "wow"

fi
if [ -d /tmp/ndiotest03/ ]; then
  rm -r /tmp/ndiotest03/
  echo "wow"

fi
cp -r ./tests/data/test_ingest_pics/. /tmp/

sudo python ./add_to_sites_enabled.py
##
