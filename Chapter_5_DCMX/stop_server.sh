for i in `ps -ef | grep "Chapter_5_DCMX" | awk '{print $2}'`
do
	kill -9 $i
done
for i in `ps -ef | grep python | grep server.py | awk '{print $2}'`
do
        kill -9 $i
done
