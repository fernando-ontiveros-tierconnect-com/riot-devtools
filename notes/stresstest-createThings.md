- command:

ab -n 200000 -c 2 -P post.txt -T 'application/x-www-form-urlencoded' http://192.168.0.197:9000/api/things/create


- results

Server Software:
Server Hostname:        192.168.0.197
Server Port:            9000

Document Path:          /api/things/create
Document Length:        743 bytes

Concurrency Level:      2
Time taken for tests:   505.834 seconds
Complete requests:      200000
Failed requests:        171312
   (Connect: 0, Receive: 0, Length: 171312, Exceptions: 0)
Total transferred:      188682363 bytes
HTML transferred:       148885081 bytes
Requests per second:    395.39 [#/sec] (mean)
Time per request:       5.058 [ms] (mean)
Time per request:       2.529 [ms] (mean, across all concurrent requests)
Transfer rate:          364.27 [Kbytes/sec] received
