from datacenter import *
import sys
import time

id_ = int(sys.argv[1])
s1 = Server(id_)
# s2 = Server(2)
# s3 = Server(3)
s1.run()
# s2.run()
# s3.run()
