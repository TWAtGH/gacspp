gacspp:
	g++ -O3 -march=native -std=c++17 -Wall -Wextra -pedantic $(wildcard *.cpp) -o gacspp.out -ldl -lpthread -lstdc++fs -lpq
sqlite3:
	gcc -O3 -march=native -DSQLITE_THREADSAFE=0 -DSQLITE_ENABLE_RTREE=1 -c sqlite3.c
.PHONY: gacspp
