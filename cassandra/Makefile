all: deps
	(cd src;$(MAKE))


deps:
	cp ../elog/ebin/* ebin
	cp ../elog/include/* include 
	cp ../core/ebin/* ebin

clean:
	(cd src;$(MAKE) clean)
