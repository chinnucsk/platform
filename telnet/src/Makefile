include ../support/include.mk

all: $(EBIN_FILES)
	(cd plugins;$(MAKE))

debug:
	$(MAKE) DEBUG=-DDEBUG

clean:
	rm -rf $(EBIN_FILES)
