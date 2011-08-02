all:
	mkdir -p elog/ebin elog/doc
	(cd elog;$(MAKE))
	mkdir -p core/ebin core/doc
	(cd core;$(MAKE))
	mkdir -p amqp/ebin amqp/doc
	(cd amqp;$(MAKE))
	mkdir -p cassandra/ebin cassandra/doc
	(cd cassandra;$(MAKE))
	mkdir -p mochiweb/ebin mochiweb/doc
	(cd mochiweb;$(MAKE))
	mkdir -p errdb/ebin errdb/doc
	(cd errdb;$(MAKE))
	#mkdir -p exmpp/ebin exmpp/doc
	#(cd exmpp;$(MAKE))
	#mkdir -p emongo/ebin emongo/doc
	#(cd emongo;$(MAKE))
	mkdir -p iconv/ebin iconv/doc
	(cd iconv;$(MAKE))
	mkdir -p mysql/ebin mysql/doc
	(cd mysql;$(MAKE))
	mkdir -p sesnmp/ebin sesnmp/doc
	(cd sesnmp;$(MAKE))
	mkdir -p telnet/ebin telnet/doc
	(cd telnet;$(MAKE))

clean:
	(cd amqp;$(MAKE) clean)
	(cd cassandra;$(MAKE) clean)
	(cd core;$(MAKE) clean)
	(cd elog;$(MAKE) clean)
	(cd errdb;$(MAKE) clean)
	#(cd exmpp;$(MAKE) clean)
	(cd iconv;$(MAKE) clean)
	(cd mochiweb;$(MAKE) clean)
	(cd mysql;$(MAKE) clean)
	(cd sesnmp;$(MAKE) clean)

