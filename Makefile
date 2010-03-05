all:
	(cd elog;$(MAKE))
	(cd core;$(MAKE))
	(cd amqp;$(MAKE))
	(cd mochiweb;$(MAKE))

clean:
	(cd amqp;$(MAKE) clean)
	(cd core;$(MAKE) clean)
	(cd elog;$(MAKE) clean)
	(cd mochiweb;$(MAKE) clean)

