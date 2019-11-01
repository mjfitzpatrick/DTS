"#\n"
"#  DTS Test Deployment Configuration\n"
"#\n"
"#  Revision History\n"
"#\n"
"#      Created   5/13/11   MJF\n"
"\n"
"\n"
"verbose   0\n"
"debug     0\n"
"#monitor	  tucana.tuc.noao.edu:3002\n"
"#monitor	  tucana.tuc.noao.edu:3003		# Test system\n"
"\n"
"\n"
"########################################\n"
"# DTS system at CTIO\n"
"########################################\n"
"\n"
"dts\n"
"    name      dts-ct\n"
"    host      dtsct1.ctio.noao.edu\n"
"    port      3000\n"
"    loPort    3005\n"
"    hiPort    3100\n"
"    root      /decam/dts/DTS/\n"
"    contact   3001\n"
"    network   des\n"
"    dbfile    /decam/dts/DTS/msg.db\n"
"    logfile   /decam/dts/DTS/dts.log\n"
"\n"
"    # /////////////////////////////////////////////\n"
"    # Queue configurations\n"
"    # /////////////////////////////////////////////\n"
"    queue 				# entry point from domes\n"
"	name	      	des		# queue name\n"
"        node          	ingest		# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	#mode	        push		# push or give\n"
"	mode	        give		# push or give\n"
"	method        	dts		# dts or ????\n"
"	#nthreads      	30		# No. of outbound threads\n"
"	nthreads      	10		# No. of outbound threads\n"
"        port	      	3005		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        purge   	yes\n"
"        dest            dts-ls    	#\n"
"	deliveryDir   	spool		# spool or /path/to/copy\n"
"        deliveryCmd     /decam/dts/istbproc $obsepoch $caldate $FULL $OH $account $D\n"
"\n"
"\n"
"    queue 				# entry point from domes\n"
"	name	      	decam		# queue name\n"
"        node          	ingest		# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	#mode	        push		# push or give\n"
"	mode	        give		# push or give\n"
"	method        	dts		# dts or ????\n"
"	#nthreads      	30		# No. of outbound threads\n"
"	nthreads      	10		# No. of outbound threads\n"
"        port	      	3035		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        purge   	yes\n"
"        dest            dts-ls    	#\n"
"	deliveryDir   	spool		# spool or /path/to/copy\n"
"        deliveryCmd     /decam/dts/istbproc $obsepoch $caldate $FULL $OH $account $D\n"
"\n"
"    # Recovery queues\n"
"    queue 				# entry point from domes\n"
"	name	      	des_r		# queue name\n"
"        node          	ingest		# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	mode	        give		# push or give\n"
"	method        	dts		# dts or ????\n"
"	nthreads      	10		# No. of outbound threads\n"
"        port	      	3105		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        purge   	yes\n"
"        dest            dts-ls    	#\n"
"	deliveryDir   	spool		# spool or /path/to/copy\n"
"        deliveryCmd     /decam/dts/istbproc $obsepoch $caldate $FULL $OH $account $D\n"
"\n"
"\n"
"    queue 				# entry point from domes\n"
"	name	      	decam_r		# queue name\n"
"        node          	ingest		# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	mode	        give		# push or give\n"
"	method        	dts		# dts or ????\n"
"	nthreads      	10		# No. of outbound threads\n"
"        port	      	3135		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        purge   	yes\n"
"        dest            dts-ls    	#\n"
"	deliveryDir   	spool		# spool or /path/to/copy\n"
"        deliveryCmd     /decam/dts/istbproc $obsepoch $caldate $FULL $OH $account $D\n"
"\n"
"\n"
"\n"
"\n"
"########################################\n"
"# DTS system at La Serena\n"
"########################################\n"
"\n"
"dts\n"
"    name      dts-ls\n"
"    host      dsas3.ctio.noao.edu\n"
"    port      3000\n"
"    loPort    3005\n"
"    hiPort    3100\n"
"    root      /data2/dts/DTS/\n"
"    contact   3001\n"
"    network   des\n"
"    dbfile    /data2/dts/DTS/msg.db\n"
"    logfile   /data2/dts/DTS/dts.log\n"
"\n"
"    # /////////////////////////////////////////////\n"
"    # Queue configurations\n"
"    # /////////////////////////////////////////////\n"
"\n"
"    queue 				# entry point from domes\n"
"	name	      	des		# queue name\n"
"        node          	transfer	# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	mode	        give		# push or give\n"
"#	method        	dts		# dts or ????\n"
"#	nthreads      	30		# No. of outbound threads\n"
"	method        	udt		# dts or ????\n"
"	nthreads      	1		# No. of outbound threads\n"
"	udt_rate      	500		# No. of outbound threads\n"
"        port	      	3005		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        purge   	yes\n"
"        src             dts-ct\n"
"        dest            dts-ncsa    	# for route thru NCSA\n"
"        #dest            dts-tuc    	# for route thru Tucson\n"
"	deliveryDir   	/data2/archive/incoming/des\n"
"        deliveryCmd     /noaosw/zdts/bin/idciproc $D $MD5 $S\n"
"	#deliveryDir   	spool\n"
"        #deliveryCmd     dts.null\n"
"\n"
"    queue 				# entry point from domes\n"
"	name	      	decam		# queue name\n"
"        node          	transfer	# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	mode	        push		# push or give\n"
"#	method        	dts		# dts or ????\n"
"#	nthreads      	30		# No. of outbound threads\n"
"	method        	udt		# dts or ????\n"
"	nthreads      	1		# No. of outbound threads\n"
"	udt_rate      	500		# No. of outbound threads\n"
"        port	      	3035		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        purge   	yes\n"
"        src             dts-ct\n"
"        dest            dts-tuc    	#\n"
"	deliveryDir   	/data2/archive/incoming/decam\n"
"        deliveryCmd     /noaosw/zdts/bin/idciproc $D $MD5 $S\n"
"	#deliveryDir   	spool\n"
"        #deliveryCmd     dts.null\n"
"\n"
"\n"
"########################################\n"
"# DTS system at NCSA\n"
"########################################\n"
"\n"
"dts\n"
"    name      dts-ncsa\n"
"    #host      desar2.cosmology.illinois.edu\n"
"    host      141.142.160.4\n"
"    port      3000\n"
"    loPort    3005\n"
"    hiPort    3100\n"
"    root      /home/fitz/DTS/\n"
"    contact   3001\n"
"    network   des\n"
"    dbfile    /home/fitz/DTS/msg.db\n"
"    logfile   /home/fitz/DTS/dts.log\n"
"\n"
"    # /////////////////////////////////////////////\n"
"    # Queue configurations\n"
"    # /////////////////////////////////////////////\n"
"    queue 				# entry point from domes\n"
"	name	      	des		# queue name\n"
"        node          	transfer	# ingest, transfer, or endpoint\n"
"        #node          	endpoint	# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	mode	        push		# push or give\n"
"	#mode	        give		# push or give\n"
"#	method        	dts		# dts or ????\n"
"#	nthreads      	20		# No. of outbound threads\n"
"	method        	udt		# dts or ????\n"
"	nthreads      	1		# No. of outbound threads\n"
"	udt_rate      	500		# No. of outbound threads\n"
"        port	      	3005		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        src             dts-ls		# route thru NCSA\n"
"        dest            dts-tuc	# route thru NCSA\n"
"        #src             dts-tuc		# route thru Tucson\n"
"        deliveryDir     /data/newdata/des\n"
"        deliveryCmd     /home/databot/acceptfiles.sh $D -keyfile /home/databot/devel/fileutils/trunk/scripts/decam_src_keywords.txt -detector DECam\n"
"\n"
"\n"
"\n"
"\n"
"########################################\n"
"# DTS system at Tucson\n"
"########################################\n"
"\n"
"dts\n"
"    name      dts-tuc\n"
"    host      dsan3.tuc.noao.edu\n"
"    port      3000\n"
"    loPort    3005\n"
"    hiPort    3100\n"
"    root      /sdm/dts1/dts/DTS\n"
"    contact   3001\n"
"    network   des\n"
"    dbfile    /sdm/dts1/dts/DTS/msg.db\n"
"    logfile   /sdm/dts1/dts/DTS/dts.log\n"
"\n"
"    # /////////////////////////////////////////////\n"
"    # Queue configurations\n"
"    # /////////////////////////////////////////////\n"
"    queue 				# entry point from domes\n"
"	name	      	des		# queue name\n"
"        node          	endpoint	# ingest, transfer, or endpoint\n"
"        #node          	transfer	# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	mode	        push		# push or give\n"
"	#mode	        give		# push or give\n"
"#	method        	dts		# dts or ????\n"
"#	nthreads      	30		# No. of outbound threads\n"
"	method        	udt		# dts or ????\n"
"	nthreads      	1		# No. of outbound threads\n"
"	udt_rate      	500		# No. of outbound threads\n"
"        port	      	3005		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        #src             dts-ls    	# route thru Tucson\n"
"        #dest            dts-ncsa    	# route thru Tucson\n"
"        src             dts-ncsa    	# route thru NCSA\n"
"	deliveryDir   	/data2/archive/incoming/des\n"
"	# deliveryCmd\n"
"\n"
"    queue 				# entry point from domes\n"
"	name	      	decam		# queue name\n"
"        node          	endpoint	# ingest, transfer, or endpoint\n"
"        type          	normal		# normal, scheduled, priority\n"
"	mode	        push		# push or give\n"
"#	method        	dts		# dts or ????\n"
"#	nthreads      	30		# No. of outbound threads\n"
"	method        	udt		# dts or ????\n"
"	nthreads      	1		# No. of outbound threads\n"
"	udt_rate      	500		# No. of outbound threads\n"
"        port	      	3035		# base transfer port\n"
"        keepalive	yes		# keep connection open?		 [NYI]\n"
"        src             dts-ls\n"
"	deliveryDir   	/data2/archive/incoming/decam\n"
"	# deliveryCmd\n"
"\n"
"\n"
"\n"
"\n"
"\n"
"# -------------------------------------------------------------------------\n"
"#\n"
"#  Sample DTS (daemon) configuration file.\n"
"#\n"
"#\n"
"#  The configuration file is made up of one or more entries with the\n"
"#  following structure:\n"
"#\n"
"#     <global values>\n"
"#\n"
"#     dts			# define a new DTS instance\n"
"#       <dts parameters>	#\n"
"#	queue			# define a queue on this DTS\n"
"#         <queue parameters>	#\n"
"#\n"
"#  This example defines a DTS daemon running on machine denali.tuc.noao.edu\n"
"#  at command port 3000.  This DTS is operating two queues: 'denali', which\n"
"#  is used to ingest data into the DTS system (i.e. it is a target point \n"
"#  for the DTSQ application;  it also runs a 'test.push' queue that is an\n"
"#  intermediate stage in a longer transport route (i.e. data coming into \n"
"#  this queue normally come from another DTS system and not a user app).\n"
"#\n"
"#\n"
"#  DESCRIPTION:\n"
"#  ===========\n"
"#\n"
"#  An explanation of the fields is as follows:\n"
"#\n"
"#    verbose	  Run the daemon in verbose mode.  When set as a global param-\n"
"#		  eter this affects all DTS instances defined in the file.\n"
"#\n"
"#    debug	  Run the daemon in debug mode.  When set as a global param-\n"
"#		  eter this affects all DTS instances defined in the file.\n"
"#\n"
"#    monitor	  Location of DTS monitor in the form  <fqdn_node> ':' <port>\n"
"#\n"
"#    DTS Parameters:\n"
"#\n"
"#    name	  Name of this DTS instance                               [REQ]\n"
"#    host	  Host machine on which it is running (FQDN or IP addr)   [REQ]\n"
"#    port	  RPC command port to be used                             [REQ]\n"
"#    root	  Root directory of the DTS sandboxed filesystem.         [REQ]\n"
"#    contact	  Contact address (to awaken daemon)\n"
"#    network	  Named \"network\" the node belongs to\n"
"#    logfile	  Local message logfile (not yet implemented).\n"
"#    dbfile	  Local transfer database\n"
"#\n"
"#    Queue Parameters:\n"
"#\n"
"#    name	  Name of queue.\n"
"#    node	  Node type of queue.  May be one of 'ingest', 'transfer'\n"
"#		  or 'endpoint'.\n"
"#    type	  Type of queue.  May be one of 'normal', 'scheduled',\n"
"#		  or 'priority'.\n"
"#    mode         Transport mode.  Defines the direction of transport, a\n"
"#		  'push' means data are moved out of this DTS, a 'pull' \n"
"# 		  means the operates by pulling data from a remote DTS.\n"
"#    method	  Transport method (NYI).  The only supported value at the\n"
"#		  moment is 'dts', meaning to use internal DTS tranport\n"
"#		  methods (currently parallel sockets).  Later versions will\n"
"#		  allow for alternate bulk transport via scp, fdt, rsync, etc.\n"
"#    nthreads     No. of transport threads.  This parameter defines the \n"
"#		  number of parallel sockets to run for transport.\n"
"#    port         Base transfer port number.\n"
"#    keepalive    Keep transport connections open?    [NOT YET IMPLEMENTED]\n"
"#    src	  Machine which sends data to this queue\n"
"#    dest	  Machine to which this queue sends data\n"
"#    deliveryDir  Spool or /path/to/delivery/copy\n"
"#    deliveryCmd  Delivery application to be run.\n"
"#\n"
"#\n"
"#  DISCUSSION:\n"
"#  ==========\n"
"#\n"
"# 	TBD\n"
"#\n"
"\n"
"\n"
"\n"
