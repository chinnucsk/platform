
-define(SYSLOG_MSG(Severity, Msg), 
	esyslog:syslog(Severity, ?MODULE, ?LINE, "~s", [Msg])).

-define(SYSLOG(Severity, Format, Args), 
	esyslog:syslog(Severity, ?MODULE, ?LINE, Format, Args)).


%%TODO: need it?
-define(SYSLOG_CRIT(Format, Args), 
	esyslog:crit(?MODULE, ?LINE, Format, Args)).

-define(SYSLOG_ERR(Format, Args), 
	esyslog:error(?MODULE, ?LINE, Format, Args)).

-define(SYSLOG_WARN(Format, Args), 
	esyslog:warn(?MODULE, ?LINE, Format, Args)).

-define(SYSLOG_INFO(Format, Args), 
	esyslog:info(?MODULE, ?LINE, Format, Args)).

-define(SYSLOG_DEBUG(Format, Args), 
	esyslog:debug(?MODULE, ?LINE, Format, Args)).

