
-define(SYSLOG_ERR(Format, Args), esyslog:error(?MODULE, ?LINE, Format, Args)).

-define(SYSLOG_WARN(Format, Args), esyslog:warn(?MODULE, ?LINE, Format, Args)).

-define(SYSLOG_INFO(Format, Args), esyslog:info(?MODULE, ?LINE, Format, Args)).

