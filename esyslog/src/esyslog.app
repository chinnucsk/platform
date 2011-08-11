{application, esyslog,
 [{description, "esyslog"},
  {vsn, "0.1.0"},
  {modules, [
    esyslog,
    esyslog_app,
    esyslog_sup
  ]},
  {registered, [esyslog, esyslog_sup]},
  {mod, {esyslog_app, []}},
  {env, []},
  {applications, [kernel, stdlib]}]}.
