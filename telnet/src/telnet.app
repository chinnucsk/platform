{application, telnet,
 [{description, "telnet"},
  {vsn, "2.0"},
  {modules, [telnet_app, telnet_sup, telnet_conn]},
  {registered, [telnet]},
  {applications, [kernel, stdlib]},
  {env, []},
  {mod, {telnet_app, []}}]}.
