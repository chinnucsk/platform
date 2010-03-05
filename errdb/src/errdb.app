{application, errdb,
 [{description, "errdb"},
  {vsn, "1.0"},
  {modules, [
    errdb,
    errdb_app,
    errdb_sup,
    errdb_deps,
	errdb_fs,
    errdb_httpd
  ]},
  {registered, [errdb, errdb_httpd]},
  {mod, {errdb_app, []}},
  {env, []},
  {applications, [kernel, stdlib, crypto]}]}.
