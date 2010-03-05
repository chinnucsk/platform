%start status
-define(STATUS_SUCCESS, 0).

-define(STATUS_ERROR,   1).

-define(STATUS_USAGE,   2).

-define(STATUS_BADRPC,  3).

%rrdfile template
%b: start time
%s: step
%dss: data sources
%rras: round robin archives
-record(rrdfile_template, {b, s, dss, rras}).

%%data source
-record(ds, {name, dst, hb, min, max}).

%%round robin archive
-record(rra, {cf, steps, rows}).

%%rrdgraph

%rrdtool graph filename [-s|--start seconds] [-e|--end seconds]
%	[-x|--x-grid x-axis grid and label]
%	[-Y|--alt-y-grid]
%	[-y|--y-grid y-axis grid and label]
%	[-v|--vertical-label string] [-w|--width pixels]
%	[-h|--height pixels] [-o|--logarithmic]
%	[-u|--upper-limit value] [-z|--lazy]
%	[-l|--lower-limit value] [-r|--rigid]
%	[-g|--no-legend]
%	[-F|--force-rules-legend]
%	[-j|--only-graph]
%	[-n|--font FONTTAG:size:font]
%	[-m|--zoom factor]
%	[-A|--alt-autoscale]
%	[-M|--alt-autoscale-max]
%	[-R|--font-render-mode {normal,light,mono}]
%	[-B|--font-smoothing-threshold size]
%	[-E|--slope-mode]
%	[-N|--no-gridfit]
%	[-X|--units-exponent value]
%	[-L|--units-length value]
%	[-S|--step seconds]
%	[-f|--imginfo printfstr]
%	[-a|--imgformat PNG]
%	[-c|--color COLORTAG#rrggbb[aa]] [-t|--title string]
%	[-W|--watermark string]
%	[DEF:vname=rrd:ds-name:CF]
%	[CDEF:vname=rpn-expression]
%	[VDEF:vdefname=rpn-expression]
%	[PRINT:vdefname:format]
%	[GPRINT:vdefname:format]
%	[COMMENT:text]
%	[SHIFT:vname:offset]
%	[TICK:vname#rrggbb[aa][:[fraction][:legend]]]
%	[HRULE:value#rrggbb[aa][:legend]]
%	[VRULE:value#rrggbb[aa][:legend]]
%	[LINE[width]:vname[#rrggbb[aa][:[legend][:STACK]]]]
%	[AREA:vname[#rrggbb[aa][:[legend][:STACK]]]]
