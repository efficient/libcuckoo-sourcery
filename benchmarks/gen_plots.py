from pylab import *
import json

# Constants

JSON_FILE = 'json_data.json'
data = json.loads(open(JSON_FILE).read())

KEYS = ['integer', 'string']
OPS = ['reads', 'inserts', 'mixed', 'memory']
TABLES = ['libcuckoo', 'tbb', 'stl']
LABELS = {
    'inserts': {'x': "Table size (power of 2)",
                'y': "Throughput\n(millions of reqs per sec)"},
    'reads': {'x': "Table size (power of 2)",
              'y': "Throughput\n(millions of reqs per sec)"},
    'mixed': {'x': "Percentage insert",
              'y': "Throughput\n(millions of reqs per sec)"},
    'memory': {'x': "Table size (power of 2)",
               'y': "Memory used (mb)"}
}

PLOT_HEIGHT = 3.5
PLOT_WIDTH = 3
DPI = 80
FONTSIZE = 11

def gen_plot(optype, numatype='two_numa'):
    """Using the data from JSON_FILE, gen_plot will return a matplotlib
    figure with a plot for the specified optype ('reads', 'inserts',
    'mixed', or 'memory') and numatype ('two_numa' or 'one_numa')

    """
    fig, axes = subplots(1, len(KEYS))
    axes[0].set_ylabel(LABELS[optype]['y'], fontsize=FONTSIZE)
    fig.set_figheight(PLOT_HEIGHT)
    fig.set_figwidth(len(KEYS) * PLOT_WIDTH + (PLOT_WIDTH + 1))
    fig.set_dpi(DPI)
    fig.subplots_adjust(bottom=0.2)
    
    lines = []

    for i in range(len(KEYS)):
        x = data[KEYS[i]][optype]['x']
        d = data[KEYS[i]][optype][numatype]

        axes[i].set_xlim((min(x)-1, max(x)+1))
        axes[i].set_xticks(x)
        axes[i].set_xlabel(LABELS[optype]['x'], fontsize=FONTSIZE)
        axes[i].tick_params(axis='both', which='major', labelsize=FONTSIZE)
        axes[i].tick_params(axis='both', which='minor', labelsize=FONTSIZE)
        axes[i].set_title((KEYS[i] + " " + optype).capitalize(), fontdict={'fontsize': FONTSIZE})

        maxes = []

        for t in TABLES:
            lines.append(tuple(axes[i].plot(x, d[t], label=t, marker='o', linestyle='-')))
            maxes.append(max(d[t]))

        axes[i].set_ylim(0, max(maxes)+10)

    fig.legend(lines[:len(TABLES)], TABLES, 'upper right', fontsize=FONTSIZE)

    return fig

for optype in OPS:
    print 'Generating ' + optype
    f = gen_plot(optype)
    filename = optype + '_plot.pdf'
    f.savefig(filename)
    print 'Wrote to ' + filename
