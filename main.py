# The program takes a Jira issue export CSV file that must contain the following fields. The names must match exactly:
# - Resolved
# - Custom field (Story Points)
# - Issue Type
# The column position is detected automatically.
# The output is two files where X stands for the base name of the input file:
# X_converted.csv -> aggregated values (points, counts) per day; contains calculation of 28 day moving averages
# X_distribution.csv -> aggregated story point distribution by issue type

import sys
import csv
from datetime import datetime, date, timedelta, timezone
import os
from pxc_jira import PRODUCT, EXPLORE, get_project_issues

RESOLVED_COL = 'date'
ISSUETYPE_COL = 'type'
POINTS_COL = 'points'
MOVING_AVG_INTERVAL = 28
USAGE = '<csv-file>|product|explore [<jira-user> <jira-pwd> <start-date yyyy-mm-dd>]'
PROJECTS = {'product': PRODUCT, 'explore': EXPLORE}


def daterange(date_from, date_to):
    for n in range(int((date_to - date_from).days) + 1):
        yield date_from + timedelta(n)


def find_column(row, text):
    try:
        return row.index(text)
    except ValueError:
        print('Could not find column "{}", exiting'.format(text))
        exit(1)


# Detect the required columns and produce a dict with the column indexes
# This is a safety measure against any column order change or name change in the Jira report
def column_configuration(row):
    return {RESOLVED_COL: find_column(row, 'Resolved'), ISSUETYPE_COL: find_column(row, 'Issue Type'),
            POINTS_COL: find_column(row, 'Custom field (Story Points)')}


def get_statechange_date(row, config):
    return datetime.strptime(row[config[RESOLVED_COL]], '%d.%m.%y %H:%M')


def get_issue_type(row, config):
    return row[config[ISSUETYPE_COL]].lower()


def get_points(row, config):
    return row[config[POINTS_COL]]


def day_key(d):
    return d.strftime('%d.%m.%Y')


def get_start_date(rows, config):
    if len(rows) > 1:
        return get_statechange_date(rows[1], config)


def get_end_date(rows, config):
    if (len(rows)) > 1:
        return get_statechange_date(rows[-1], config)


# an empty structure for a single report_value entry
def new_day_values():
    return {'bug': {'count': 0, 'points': 0.0},
            'story': {'count': 0, 'points': 0.0},
            'task': {'count': 0, 'points': 0.0}}


# process a single row and update report_values for the matching date
# report_values are the aggregated counters for each day that has resolved issues
def process_row(row, report_values, config):
    issue_type = get_issue_type(row, config)
    key = day_key(get_statechange_date(row, config))
    values = report_values.setdefault(key, new_day_values())
    values[issue_type]['count'] += 1
    p = get_points(row, config)
    if p != '':
        values[issue_type]['points'] += float(p)


# create a dict with key = day ISO, value = counters and points of the day
def process_rows(rows, config):
    report_values = {}
    for i in range(1, len(rows)):
        process_row(rows[i], report_values, config)
    return report_values


def csv_headline_conv():
    return ['date', 'bug_count', 'bug_points', 'task_count', 'task_points', 'story_count', 'story_points',
            'avg_bug_count', 'avg_bug_points', 'avg_task_count', 'avg_task_points', 'avg_story_count',
            'avg_story_points', 'bug p/c', 'task p/c', 'story p/c']


def csv_headline_dist():
    return ['points', 'bug', 'task', 'story']


# transform the dict of a day into a sequence of values
def serialize_day_values(val):
    return [val['bug']['count'], val['bug']['points'], val['task']['count'], val['task']['points'],
            val['story']['count'], val['story']['points']]


# run a date loop across the report's time interval and fill in the collected day values
def generate_timeseries(report_values, start_date, end_date):
    for d in daterange(start_date, end_date):
        key = day_key(d)
        yield [key] + serialize_day_values(report_values.get(key, new_day_values()))


# divide the second by the first value for each pair in the sequence; 0 if the denominator is 0
def calculate_pairwise_relations(pairs):
    return [0.0 if pairs[i - 1] == 0 else pairs[i] / pairs[i - 1] for i in range(1, int(len(pairs)), 2)]


# extend each day's values with moving averages and other stuff
# while generating a proper time series without holes
def generate_rows(report_values, start_date, end_date):
    rowcount = 1
    windows = [[], [], [], [], [], []]  # one window for each individual average value
    for d in generate_timeseries(report_values, start_date, end_date):
        # d is a sequence of values; the average's source values are from column 1 (=date) onwards
        avg = []
        for i in range(0, 6):
            windows[i].append(d[i + 1])  # extend the window by one day
            if rowcount >= MOVING_AVG_INTERVAL:
                # window has sufficient rows, can calculate the average
                avg.append(sum(windows[i]) / MOVING_AVG_INTERVAL)
                windows[i].pop(0)  # make room to shift the window by one day
            else:
                # window has still insufficient rows
                avg.append(0)
        rowcount += 1
        yield d + avg + calculate_pairwise_relations(avg)


def new_dist_values(key):
    # bug, task, story counter for key value
    return {key: [0, 0, 0]}


# calculate the story points distribution
def generate_distribution(report_values):
    dist = {}
    types = ['bug', 'task', 'story']
    for v in report_values:
        for i, t in enumerate(types):
            p = report_values[v][t]['points']
            if p > 0:
                dist.setdefault(p, [0, 0, 0])
                dist[p][i] += 1
                # print(dist[p])
    # print(dist)
    return dist


def serialize_distribution(dist):
    for k in sorted(dist):
        yield [k] + dist[k]


# read the input file into a sequence
def read_rows(filename):
    rows = []
    with open(filename) as csvfile:
        rd = csv.reader(csvfile, delimiter=',')
        for row in rd:
            rows.append(row)
    return rows


def aqcuire_rows(source):
    if source in PROJECTS:
        if len(sys.argv) < 5:
            print(USAGE)
            exit(1)
        print('getting issues for project "{}" from {} for user {}'.format(PROJECTS[source], sys.argv[2], sys.argv[3]))
        rows = get_project_issues(PROJECTS[source], sys.argv[2], sys.argv[3], sys.argv[4])
    else:
        print('using file {} as input'.format(source))
        rows = read_rows(source)
    return rows


def run_calculations():
    source = sys.argv[1]
    rows = aqcuire_rows(source)
    basename = source if source == EXPLORE or source == PRODUCT else os.path.splitext(source)[0]
    out_converted = basename + '_converted.csv'
    out_distribution = basename + "_distribution.csv"
    if len(rows) > 1:
        config = column_configuration(rows[0])
        print('detected columns: ', config)
        report_values = process_rows(rows, config)
        dist = generate_distribution(report_values)

        with open(out_distribution, "w", newline='') as outfile:
            print('writing ' + out_distribution)
            wr = csv.writer(outfile)
            wr.writerow(csv_headline_dist())
            for d in serialize_distribution(dist):
                # print(d)
                wr.writerow(d)

        with open(out_converted, "w", newline='') as outfile:
            print('writing ' + out_converted)
            wr = csv.writer(outfile)
            wr.writerow(csv_headline_conv())
            for d in generate_rows(report_values, get_start_date(rows, config), get_end_date(rows, config)):
                # print(d)
                wr.writerow(d)
    else:
        print('Input file has no data, exiting')
        exit(0)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(USAGE)
        exit(1)
    run_calculations()
