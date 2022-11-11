# CSV file
# ----------------
# Provide a Jira issue export CSV file that must contain the following fields. The names must match exactly:
# "Resolved"
# "Custom field (Story Points)"
# "Issue Type"
# The column position is detected automatically.
# Look into pxc_jira.py for the JQL expression.
# The output is two files where X stands for the base name of the input file:
# X_converted.csv -> aggregated values (points, counts) per day; contains calculation of 28 day moving averages
# X_distribution.csv -> aggregated story point distribution by issue type
# X_biggies.csv -> list of issues with big points
#
# Jira direct access
# --------------------------
# Instead of a file name provide either "explore" or "product" as project aliases or a Jira project name
# plus the login credentials (see USAGE). The program will figure out by itself whether the parameter is a file.
# The end date is optional and defaults to today.
# The date range is limited to 555 days (a little more than 1 1/2 years). A longer interval does not give more insight.
import sys
import csv
from datetime import datetime, timedelta
import os
from pathlib import Path

from pxc_jira import PROJECTS, JIRA_URL, get_project_issues

USAGE = '<csv-file>|product|explore|<Jira project name> [<jira-user> <jira-pwd> [<end-date yyyy-mm-dd>]]'
RESOLVED_COL = 'date'
ISSUETYPE_COL = 'type'
POINTS_COL = 'points'
KEY_COL = 'key'
BOARD_COL = 'bdate'
POINTS_THRESHOLD = 5
MOVING_AVG_INTERVAL = 28


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
            POINTS_COL: find_column(row, 'Custom field (Story Points)'), KEY_COL: find_column(row, 'Issue key'),
            BOARD_COL: find_column(row, 'board enter date')}


# The time is rounded down to 0:0:0, we are only interested in the day.
def round_down_time(datet):
    return datet.replace(hour=0, minute=0, second=0, microsecond=0)


def get_issue_type(row, config):
    return row[config[ISSUETYPE_COL]].lower()


def get_issue_key(row, config):
    return row[config[KEY_COL]]


def get_points(row, config):
    return row[config[POINTS_COL]]


def day_key(d):
    return d.strftime('%d.%m.%Y')


# date of resolution
def get_resolved_date(row, config):
    return round_down_time(datetime.strptime(row[config[RESOLVED_COL]], '%d.%m.%y %H:%M'))


# date when issue entered the Kanban board
def get_board_enter_date(row, config):
    return round_down_time(datetime.strptime(row[config[BOARD_COL]], '%d.%m.%y %H:%M'))


# get the date of the first data row; row 0 is the heading
def get_start_date(rows, config):
    if len(rows) > 1:
        return get_resolved_date(rows[1], config)


# get the date from the last data row
def get_end_date(rows, config):
    if (len(rows)) > 1:
        return get_resolved_date(rows[-1], config)


# an empty structure for a single report_value entry
def new_day_values():
    empty_dict = {'count': 0, 'points': 0.0, 'board_days': [], 'days_per_point': []}
    return {'bug': empty_dict.copy(),
            'story': empty_dict.copy(),
            'task': empty_dict.copy()}


# process a single row and update report_values for the matching date
# report_values are the aggregated values for each day that has resolved issues
def process_row(row, report_values, config):
    issue_type = get_issue_type(row, config)
    res_date = get_resolved_date(row, config)
    key = day_key(res_date)

    values = report_values.setdefault(key, new_day_values())
    values[issue_type]['count'] += 1

    board_date = get_board_enter_date(row, config)
    board_time = res_date - board_date

    # board_days is a list of issues with their days spent on the Kanban board
    values[issue_type]['board_days'].append({'issue_key': get_issue_key(row, config), 'days': board_time.days + 1})

    p = get_points(row, config)
    if p != '' and p != 0:
        values[issue_type]['points'] += float(p)
        values[issue_type]['days_per_point'].append({'issue_key': get_issue_key(row, config),
                                                     'days': board_time.days + 1,
                                                     'points': p,
                                                     'dpt': round((board_time.days + 1) / p, 1)})


# create a dict with key = day ISO, value = counters and points of the day
def calc_report_values(rows, config):
    report_values = {}
    for i in range(1, len(rows)):
        process_row(rows[i], report_values, config)
    return report_values


# collect all issues whose points exceed the threshold
def find_big_points(rows, config):
    biggies = []
    for i in range(1, len(rows)):
        row = rows[i]
        p = get_points(row, config)
        if p > POINTS_THRESHOLD:
            biggies.append({'date': day_key(get_resolved_date(row, config)),
                            'type': get_issue_type(row, config),
                            'points': p,
                            'URL': get_issue_key(row, config)})
    return biggies


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


# run a date loop across the report's time interval and fill in the collected day values;
# dates that have no data default to 0 values
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
    windows = [[], [], [], [], [], []]  # one interval sliding window for each individual average value
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
        yield d + avg + calculate_pairwise_relations(avg)  # three sequences appended to form the row values


def new_dist_values(key):
    # bug, task, story counter for key value
    return {key: [0, 0, 0]}


# calculate the story points distribution
# The distribution has the points value on the horizontal axis and one associated value for each task type
def generate_distribution(report_values):
    dist = {}  # key = points counter, value = list of counters for each task type
    types = ['bug', 'task', 'story']
    for v in report_values:
        for i, t in enumerate(types):
            p = report_values[v][t]['points']
            if p > 0:
                dist.setdefault(p, [0, 0, 0])
                dist[p][i] += 1
    return dist


# The story days distribution separates by points. For each point value it produces (board_days, count) value pairs.
# This can be used to plot the board days on the x-axis and the counter on the y-axis.
# Only stories are considered as they have always been estimated with points. Bugs and Tasks have not.
def generate_days_distribution(report_values):
    result = {}  # key = point value, value = dict of k=board_days, v=count
    for v in report_values:
        for issue_type in ['story', 'task']:
            issue = report_values[v][issue_type]
            p = issue['points']
            if p > 0:
                # count the occurrence of each board day for the current point
                for d in issue['board_days']:
                    result.setdefault(p, {})
                    result[p].setdefault(d['days'], 0)
                    result[p][d['days']] += 1
    return result


def generate_board_days(report_values, issue_type):
    result = []  # a list of [date, days, issue_key] for each issue
    for v in report_values:
        bdays = report_values[v][issue_type]['board_days']
        for bd in bdays:
            result.append([v, bd['days'], bd['issue_key']])
    return result


def generate_days_per_point(report_values, issue_type):
    result = []  # a list of: [date, days per point, issue_key] for each issue
    for v in report_values:
        bdays = report_values[v][issue_type]['days_per_point']
        for bd in bdays:
            result.append([v, bd['dpt'],  bd['points'], bd['days'], bd['issue_key']])
    return result


def serialize_distribution(dist):
    for k in sorted(dist):
        yield [k] + dist[k]


def determine_source(source):
    p = Path(source)
    if p.is_file():
        print('using file {} as input'.format(source))
        return False, source
    if source in PROJECTS:
        print('identified project shortcut for "{}"'.format(source))
        return True, PROJECTS[source]
    else:
        print('using {} as project name for Jira'.format(source))
        return True, source


# read a CSV export file
def read_rows(filename):
    rows = []
    with open(filename) as csvfile:
        rd = csv.reader(csvfile, delimiter=',')
        for row in rd:
            rows.append(row)
    return rows


# the Jira direct rows will look as if they came from a Jira export CSV file
def jira_online(project):
    end_date = round_down_time(datetime.today())
    if len(sys.argv) < 4:
        print(USAGE)
        exit(1)
    arg_user = sys.argv[2]
    arg_pwd = sys.argv[3]
    if len(sys.argv) == 5:
        end_date = datetime.strptime(sys.argv[4], '%Y-%m-%d')
    print('getting issues for project "{}" with end date {} for user {}'.format(project, end_date, arg_user))
    print('ignore the warnings that come from the disabled certificate validation')
    return [['Issue key', 'Issue id', 'Issue Type', 'Custom field (Story Points)', 'Resolved',
             'board enter date']] + get_project_issues(project, arg_user, arg_pwd, end_date)


def run_calculations():
    source = sys.argv[1]
    is_project, mapped_project = determine_source(source)
    if is_project:
        rows = jira_online(mapped_project)
        # get rid of blanks in the project name
        basename = source.replace(' ', '_')
    else:
        rows = read_rows(source)
        basename = os.path.splitext(source)[0]

    if len(rows) > 1:
        config = column_configuration(rows[0])
        print('detected columns: ', config)

        out_stats = basename + '_statistics.csv'
        report_values = calc_report_values(rows, config)
        write_statistics(report_values, rows, config, out_stats)

        out_sd_distribution = basename + '_days_distribution.csv'
        sd_dist = generate_days_distribution(report_values)
        write_sd_distribution(sd_dist, out_sd_distribution)

        out_distribution = basename + '_distribution.csv'
        dist = generate_distribution(report_values)
        write_distribution(dist, out_distribution)

        out_biggies = basename + '_biggies.csv'
        biggies = find_big_points(rows, config)
        write_biggies(biggies, out_biggies)

        write_board_days(report_values, 'bug', basename)
        write_board_days(report_values, 'task', basename)
        write_board_days(report_values, 'story', basename)
        write_days_per_point(report_values, 'bug', basename)
        write_days_per_point(report_values, 'task', basename)
        write_days_per_point(report_values, 'story', basename)

    else:
        print('Input file has no data, exiting')
        exit(0)


def write_board_days(report_values, issue_type, basename):
    filename = basename + '_boarddays_' + issue_type + '.csv'
    with open(filename, "w", newline='') as outfile:
        print('writing ' + filename)
        wr = csv.writer(outfile)
        for r in generate_board_days(report_values, issue_type):
            wr.writerow(r)


def write_days_per_point(report_values, issue_type, basename):
    filename = basename + '_days_per_point_' + issue_type + '.csv'
    with open(filename, "w", newline='') as outfile:
        print('writing ' + filename)
        wr = csv.writer(outfile)
        for r in generate_days_per_point(report_values, issue_type):
            wr.writerow(r)


def write_statistics(report_values, rows, config, out_stats):
    with open(out_stats, "w", newline='') as outfile:
        print('writing ' + out_stats)
        wr = csv.writer(outfile)
        wr.writerow(csv_headline_conv())
        for d in generate_rows(report_values, get_start_date(rows, config), get_end_date(rows, config)):
            # print(d)
            wr.writerow(d)
    return report_values


def write_biggies(biggies, out_biggies):
    with open(out_biggies, "w") as outfile:
        print('writing ' + out_biggies)
        outfile.write('date,type,points,URL\n')
        for b in biggies:
            outfile.write('{},{},{},{}/browse/{}\n'.format(b['date'], b['type'], b['points'], JIRA_URL, b['URL']))


def write_distribution(dist, out_distribution):
    with open(out_distribution, "w", newline='') as outfile:
        print('writing ' + out_distribution)
        wr = csv.writer(outfile)
        wr.writerow(csv_headline_dist())
        for d in serialize_distribution(dist):
            # print(d)
            wr.writerow(d)


def write_sd_distribution(dist, out_distribution):
    print('writing' + out_distribution)
    with open(out_distribution, 'w') as outfile:
        for p in sorted(dist):
            outfile.write('\n{} points\n'.format(p))
            for day, count in dist[p].items():
                outfile.write('{},{}\n'.format(day, count))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print(USAGE)
        exit(1)
    run_calculations()
