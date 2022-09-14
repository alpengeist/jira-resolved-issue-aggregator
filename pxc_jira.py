#
#  el cheapo Jira interface module custom for PxC
#
import sys

from jira import JIRA
from datetime import datetime, timedelta

EXPLORE = '[SCS] Explore'
PRODUCT = '[SCS] Product'
POINTS = 'customfield_10106'
MAX_DAYS = 365 + 365 / 2


def get_session(credentials):
    # silly PxC certificate cannot be validated
    return JIRA(options={'server': 'https://jira-web.europe.phoenixcontact.com', 'verify': False},
                basic_auth=credentials)


def calc_start_date(end_date):
    return end_date - timedelta(days=MAX_DAYS)


def format_query_date(date):
    return datetime.strftime(date, '%Y-%m-%d')


def format_data_date(date):
    return datetime.strftime(date, '%d.%m.%y %H:%M')


def jql_query(project, end_date):
    q = ('project in ("' + project + '") AND issueType in (Story, Bug, Task)' +
         ' AND resolved <= ' + format_query_date(end_date) +
         ' AND resolved >= ' + format_query_date(calc_start_date(end_date)) +
         ' AND resolution = Done ORDER BY resolved ASC')
    print(q)
    return q


# page through the query until it is empty
def get_issues(session, project, end_date):
    result = []
    start = 0
    query = jql_query(project, end_date)
    while True:
        issues = session.search_issues(query, fields=[POINTS, 'issuetype', 'resolutiondate'], startAt=start)
        if len(issues) == 0:
            break
        else:
            result += issues
            start += len(issues)
    return result


def format_resolution(res):
    # example res date from Jira: 2021-01-04T11:13:36.000+0100
    return format_data_date(datetime.fromisoformat(res[0:19]))


# transform the issues to rows as if read from a CSV Jira export
# well, the CSV came first, so that's our common denominator
def issues_to_rows(issues):
    rows = []
    for i in issues:
        rows.append(
            [i.key, i.id, i.fields.issuetype.name, int(i.fields.customfield_10106 or 0),
             format_resolution(i.fields.resolutiondate)])
    return rows


# get the issues for a specific project
def get_project_issues(project, user, pwd, end_date):
    session = get_session((user, pwd))
    issues = get_issues(session, project, end_date)
    return issues_to_rows(issues)


if __name__ == '__main__':
    ROWS = get_project_issues(PRODUCT, sys.argv[1], sys.argv[2], datetime.now())
    for r in ROWS:
        print(r)
