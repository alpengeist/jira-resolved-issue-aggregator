#
#  el cheapo Jira interface module custom for PxC
#

from jira import JIRA
from datetime import datetime, timedelta

# project convenience shortcuts, avoids using blanks on the commandline
PROJECTS = {'product': '[SCS] Product', 'explore': '[SCS] Explore'}
POINTS = 'customfield_10106'
MAX_DAYS = 555
JIRA_URL = 'https://jira-web.europe.phoenixcontact.com'


def get_session(credentials):
    # silly PxC certificate cannot be validated
    return JIRA(options={'server': JIRA_URL, 'verify': False},
                basic_auth=credentials)


def calc_start_date(end_date):
    return end_date - timedelta(days=MAX_DAYS-1)


def format_query_date(date):
    return datetime.strftime(date, '%Y-%m-%d')


def format_data_date(date):
    return datetime.strftime(date, '%d.%m.%y %H:%M')


def format_status_date(datestring):
    # example date from Jira: 2021-01-04T11:13:36.000+0100
    return format_data_date(datetime.fromisoformat(datestring[0:19]))


def jql_resolved(project, end_date):
    # The actual excluded end date for the query is the next date 00:00, otherwise Jira would not find anything
    # from the specified end day.
    actual_end = end_date + timedelta(days=1)
    q = ('project in ("' + project + '") AND issueType in (Story, Bug, Task)' +
         ' AND resolved < ' + format_query_date(actual_end) +
         ' AND resolved >= ' + format_query_date(calc_start_date(end_date)) +
         ' AND resolution = Done ORDER BY resolved ASC')
    print(q)
    return q


# page through the query until it is empty
def get_issues(session, project, end_date):
    result = []
    start = 0
    query = jql_resolved(project, end_date)
    while True:
        issues = session.search_issues(query, fields=[POINTS, 'issuetype', 'resolutiondate'], expand='changelog',
                                       startAt=start)
        # print(issues)
        if len(issues) == 0:
            break
        else:
            result += issues
            start += len(issues)
    return result


# transform the issues to rows as if read from a CSV Jira export
# well, the CSV came first, so that's our common denominator
def issues_to_rows(issues):
    rows = []
    for i in issues:
        rows.append(
            [i.key, i.id, i.fields.issuetype.name, int(i.fields.customfield_10106 or 0),
             format_status_date(i.fields.resolutiondate),
             format_status_date(i.board_enter_date)])
    return rows


# get the issues for a specific project
def get_project_issues(project, user, pwd, end_date):
    session = get_session((user, pwd))
    issues = get_issues(session, project, end_date)
    for issue in issues:
        issue.board_enter_date = find_board_enter_date(issue)
    return issues_to_rows(issues)


# dig into the history and find each statuschange with the status as key and the date string as value
def get_issue_statuschanges(issue):
    changes = {}
    for history in issue.changelog.histories:
        for item in history.items:
            if item.field == 'status':
                changes[item.toString.lower()] = history.created
    return changes


# Find the date when the issue entered the Kanban board.
# The status changes are very inconsistent in the history. Not all seem to be registered. The workflow is practically
# always incompletely represented.
# We start with the earliest from the various Scrum workflows.
def find_board_enter_date(issue):
    changes = get_issue_statuschanges(issue)
    for c in ['initiation', 'refinement', 'in progress', 'review', 'approved', 'done', 'operation']:
        if c in changes:
            return changes[c]
    raise 'incomplete history in issue ' + issue.key  # this should not happen, as all issues are done or operation


if __name__ == '__main__':
    print("nix zu tun")
