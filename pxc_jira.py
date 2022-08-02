#
#  el cheapo Jira interface module custom for PxC
#
import sys

from jira import JIRA
from datetime import datetime

EXPLORE = '[SCS] Explore'
PRODUCT = '[SCS] Product'
POINTS = 'customfield_10106'


def get_session(credentials):
    # silly PxC certificate cannot be validated
    return JIRA(options={'server': 'https://jira-web.europe.phoenixcontact.com', 'verify': False},
                basic_auth=credentials)


# page through the query until it is empty
def get_issues(session, project, start_date):
    result = []
    start = 0
    while True:
        issues = session.search_issues( 'project in ("' + project + '") AND issueType in (Story, Bug, Task) '
                                        'AND resolved >' + start_date + ' AND resolution = Done ORDER BY resolved ASC',
                                        fields=[POINTS, 'issuetype', 'resolutiondate'],
                                        startAt=start)
        if len(issues) == 0:
            break
        else:
            result += issues
            start = start + len(issues)
    return result


def convert_resolution(res):
    # example res date from Jira: 2021-01-04T11:13:36.000+0100
    return datetime.fromisoformat(res[0:19]).strftime('%d.%m.%y %H:%M')


# transform the issues to rows as if read from a CSV Jira export
# well, the CSV came first, so that's our common denominator
def issues_to_rows(issues):
    rows = []
    for i in issues:
        rows.append(
            [i.key, i.id, i.fields.issuetype.name, int(i.fields.customfield_10106 or 0),
             convert_resolution(i.fields.resolutiondate)])
    return rows


def get_project_issues(project, start_date, user, pwd):
    session = get_session((user, pwd))
    issues = get_issues(session, project, start_date)
    return issues_to_rows(issues)


if __name__ == '__main__':
    ROWS = get_project_issues(PRODUCT, '2021-01-01', sys.argv[1], sys.argv[2])
    for r in ROWS:
        print(r)
